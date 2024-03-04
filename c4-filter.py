import re
import sys
import gzip
import json
import logging
import dataclasses
import hashlib
import collections
from typing import Optional, Iterable, Mapping, Sequence
import tensorflow_datasets as tfds
from tensorflow_datasets.text import c4_utils
import tensorflow as tf


# Filters
_MIN_WORDS_PER_LINE = 5
_MIN_NUM_SENTENCES = 3
_MAX_WORD_LENGTH = 1000
_END_MARKS = (".", "?", "!", '"')    # FIXME add Arabic
_ELLIPSIS = "..."
_POLICY_SUBSTRINGS = [
    "terms of use",
    "privacy policy",
    "cookie policy",
    "uses cookies",
    "use of cookies",
    "use cookies",
]


_BADWORDS_URL = "https://raw.githubusercontent.com/LDNOOBW/List-of-Dirty-Naughty-Obscene-and-Otherwise-Bad-Words/5faf2ba42d7b1c0977169ec3611df25a3c08eb13/ar"


UNKNOWN_LANGUAGE = "und"



@dataclasses.dataclass
class PageFeatures:
  url: str = ""
  normalized_url: str = ""
  text: str = ""
  timestamp: str = ""
  content_length: str = ""
  content_type: str = ""
  language: Optional[str] = None


def normalize_url(url):
  # url = tf.compat.as_text(url)
  url = re.sub(r"https?:\/\/(www\.)?", "", url)
  url = re.sub(r"\?(utm_|ref|feed).*", "", url)
  url = url.rstrip("/")
  return url


line_delimiter = '\n'


counts = {}

def counter_inc_fn(name):
  counts[name] = counts.get(name, 0) + 1


def get_counter_inc_fn(counter_name):
  return counter_inc_fn


def is_javascript_code(text):
    # Count occurrences of specific characters
    count_open_parenthesis = text.count('(')
    count_close_parenthesis = text.count(')')
    count_dollar_sign = text.count('$')
    count_semicolon = text.count(';')
    count_equals = text.count('=')
    count_equals = text.count('{')
    count_equals = text.count('}')
    count_equals = text.count('+')
    count_equals = text.count('_')
    count_equals = text.count("'")
    count_equals = text.count('"')
    count_equals = text.count('#')
    count_equals = text.count('/')
    
    # Calculate the total number of characters
    total_characters = len(text)
    
    # Calculate the total count of specific characters
    total_count = (count_open_parenthesis + count_close_parenthesis +
                   count_dollar_sign + count_semicolon + count_equals)
    
    # Calculate the percentage of the total count relative to the total characters
    percentage_total_count = (total_count / total_characters) * 100
    
    # Check if the percentage exceeds 8%
    if percentage_total_count > 8:
        return True
    else:
        return False


def contains_arabic(text):
    # Regular expression pattern to match Arabic characters
    arabic_pattern = re.compile(r'[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF\uFB50-\uFDFF\uFE70-\uFEFF]+')
    
    # Check if the pattern matches the text
    if arabic_pattern.search(text):
        return True
    else:
        return False


def clean_page(
    page: PageFeatures,
    citation_regex=re.compile(r"\[\d*\]|\[edit\]|\[citation needed\]"),
    counter_inc_fn=None,
    min_words_per_line=_MIN_WORDS_PER_LINE,
    min_num_sentences=_MIN_NUM_SENTENCES,
    max_word_length=_MAX_WORD_LENGTH,
    line_delimiter="\n",
) -> Iterable[PageFeatures]:
  """Cleans a CommonCrawl page, yielding nothing if it should be skipped.

  Cleaning removes lines with no end marks or with too few words. After line
  filtering, pages are filtered out if they have too few sentences based on a
  simple count of end marks.

  Args:
    page: the features of the page.
    citation_regex: Regex to use for finding Wikipedia-like citations to filter.
    counter_inc_fn: function, a function taking the name of a counter to be
      incremented and the (optional) amount. Defaults to a beam Metric counter.
    min_words_per_line: int, the minimum number of words a line needs to not be
      removed.
    min_num_sentences: int, the minimum number of sentences a page needs to not
      be skipped.
    max_word_length: int, the maximum number of characters allowed in a word.
      Lines containing a word with too many characters are removed.
    line_delimiter: str, the delimiter used to separate and join lines.

  Yields:
    The url and cleaned text for the page.
  """
  text = page.text

  if not counter_inc_fn:
    counter_inc_fn = get_counter_inc_fn("clean-page")

  lines = text.splitlines()
  valid_lines = []
  num_sentences = 0

  def line_is_arabic(line):
    return contains_arabic(line)

  def line_is_code(line):
    return is_javascript_code(line)

  def line_has_too_long_word(line):
    for word in line.split():
      if len(word) > max_word_length:
        return True
    return False

  for line in lines:
    line = line.strip()
    if not line_is_arabic(line):
      counter_inc_fn("line-filtered:not_arabic")
      continue
    if line_has_too_long_word(line):
      counter_inc_fn("line-filtered:too_long_word")
      continue
    if line_is_code(line):
      counter_inc_fn("line-filtered:code")
      continue
    line = citation_regex.sub("", line)
    if not line.endswith(_END_MARKS) or line.endswith(_ELLIPSIS):
      counter_inc_fn("line-filtered:no_endmark")
      continue
    if len(line.split()) < min_words_per_line:
      counter_inc_fn("line-filtered:too_short")
      continue
    line_lower = line.lower()
    # Remove documents which contain lorem ipsum
    if "lorem ipsum" in line_lower:
      counter_inc_fn("filtered:loremipsum")
      return
    # Remove "javascript must be enabled" notices
    if "javascript" in line_lower:
      counter_inc_fn("line-filtered:javascript")
      continue
    # Remove docs which probably contain javascript code
    if "{" in line:
      counter_inc_fn("filtered:squigglybracket")
      return
    # Remove policy lines
    if any(p in line_lower for p in _POLICY_SUBSTRINGS):
      counter_inc_fn("line-filtered:policy")
      continue
    # FIXME adapt it for Arabic
    # num_sentences += len(_get_sentences(line))
    valid_lines.append(line)
    counter_inc_fn("line-passed")

  # FIXME adapt it for Arabic
  # if num_sentences < min_num_sentences:
  #   counter_inc_fn("filtered:too_few_sentences")
  #   return
  counter_inc_fn("passed")
  return dataclasses.replace(page, text=line_delimiter.join(valid_lines).strip())


class PredictLanguage():
  """Predicts page's language using cld3 and adds to features."""

  def __init__(self, valid_languages, min_probability=0.95):
    self._valid_languages = set(valid_languages)
    self._counter_inc_fn = get_counter_inc_fn("language-filter")
    self._min_probability = min_probability

  def start_bundle(self):
    self._detector = tfds.core.lazy_imports.gcld3.NNetLanguageIdentifier(
        # CLD3 is not expected to work well on very short documents.
        min_num_bytes=100,
        max_num_bytes=10000,
    )

  def process(self, page: PageFeatures):
    result = self._detector.FindLanguage(page.text)
    if not result.is_reliable:
      self._counter_inc_fn("filtered:no_predictions")
      lang = UNKNOWN_LANGUAGE
    elif result.probability < self._min_probability:
      self._counter_inc_fn("filtered:low_confidence")
      lang = UNKNOWN_LANGUAGE
    else:
      lang = result.language
      if lang not in self._valid_languages:
        self._counter_inc_fn("filtered:ignored_language")
        return
    self._counter_inc_fn("passed")
    self._counter_inc_fn("passed:%s" % lang)
    return dataclasses.replace(page, language=lang)


def get_hashed_url_filter_fn(predicate_fn):
  def filter_fn(page):
    url = page.normalized_url
    val = int(
        hashlib.md5(tf.compat.as_text(url).encode("utf-8")).hexdigest(), 16
    )
    return predicate_fn(val)

  return filter_fn
  
def load_badwords():
  badwords = collections.defaultdict(set)
  with open('ar-badwords.txt', 'rt') as f:
    badwords['ar'].update(x.strip() for x in f)
  return badwords

def get_badwords_filter_fn(badwords, filter_fraction: float = 1.0):
  """Filters pages at given rate that contain language-specific bad word(s)."""
  badwords_regex = {}

  for lang, words in badwords.items():
    words = [re.escape(w) for w in words]
    badwords_regex[lang] = (
        # For Japanese, Thai, and Chinese, do not require word separations.
        re.compile("|".join(words))
        if lang in ("ja", "th", "zh")
        # For other languages, match only when flanked by non-word chars.
        else re.compile(r"(?:\W|^)({})(?:\W|$)".format("|".join(words)))
    )

  filter_ratio = float.as_integer_ratio(filter_fraction)
  keep_badword_page = get_hashed_url_filter_fn(
      lambda x: x % filter_ratio[1] >= filter_ratio[0]
  )


  def badwords_filter(page):
    lang = page.language.split("-")[0]  # remove suffix if present

    if lang in badwords_regex:
      text = page.text
      badwords_found = badwords_regex[lang].search(text.lower())
      if badwords_found is not None:
        if keep_badword_page(page):
          get_counter_inc_fn("badwords-filter")("soft-passed")
          get_counter_inc_fn("badwords-filter-%s" % lang)("soft-passed")
          return True
        get_counter_inc_fn("badwords-filter")("filtered")
        get_counter_inc_fn("badwords-filter-%s" % lang)("filtered")
        return False
      get_counter_inc_fn("badwords-filter-%s" % lang)("passed")

    get_counter_inc_fn("badwords-filter")("passed")
    return True

  return badwords_filter



def process(args):

  gz_file_path = args.input
  outfile_path = args.output or gz_file_path[:-len('gz')]+"cleaned.gz"


  langdetect = PredictLanguage(["ar"])
  langdetect.start_bundle()

  badwords = load_badwords()
  badwords_filter = get_badwords_filter_fn(badwords, filter_fraction=0.999)

  with gzip.open(gz_file_path, "rt", encoding="utf-8") as f, \
    gzip.open(outfile_path, "wt", encoding="utf8") as o:

    for json_line in f:
      page = PageFeatures(**json.loads(json_line))
      
      if args.debug:
        print(page.text)

      if args.clean:
        page = clean_page(page)
        if not page:
          if args.debug:
            print("*********** skipped due to cleaning")
          continue

      if not c4_utils.is_valid_length(page):
        if args.debug:
          print('*********** skipped due to not valid length:', len(page.text))
        continue

      # url dedupe, choose newest page for same url (not applicable)

      if args.paragraph_filter and not c4_utils.paragraph_filter(page):
        if args.debug:
          print('********** skipped due to paragraph_filter')
        continue

      # language
      if args.lang_detect:
        page = langdetect.process(page)
        if not page:
          if args.debug:
            print('******** skipped due to language')
          continue

      if args.badwords_filter and not badwords_filter(page):
        if args.debug:
          print('********** skipped due to bad words')
        continue

      o.write(json.dumps(dataclasses.asdict(page), ensure_ascii=False))
      o.write("\n")


if __name__ == '__main__':
  import argparse

  parser = argparse.ArgumentParser(
    prog="c4-filter",
    description="filter and clean using c4 strategy",
  )
  parser.add_argument('input', type=str, help='input file (.jsons.gz)')
  parser.add_argument('-o', '--output', dest='output', help='output file')
  parser.add_argument('--debug', dest='debug', action='store_true',
                      help='debug')
  parser.add_argument('--clean', dest='clean', action='store_true', default=False,
                      help='run text cleaning for article')
  parser.add_argument('--paragraph-filter', dest='paragraph_filter', action='store_true', default=False,
                      help='run paragraph filter')
  parser.add_argument('--lang-detect', dest='lang_detect', action='store_true', default=False,
                      help='run language detection')
  parser.add_argument('--badwords-filter', dest='badwords_filter', action='store_true', default=False,
                      help='run badwords filter')

  args = parser.parse_args()
  process(args)
