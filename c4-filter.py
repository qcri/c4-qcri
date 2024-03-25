import os
import re
import sys
import json
import heapq
import fileinput
import dataclasses
import hashlib
import collections
from typing import Optional, Any


_SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


# Filters
_MIN_WORDS_PER_LINE = 5
_MIN_NUM_SENTENCES = 3
_MAX_WORD_LENGTH = 1000
_END_MARKS = (".", "?", "!", '"', "؟")    # FIXME add Arabic
_ELLIPSIS = "..."
_POLICY_SUBSTRINGS = [
    "terms of use",
    "privacy policy",
    "cookie policy",
    "uses cookies",
    "use of cookies",
    "use cookies",
]

_MIN_PARAGRAPHS = 3
_MIN_PARAGRAPH_LEN = 200


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
  word_count: Optional[int] = None
  language: Optional[str] = None
  discarded: Optional[str] = None


class Filter:
  def __init__(self):
    pass

  def should_pass(self, page):
    return True

  def __call__(self, page):
    if self.should_pass(page):
      return page


class Processor:
  def __init__(self):
    pass

  def process(self, page):
    pass

  def __call__(self, page):
    self.process(page)
    return page


class Pipeline:
  def __init__(self, modules, debug=False):
    self.modules = modules
    self.debug = debug

  def __call__(self, dataset):
    for page in dataset:
      for module in self.modules:
        new_page = module(page)
        if new_page is None:
          if self.debug:
            page.discarded = module.__class__.__name__
            yield page
          break
        page = new_page
      else:
        yield page


class NormalizeUrlProcessor(Processor):
  def __init__(self):
    super().__init__()

  def process(self, page):
    url = page.url
    url = re.sub(r"https?:\/\/(www\.)?", "", url)
    url = re.sub(r"\?(utm_|ref|feed).*", "", url)
    url = url.rstrip("/")
    page.normalized_url = url


class WordCountProcessor(Processor):
  def __init__(self):
    super().__init__()

  def process(self, page):
    page.word_count = len(page.text.split())


class CleanTextProcessor(Processor):
  def __init__(self):
    super().__init__()
    self.citation_regex = re.compile(r"\[\d*\]|\[edit\]|\[citation needed\]")
    self.min_words_per_line = _MIN_WORDS_PER_LINE
    self.min_num_sentences = _MIN_NUM_SENTENCES
    self.max_word_length = _MAX_WORD_LENGTH
    self.line_delimiter = "\n"

  @staticmethod
  def line_is_copyright(text):
    return '©' in text
    
  @staticmethod
  def line_is_javascript_code(text):
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

  @staticmethod
  def contains_arabic(text):
    # Regular expression pattern to match Arabic characters
    arabic_pattern = re.compile(r'[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF\uFB50-\uFDFF\uFE70-\uFEFF]+')
    
    # Check if the pattern matches the text
    if arabic_pattern.search(text):
        return True
    else:
        return False

  def process(self, page):
    text = page.text
    lines = text.splitlines()

    valid_lines = []

    def line_has_too_long_word(line):
      for word in line.split():
        if len(word) > max_word_length:
          return True
      return False

    for line in lines:
      line = line.strip()
      line = self.citation_regex.sub("", line)

      if line_has_too_long_word(line):
        continue

      if not line.endswith(_END_MARKS) or line.endswith(_ELLIPSIS):
        counter_inc_fn("line-filtered:no_endmark")
        continue
      if len(line.split()) < min_words_per_line:
        counter_inc_fn("line-filtered:too_short")
        continue

      if CleanTextProcessor.line_is_javascript_code(line):
        continue

      if not CleanTextProcessor.contains_arabic(line):
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
        continue
      # Remove copyrights
      if "©" in line:
        counter_inc_fn("filtered:copyright")
        continue
      # Remove policy lines
      if any(p in line_lower for p in _POLICY_SUBSTRINGS):
        counter_inc_fn("line-filtered:policy")
        continue

      # num_sentences += len(_get_sentences(line))
      valid_lines.append(line)

    page.text = '\n'.join(valid_lines)
    

class BadUrlFilter(Filter):
  def __init__(self, **kwargs):
    super().__init__(**kwargs)
    self.regex = re.compile(r"(porn|video)")

  def should_pass(self, page):
    if self.regex.search(page.url.lower()):
      return False
    return True


class LengthFilter(Filter):
  def __init__(self, min_len=100, max_len=10000, **kwargs):
    super().__init__(**kwargs)
    self.min_len = min_len
    self.max_len = max_len

  def should_pass(self, page):
    if self.min_len < len(page.text) < self.max_len:
      return True
    return False


class BadWordsFilter(Filter):
  def __init__(self, badwords, filter_fraction=0.01, **kwargs):
    super().__init__(**kwargs)
    self.badwords = badwords
    self.filter_fraction = filter_fraction
    self.filter = get_badwords_filter_fn(badwords=badwords, filter_fraction=filter_fraction)

  def should_pass(self, page):
    return self.filter(page)


class C4ParagraphFilter(Filter):
  def __init__(self, min_paragraphs=3, min_paragraph_len=200, line_delimiter="\n"):
    super().__init__()
    self.min_paragraphs = min_paragraphs
    self.min_paragraph_len = min_paragraph_len
    self.line_delimiter = line_delimiter

  def should_pass(self, page):
    lines = page.text.split(line_delimiter)
    if len(lines) < self.min_paragraphs or \
      min(heapq.nlargest(3, [len(l) for l in lines])) < self.min_paragraph_len:
      return False
    return True


line_delimiter = '\n'


counts = {}

def counter_inc_fn(name):
  counts[name] = counts.get(name, 0) + 1


def get_counter_inc_fn(counter_name):
  return counter_inc_fn


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
  with open(os.path.join(_SCRIPT_DIR, 'ar-badwords.txt'), 'rt') as f:
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

  badwords = load_badwords()

  pipeline = Pipeline([
    NormalizeUrlProcessor(),
    BadUrlFilter(),
    CleanTextProcessor(),
    C4ParagraphFilter(), 
    BadWordsFilter(badwords=badwords),
  ], debug=args.debug)

  def pages():
    for json_line in fileinput.input(files=("-"), encoding="utf-8"):
      yield PageFeatures(**json.loads(json_line))

  for page in pipeline(pages()):
    print(json.dumps(dataclasses.asdict(page, dict_factory=lambda x: {k: v for (k, v) in x if v is not None}), ensure_ascii=False), file=sys.stdout)

    
if __name__ == '__main__':
  import argparse

  parser = argparse.ArgumentParser(
    prog="c4-filter",
    description="filter and clean using c4 strategy",
  )
  parser.add_argument('--debug', dest='debug', action='store_true',
                      help='debug')
  parser.add_argument('--debug-url', dest='debug_url', type=str,
                      help='use if you want to debug a specific url')
  parser.add_argument('--debug-clean', dest='debug_clean', action='store_true', default=False,
                      help='use this flag to display decisions made in cleaning')
  parser.add_argument('-o', '--output', dest='output', help='output file')
  parser.add_argument('--out-dir', dest='out_dir', type=str,
                      help='output directory')
  parser.add_argument('--clean', dest='clean', action='store_true', default=False,
                      help='run text cleaning for article')
  parser.add_argument('--only-arabic', dest='only_arabic', action='store_true', default=False,
                      help='keep text only if it contains at least some Arabic characters')
  parser.add_argument('--length-filter', dest='length_filter', action='store_true', default=False,
                      help='filter content too short or too long')
  parser.add_argument('--paragraph-filter', dest='paragraph_filter', action='store_true', default=False,
                      help='run paragraph filter')
  parser.add_argument('--min-paragraphs', dest='min_paragraphs', type=int, default=_MIN_PARAGRAPHS,
                      help='minimal number of paragraphs')
  parser.add_argument('--min-paragraph-len', dest='min_paragraph_len', type=int, default=_MIN_PARAGRAPH_LEN,
                      help='minimal length for a paragraph')
  parser.add_argument('--lang-detect', dest='lang_detect', action='store_true', default=False,
                      help='run language detection')
  parser.add_argument('--badwords-filter', dest='badwords_filter', action='store_true', default=False,
                      help='run badwords filter')
  parser.add_argument('--add-word-count', dest='add_word_count', action='store_true', default=False,
                      help='add word counts in output')

  args = parser.parse_args()
  process(args)
