import re
import os
import sys
import gzip
import json
import logging
import dataclasses
from functools import lru_cache
from typing import Optional


# WET file constants
_PAGE_DELIMITER = "WARC/1.0"
_URL_KEY = "WARC-Target-URI:"
_URL_DATE = "WARC-Date:"
_LANGUAGE = "WARC-Identified-Content-Language:"
_CONTENT_TYPE = "Content-Type:"
_CONTENT_LEN = "Content-Length:"
_METADATA_PREFIXES = ("WARC", "CONTENT-", "Content-")


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


wet_file_path = sys.argv[1]
line_delimiter = '\n'

logging.info("Splitting file: %s", wet_file_path)

def _validate_features(page):
  if page.url and page.text and page.timestamp:
    return True
  return False

def split_pages(wet_file_path):
  with gzip.open(wet_file_path, mode="rt") as f:
    page = PageFeatures()
    for i, line in enumerate(f.readlines()):
      line = line.strip()
      if not line:
        continue
      if line == _PAGE_DELIMITER:
        if i > 0 and _validate_features(page):
          yield page
        page = PageFeatures()

      if line.startswith(_URL_KEY):
        page.url = line[len(_URL_KEY) :].strip()
        page.normalized_url = normalize_url(line[len(_URL_KEY) :].strip())

      if line.startswith(_URL_DATE):
        page.timestamp = line[len(_URL_DATE) :].strip()

      if line.startswith(_LANGUAGE):
        page.language = line[len(_LANGUAGE) :].strip()

      if line.startswith(_CONTENT_TYPE):
        page.content_type = line[len(_CONTENT_TYPE) :].strip()

      if line.startswith(_CONTENT_LEN):
        page.content_length = line[len(_CONTENT_LEN) :].strip()

      if line.startswith(_METADATA_PREFIXES):
        continue

      if page.text:
        page.text += line_delimiter
      page.text += line

    if _validate_features(page):
      yield page


if not os.path.exists(wet_file_path):
  print(f"input file not found: {wet_file_path}", file=sys.stderr) 
  sys.exit(1)


@lru_cache
def lazy_gzip_open():
  return gzip.open(wet_file_path[:-len('gz')]+"pages.jsonl.gz", "wt", encoding="utf8")

total = 0
count = 0

for page in split_pages(wet_file_path):
  total += 1
  if page.language and page.language.startswith('ara'):
    count += 1
    lazy_gzip_open().write(json.dumps(dataclasses.asdict(page), ensure_ascii=False) + "\n")

logging.info("%d/%d extracted", count, total)
