import sys
import re
import os
import time
import random
import requests


headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}


def login():
  s = requests.Session()
  s.headers.update(headers)

  url = "https://www.arabworldbooks.com/en/login"
  r = s.get(url)
  m = re.search('<input type="hidden" name="_token" value="(?P<token>.*?)">', r.text)
  if m is None:
    print("login failed")
    print(r.text)
    sys.exit(0)
    return

  token = m.group('token')

  # login
  r = s.post(url, data={
    'email': '05_edit.twelve@icloud.com',
    'password': 'password',
    '_token': token,
  })

  print(r.text)
  print(r.status_code)

  return s


def download(title, url, s):
  local_filename = os.path.join("downloads", title + ".pdf")
  with s.get(url, stream=True) as r:
    r.raise_for_status()
    with open(local_filename, "wb") as f:
      for chunk in r.iter_content(chunk_size=8192):
        f.write(chunk)
  return local_filename



session  = login()
url = "https://www.arabworldbooks.com/en/books?bookOfMonth=&author=&genre=&publisher=&publish_year=&free_download=1&q=&page="
bookUrlRegex = r"<a href=\"(?P<url>https://www.arabworldbooks.com/en/books/.*?)\">"

page = 1
while True:
  r = session.get(url+str(page))
  if r.status_code != 200:
    print(r.text)
    break

  print("working on page ", url+str(page))
  for match in re.finditer(bookUrlRegex, r.text):
    url = match.group('url')
    print(url)
    title = url.split("/")[-1]
    if re.search(r'[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF\uFB50-\uFDFF\uFE70-\uFEFF]+', title) is None:
      print("Skipping non-Arabic", title)
      continue

    print("downloading", title, url)
    download(title, url=url.rstrip("/")+"/download", s=session)
    time.sleep(random.randint(1,9))
    
  page += 1