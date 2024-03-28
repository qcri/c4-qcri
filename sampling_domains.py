# Sampling from dataset by article url domain
# Yifan Zhang (yzhang@hbku.edu.qa)

import sys
import json
import random
import fileinput
from collections import Counter, defaultdict
from urllib.parse import urlparse

def domain_of(url):
  parts = urlparse(url)
  netloc = parts.netloc or parts.path
  return netloc

NSAMPLES = 10

domains = Counter()
samples = defaultdict(list)

for i, line in enumerate(fileinput.input(files=("-"), encoding="utf-8")):
  page = json.loads(line)

  # in order to process the result of our script (merge results), we
  # will use domain and domain_count if available
  domain = page.get('domain', domain_of(page['url']))
  count = page.get('domain_count', 1)
  domains.update({domain: count})

  if page['text'] and \
    page['language'].startswith("ara"):

    if len(samples[domain]) < NSAMPLES:
      samples[domain].append({'domain': domain, **page})
    elif domains[domain] < 100:
        samples[domain].pop(0)
        samples[domain].append({'domain': domain, **page})
    elif domains[domain] % 1000 == 1:
      samples[random.randint(NSAMPLES)] = {'domain': domain, **page}


for domain, count in domains.most_common():
  for page in samples.get(domain, []):
    page.update({'domain_count': count})
    print(json.dumps(page, ensure_ascii=False), file=sys.stdout)