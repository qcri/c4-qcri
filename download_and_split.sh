#!/bin/bash -ex
#SBATCH --job-name=download_and_split_$3
#SBATCH --output=download_and_split/output_task_$3.log

mkdir -p download_and_split_$3

START_LINE=$1
END_LINE=$2
TOTAL_LINE=$((END_LINE - START_LINE + 1))

awk "NR >= $1 && NR <= $2" "download_and_split/input.txt" | \
  while read LINE;
do
  # download wet file
  DOWNLOADED="download_and_split_$3/$(basename $LINE)"

  if [ ! -s "${DOWNLOADED%gz}pages.jsons.gz"]; then
    if [ ! -s "$DOWNLOADED" ]; then
      wget -q -O "$DOWNLOADED" "https://data.commoncrawl.org"/${LINE}
    fi
    python3 split_wet_file.py "$DOWNLOADED"
  fi
  if [ -s "${DOWNLOADED%gz}pages.jsons.gz"]; then
    rm -f "$DOWNLOADED"
  fi
done
