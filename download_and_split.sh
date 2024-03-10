#!/bin/bash -x
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
  OUTPUT="${DOWNLOADED%gz}pages.jsons.gz"

  # if we have downloaded wet file, we probably didn't finish
  # previous job, it is better to remove both output file and
  # redo this particular file
  if [ -s "$DOWNLOADED" ]; then
    rm -f "$OUTPUT"
    rm -f "$DOWNLOADED"
  fi

  # if output file exist, skip; if not, download and split
  if [ ! -s "$OUTPUT" ]; then
    if [ ! -s "$DOWNLOADED" ]; then
      while true; do
        wget -T 15 -q -O "$DOWNLOADED" "https://data.commoncrawl.org"/${LINE} && break
      done
    fi
    python3 split_wet_file.py "$DOWNLOADED"
  fi

  # remove downloaded wet file to save space, only keep output
  if [ -s "$OUTPUT" ]; then
    rm -f "$DOWNLOADED"
  fi
done
