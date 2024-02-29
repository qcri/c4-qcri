#!/bin/bash -ex
#SBATCH --job-name=download_and_split_$3
#SBATCH --output=download_and_split/output_task_$3.log

mkdir -p download_and_split_$3

awk "NR >= $1 && NR <= $2" "download_and_split/input.txt" | while read LINE;
do
  # download wet file
  DOWNLOADED="download_and_split_$3/$(basename $LINE)"
  if [ ! -s "$DOWNLOADED" ]; then
    wget -q -O "$DOWNLOADED" "https://data.commoncrawl.org"/${LINE}
  fi
  python3 split_wet_file.py "$DOWNLOADED"
done
