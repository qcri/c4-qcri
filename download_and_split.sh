#!/bin/bash -ex
#SBATCH --job-name=download_and_split_$3
#SBATCH --output=download_and_split/output_task_$3.log

awk "NR >= $1 && NR <= $2" "download_and_split/input.txt" | while read LINE;
do
  # download wet file
  mkdir -p download_and_split_$3
  wget -O download_and_split_$3/$(basename $LINE) "https://data.commoncrawl.org"/${LINE}
  python split_wet_file.py download_and_split_$3/$(basename $LINE)
done
