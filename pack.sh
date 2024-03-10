#!/bin/bash

# FIXME hardcoded variables, should go into c4-qcri.sh

NUM_TASKS=64
NUM_LINES=$(wc -l < download_and_split/input.txt)
LINES_PER_TASK=$((NUM_LINES / NUM_TASKS))
EXTRA_LINES=$((NUM_LINES % NUM_TASKS))
INPUT_FILE="download_and_split/input.txt"

for ((i = 0; i < NUM_TASKS; i++)); do
    START_LINE=$((i*LINES_PER_TASK + 1))
    END_LINE=$((START_LINE + LINES_PER_TASK - 1))
    if [ $i -eq $((NUM_TASKS - 1)) ]; then
        END_LINE=$((END_LINE + EXTRA_LINES))
    fi

    rm -f download_and_split/output.txt

    awk "NR >= $1 && NR <= $2" "download_and_split/input.txt" | \
      while read LINE;
    do

      # download wet file
      DOWNLOADED="download_and_split_$3/$(basename $LINE)"
      OUTPUT="${DOWNLOADED%gz}pages.jsons.gz"

      echo $OUTPUT >> download_and_split/output.txt
    done
done

cat "download_and_split/output.txt" | while read LINE; do
  # group split into a large gz file
  GZFILENAME=${$(basename LINE)%-*}
  cat $LINE >> $GZFILENAME.warc.wet.pages.jsonl.gz
done