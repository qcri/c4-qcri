#!/bin/bash

# number of parallel processes

POOL_SIZE=4

INPUT=$1
LOGDIR=$2
CMD=${@:3}

mkdir -p $LOGDIR

NJOBS=$(wc -l < $INPUT)

# start initial POOL_SIZE - 1 jobs
for (( i=1; i<POOL_SIZE; i++ )); do
  LINE=$(awk "NR==$i" $INPUT)
  $CMD $LINE &> $LOGDIR/$i.log &
done

for ((i=POOL_SIZE; i<=NJOBS; i++)); do
  LINE=$(awk "NR==$i" $INPUT)
  $CMD $LINE &> $LOGDIR/$i.log &
  wait -n
done