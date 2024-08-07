#!/bin/bash -e

# c4-qcri.sh
# QCRI's script to download and clean Arabic data from CommonCrawl. It does
# - download a given CommonCrawl dump (WET files)
# - parse and extract only pages with 'ara' in identified languages
# - output is gzipped jsonl format
#
# by Yifan Zhang (yzhang@hbku.edu.qa)
# Copyright (C) 2024, Qatar Computing Research Institute

export SCRIPT_DIR=$(dirname "$0")

# to clean up and terminate child processes
function cleanup {
    echo "Interrupt signal received. Cleaning up..."
    # Terminate all child processes
    pkill -P $$
    exit 1
}

# Trap SIGINT signal (Ctrl+C) and call cleanup function
trap cleanup SIGINT


function wget_until_success {
    URL=$1
    SAVETO=$2
    while true; do
        wget -T 120 -q -O "$SAVETO" "$URL" && break
        sleep 60
    done
}


export -f wget_until_success

function extract_unique_name {
    WETPATH=$1

    UNIQNAME=${WETPATH#*segments/}
    UNIQNAME=${UNIQNAME//\//-}

    echo $UNIQNAME
}

export -f extract_unique_name


function download_and_parse {
    WETPATH=$1
    OUTDIR=$2

    # download wet file
    BASENAME=$(basename $WETPATH)
    SUBDIR=${OUTDIR}/${BASENAME:0:22}
    UNIQNAME=$(extract_unique_name $WETPATH)
    DOWNLOADED="$SUBDIR/$UNIQNAME"
    GZOUTPUT="${DOWNLOADED%gz}pages.jsonl.gz"

    mkdir -p $SUBDIR

    # if we have downloaded wet file, we probably didn't finish
    # previous job, it is better to remove both output file and
    # redo this particular file
    if [ -s "$DOWNLOADED" ]; then
        rm -f "$GZOUTPUT"
        rm -f "$DOWNLOADED"
    fi

    # if output file exist, skip; if not, download and process it
    if [ ! -s "$GZOUTPUT" ]; then
        if [ ! -s "$DOWNLOADED" ]; then
            wget_until_success "https://data.commoncrawl.org/$WETPATH" "$DOWNLOADED"
        fi

        if [ ! -s "$DOWNLOADED" ]; then
            echo "Downloading failed"
            return 1
        else
            # check the integrity of downloaded file before processing
            gzip -t "$DOWNLOADED"
            if [ $? -ne 0 ]; then
                echo "file may be corrupted"
                rm -f "$DOWNLOADED"
                if grep -q "$WETPATH" $SCRIPT_DIR/corrupted.lst; then
                    echo "found file in corrupted.lst, will skip and create empty output"
                    touch $GZOUTPUT
                fi
            else
                echo "$GZOUTPUT"
                python3 $SCRIPT_DIR/split_wet_file.py "$DOWNLOADED"
                if [ $? -ne 0 ]; then
                    ls -lh $DOWNLOADED
                    echo "Failed on $DOWNLOADED"
                    rm $DOWNLOADED
                else
                    if [ ! -e "$GZOUTPUT" ]; then
                        touch $GZOUTPUT
                    fi
                fi
            fi
        fi
    fi

    # remove downloaded wet file to save space, only keep output
    if [ -s "$GZOUTPUT" ]; then
        rm -f "$DOWNLOADED"
    fi

}

export -f download_and_parse

CC_VERSION=$1
NJOBS=${2-32}

DOWNLOAD_HOST="https://data.commoncrawl.org"
WET_PATH_URL="https://data.commoncrawl.org/crawl-data/CC-MAIN-CC-VERSION/wet.paths.gz"
PATHS_LST="paths.lst"

date '+%Y-%m-%d %H:%M:%S'


mkdir -p wet.paths

if [[ ! -s "wet.paths/${CC_VERSION}.wet.paths.gz" ]]; then
    wget -q -O wet.paths/${CC_VERSION}.wet.paths.gz ${WET_PATH_URL/CC-VERSION/${CC_VERSION}}
fi

mkdir -p $CC_VERSION

PATHS_LST=${CC_VERSION}/paths.lst

if [ ! -s "$PATHS_LST" ]; then
    if [[ "$OSTYPE" == "darwin"* ]]; then
        gzcat wet.paths/${CC_VERSION}.wet.paths.gz > $PATHS_LST
    else
        zcat wet.paths/${CC_VERSION}.wet.paths.gz > $PATHS_LST
    fi
fi

# verify that we don't have filename collision
NUM_NAMES=$(cat $PATHS_LST | while read FILENAME; do extract_unique_name $FILENAME; done | sort | uniq | wc -l)
if [ $(cat $PATHS_LST | wc -l) -ne $NUM_NAMES ]; then
    echo "Failed because of naming collision before processing files, this could lead to"
    echo "jobs overwrite others' output"
    exit 1
fi

set +e
which parallel
NO_PARALLEL=$?
set -e

if [[ $NO_PARALLEL -eq 1 ]]; then
    cat $PATHS_LST | xargs -I '{}' -P $NJOBS bash -c 'download_and_parse "$@"' _ {} ${CC_VERSION}
else
    parallel --retries 10 --halt now,fail=1 --joblog $CC_VERSION/jobs.log -j $(nproc) -a "$PATHS_LST" download_and_parse {} ${CC_VERSION}
fi

# check if all download was okay
EXPECTED_NUM_FILES=$(cat $PATHS_LST | wc -l)
ACTUAL_NUM_FILES=$(find $CC_VERSION -name '*.pages.jsonl.gz' -mindepth 2 | wc -l)
if [[ $ACTUAL_NUM_FILES -lt $EXPECTED_NUM_FILES ]]; then
    echo "Expecting ${EXPECTED_NUM_FILES} files only got ${ACTUAL_NUM_FILES}"
    echo "Stopped at " $(date '+%Y-%m-%d %H:%M:%S')
else
    find $CC_VERSION -name "CC-MAIN-*" -type d | while read CC_MAIN_DIR; do
        find $CC_MAIN_DIR -name '*.pages.jsonl.gz' -exec cat {} + > ${CC_MAIN_DIR}.warc.wet.pages.jsonl.gz
    done

    echo "Generated $ACTUAL_NUM_FILES Expected $EXPECTED_NUM_FILES"
    echo "Successfully finished at " $(date '+%Y-%m-%d %H:%M:%S')
fi

