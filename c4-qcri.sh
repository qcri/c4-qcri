#!bash -ex

# c4-qcri.sh
# QCRI's script to download and clean Arabic data from CommonCrawl. It does
# - download a given CommonCrawl dump (WET files)
# - parse and extract only pages with 'ara' in identified languages
# - output is gzipped jsonl format
#
# by Yifan Zhang (yzhang@hbku.edu.qa)
# Copyright (C) 2024, Qatar Computing Research Institute


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


function download_and_parse {
    WETPATH=$1

    # download wet file
    BASENAME=$(basename $WETPATH)
    SUBDIR=${BASENAME%-*}
    DOWNLOADED="$SUBDIR/$BASENAME"
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
            python3 split_wet_file.py "$DOWNLOADED"
        fi
    fi

    # remove downloaded wet file to save space, only keep output
    if [ -s "$GZOUTPUT" ]; then
        rm -f "$DOWNLOADED"
    fi

}

export -f download_and_parse


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
    for CC_VERSION in "${CC_VERSIONS[@]}";
    do
        if [[ "$OSTYPE" == "darwin"* ]]; then
            gzcat wet.paths/${CC_VERSION}.wet.paths.gz
        else
            zcat wet.paths/${CC_VERSION}.wet.paths.gz
        fi
    done >> "$PATHS_LST"
fi

parallel --joblog $CC_VERSION/jobs.log -j $(nproc) -a "$PATHS_LST" download_and_parse

for CC_MAIN_DIR in $CC_VERSION/CC-MAIN-*; do
    cat $CC_MAIN_DIR/*.gz > ${CC_MAIN_DIR}.warc.wet.pages.jsonl.gz
done

date 'Finished at +%Y-%m-%d %H:%M:%S'
