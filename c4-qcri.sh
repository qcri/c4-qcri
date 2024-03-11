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
    done
}


export -f wget_until_success


function download_and_parse {
    WETPATH=$1

    # download wet file
    BASENAME=$(basename $WETPATH)
    SUBDIR=${BASENAME%-*}
    DOWNLOADED="$SUBDIR/$BASENAME"
    GZOUTPUT="${DOWNLOADED%gz}ara.jsonl.gz"

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

        python3 split_wet_file.py "$DOWNLOADED"
    fi

    # remove downloaded wet file to save space, only keep output
    if [ -s "$GZOUTPUT" ]; then
        rm -f "$DOWNLOADED"
    fi

}

export -f download_and_parse


CC_VERSIONS=(
  $(basename $(pwd))
# "2022-33"
#  "2022-40"
#  "2022-49"
#  "2023-06"
#  "2023-14"
#  "2023-23"
#  "2023-40"
#  "2023-50"
)


DOWNLOAD_HOST="https://data.commoncrawl.org"
WET_PATH_URL="https://data.commoncrawl.org/crawl-data/CC-MAIN-CC-VERSION/wet.paths.gz"

date '+%Y-%m-%d %H:%M:%S'

mkdir -p wet.paths
for CC_VERSION in "${CC_VERSIONS[@]}";
do
    if [[ ! -s "wet.paths/${CC_VERSION}.wet.paths.gz" ]]; then
        wget -q -O wet.paths/${CC_VERSION}.wet.paths.gz ${WET_PATH_URL/CC-VERSION/${CC_VERSION}}
    fi
done


mkdir -p run

if [ ! -s "download_and_split/input.txt" ]; then
    for CC_VERSION in "${CC_VERSIONS[@]}";
    do
        if [[ "$OSTYPE" == "darwin"* ]]; then
            gzcat wet.paths/${CC_VERSION}.wet.paths.gz
        else
            zcat wet.paths/${CC_VERSION}.wet.paths.gz
        fi
    done >> run/input.txt
fi


parallel --joblog download_and_parse.log -j $(nproc) -a run/input.txt download_and_parse

date '+%Y-%m-%d %H:%M:%S'