#!bash -ex

CC_VERSIONS=(
 "2022-33"
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


mkdir -p download_and_split

if [ ! -s "download_and_split/input.txt" ]; then
    for CC_VERSION in "${CC_VERSIONS[@]}";
    do
        if [[ "$OSTYPE" == "darwin"* ]]; then
            gzcat wet.paths/${CC_VERSION}.wet.paths.gz
        else
            zcat wet.paths/${CC_VERSION}.wet.paths.gz
        fi
    done >> download_and_split/input.txt
fi


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

    ./download_and_split.sh $START_LINE $END_LINE $i &> $i.log &
done

wait

date '+%Y-%m-%d %H:%M:%S'