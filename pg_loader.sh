#!/bin/bash

DIR=$1
HOST=$2
USER=$3
PASS=$4
DB=$5
export PGPASSWORD=$PASS

N=6

function log {
        now=`date +'%Y-%m-%d %H:%M:%S'`
        echo "[$now] $1"
}


for f in $DIR/*; do
        fname=$(basename $f)
        tbl=${fname::-5}
        (
                log "-> Loading $tbl"
                psql -h $HOST -U $USER -f $DIR/$fname -q $DB >> /dev/null
                log "<- Done $tbl"
        ) &
        # allow to execute up to $N jobs in parallel
        if [[ $(jobs -r -p | wc -l) -ge $N ]]; then
                # now there are $N jobs already running, so wait here for any job
                # to be finished so there is a place to start next one.
                wait -n
        fi
done

wait

