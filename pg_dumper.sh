#!/bin/bash

HOST=$1
USER=$2
PASS=$3
DB=$4
NOW=`date +%Y%m%d%H%M%S`
HERE=`pwd`
DIR="$HERE/${DB}_${NOW}"
export PGPASSWORD=$PASS

N=6

function log {
        now=`date +'%Y-%m-%d %H:%M:%S'`
        echo "[$now] $1"
}


mkdir $DIR

for tbl in `psql -h $HOST -U $USER $DB -c '\dt' | grep '|' | grep -v 'Name' | awk '{ print $3; }'`; do 
        (
                log "-> Starting $tbl"
                pg_dump -f $DIR/$tbl.dump -h $HOST -U $USER $DB -t $tbl
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

