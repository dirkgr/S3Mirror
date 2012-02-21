#!/bin/bash

PIDFILE=/tmp/upload-to-s3.pid

test -e $PIDFILE && echo "Found $PIDFILE; script already running?" && exit 1
echo $$ > $PIDFILE

cd /var/lib/transmission-daemon/downloads && \
ls -d * 2> /dev/null | while read i; do
	s3cmd --add-header=x-amz-storage-class:REDUCED_REDUNDANCY -r put "$i" s3://groeneveld-videos > /dev/null && \
	rm -r "$i"
done

rm $PIDFILE
