#!/bin/bash -x

while true; do
	val=$(cat /proc/loadavg | cut -d ' ' -f 1)
	./gorrd -f database.rdb put $val $val $val
	sleep 1
done
