#!/bin/bash

python main.py

TMP=$(cat /proc/sys/kernel/random/uuid | sed 's/[-]//g' | head -c 5)

cp "influxdb.csv" "$TMP_OUTPUT_DIR/dummy_${TMP}_influxdb.csv"
