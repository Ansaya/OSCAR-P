#!/bin/bash

echo "#bash_script_start: $(date +"%d-%m-%Y %H:%M:%S.%6N")"
# SUBFOLDER_NAME=`basename "$(dirname "$STORAGE_OBJECT_KEY")"`
# mkdir "$TMP_OUTPUT_DIR/$SUBFOLDER_NAME"

INPUT_FILE=`basename "$INPUT_FILE_PATH"`
# OUTPUT_FILE="$TMP_OUTPUT_DIR/$SUBFOLDER_NAME/$INPUT_FILE"

ffmpeg -i "$INPUT_FILE_PATH" -vn -ar 16000 -ac 1 "$TMP_OUTPUT_DIR/$INPUT_FILE"

echo "#bash_script_end: $(date +"%d-%m-%Y %H:%M:%S.%6N")"
