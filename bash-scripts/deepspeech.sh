#!/bin/bash

echo "#bash_script_start: $(date +"%d-%m-%Y %H:%M:%S.%6N")"
# SUBFOLDER_NAME=`basename "$(dirname "$STORAGE_OBJECT_KEY")"`
# mkdir "$TMP_OUTPUT_DIR/$SUBFOLDER_NAME"

INPUT_FILE=`basename "$INPUT_FILE_PATH"`
# OUTPUT_FILE="$TMP_OUTPUT_DIR/$SUBFOLDER_NAME/$INPUT_FILE"

deepspeech --model deepspeech-0.9.3-models.pbmm --scorer deepspeech-0.9.3-models.scorer --audio "$INPUT_FILE_PATH" > "$TMP_OUTPUT_DIR/$INPUT_FILE-transcript.txt"
cat "$TMP_OUTPUT_DIR/$INPUT_FILE-transcript.txt"
echo "#bash_script_end: $(date +"%d-%m-%Y %H:%M:%S.%6N")"
