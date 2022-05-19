#!/bin/bash

INPUT_FILE=`basename "$INPUT_FILE_PATH"`

ffmpeg -i "$INPUT_FILE_PATH" -vn -ar 16000 -ac 1 "$TMP_OUTPUT_DIR/$INPUT_FILE"
