#!/bin/bash

INPUT_FILE=`basename "$INPUT_FILE_PATH"`

deepspeech --model deepspeech-0.9.3-models.pbmm --scorer deepspeech-0.9.3-models.scorer --audio "$INPUT_FILE_PATH" > "$TMP_OUTPUT_DIR/$INPUT_FILE-transcript.txt"
cat "$TMP_OUTPUT_DIR/$INPUT_FILE-transcript.txt"
