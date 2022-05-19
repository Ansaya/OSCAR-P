#!/bin/bash

INPUT_FILE=`basename "$INPUT_FILE_PATH"`

# decompress tar archive, containing timestamps.txt and a video file

filename='timestamps.txt'
n=0
while read line; do
n=$((n+1))
array=($line)
echo ${array[0]}
echo ${array[1]}
ffmpeg -ss ${array[0]} -to ${array[1]} -i $INPUT_FILE -c copy "${INPUT_FILE%.mp4}$n.mp4"
done < $filename
