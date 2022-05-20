import librosa
import sys
import time
import math


def samples_to_timestamp(sample, is_start):
    time_in_seconds = sample / 22050
    if is_start:
        time_in_seconds = math.floor(time_in_seconds)
    else:
        time_in_seconds = math.ceil(time_in_seconds)
    formatted_time = time.strftime('%H:%M:%S', time.gmtime(time_in_seconds))
    return formatted_time


audio_file = sys.argv[1]
threshold_db = int(sys.argv[2])
audio, sr = librosa.load(audio_file, sr=22050, mono=True)

clips = librosa.effects.split(audio, top_db=threshold_db)

with open("timestamps.txt", "a") as file:
    for i in range(len(clips)):
        c = clips[i]
        start = samples_to_timestamp(c[0], True)
        end = samples_to_timestamp(c[1], False)
        file.write(start + " " + end + "\n")
