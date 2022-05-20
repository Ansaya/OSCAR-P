import librosa
import sys

audio_file = sys.argv[1]
threshold_db = sys.argv[2]
print(audio_file)
audio, sr = librosa.load(audio_file, sr=22050, mono=True)

clips = librosa.effects.split(audio, top_db=threshold_db)  # change threshold to parameter

with open("timestamps.txt", "a") as file:
    for i in range(len(clips)):
        c = clips[i]
        file.write(str(c[0]) + " " + str(c[1]) + "\n")
