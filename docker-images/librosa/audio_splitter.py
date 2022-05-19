import librosa
import soundfile as sf


audio_file = r'output.wav'
audio, sr = librosa.load(audio_file, sr= 22050, mono=True)

clips = librosa.effects.split(audio, top_db=10)

for i in range(len(clips)):
    c = clips[i]
    data = audio[c[0]: c[1]]
    sf.write(str(i) + ".wav", data, sr)
