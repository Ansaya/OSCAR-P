FROM python:3.7.12-slim-buster

RUN apt update && \
    apt install -y --no-install-recommends libsndfile1 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN pip install SoundFile && \
    pip install librosa && \
    rm -rf /root/.cache/pip/* && \
    rm -rf /tmp/*

COPY audio_splitter.py .

