FROM python:3.7.12-slim-buster

RUN pip install deepspeech && \
    rm -rf /root/.cache/pip/* && \
    rm -rf /tmp/*

COPY deepspeech-0.9.3-models.pbmm deepspeech-0.9.3-models.pbmm
COPY deepspeech-0.9.3-models.scorer deepspeech-0.9.3-models.scorer

