FROM python:3.8.13-slim-buster

COPY ./aisprint ./aisprint

RUN pip install ./aisprint/

RUN pip install cookiecutter click numpy pandas && \
    rm -rf /root/.cache/pip/* && \
    rm -rf /tmp/*

COPY main.py main.py

