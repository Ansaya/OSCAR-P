FROM python:3.7.12-slim-buster

RUN pip install --no-cache-dir opencv-python==4.4.0.44 numpy==1.19.2 && \
    rm -rf /root/.cache/pip/* && \
    rm -rf /tmp/*

RUN apt update && \
    apt install -y --no-install-recommends libgl1 libglib2.0-0 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir /opt/mask-detector

WORKDIR /opt/mask-detector

COPY mask-detector-image.py .
COPY cfg cfg

