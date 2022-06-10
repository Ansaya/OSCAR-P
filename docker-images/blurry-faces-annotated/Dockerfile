FROM python:3.8.13-slim-buster

COPY ./aisprint ./aisprint

RUN pip install ./aisprint/

RUN pip install --no-cache-dir opencv-python==4.4.0.44 numpy==1.19.2 tensorflow cookiecutter click numpy pandas && \
    rm -rf /root/.cache/pip/* && \
    rm -rf /tmp/*

RUN apt update && \
    apt install -y --no-install-recommends ffmpeg && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY . /opt/blurry-faces

WORKDIR /opt/blurry-faces/src

