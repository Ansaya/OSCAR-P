FROM python:3.7.12-slim-buster

RUN apt update && \
	apt install -y --no-install-recommends gcc g++ gfortran ffmpeg pkg-config && \
	apt-get clean && \
	rm -rf /var/lib/apt/lists/*

RUN pip3 install --upgrade pip setuptools wheel

RUN pip3 install --no-cache-dir numpy && \
	rm -rf /root/.cache/pip/* && \
	rm -rf /tmp/*

COPY demo /opt/blurry-faces

COPY openvino /opt/intel/openvino

COPY model /opt/blurry-faces/model

WORKDIR /opt/blurry-faces/
