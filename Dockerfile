FROM ubuntu:latest

RUN apt-get update && apt-get install -y \
    build-essential \
    binutils \
    git \
    python3 \
    python3-pip \
    python3-venv \
    unzip 

RUN python3 -m venv /venv && /venv/bin/pip install pylint radon 

ENV PATH="/venv/bin:$PATH"