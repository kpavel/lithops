FROM kpavel/pywren_runtime:8.0

RUN mkdir /pywren

WORKDIR /pywren

ADD . /pywren

RUN python3 setup.py develop

RUN apt update
RUN apt install telnet

WORKDIR /
