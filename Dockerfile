FROM ubuntu:xenial
MAINTAINER Phaistos Networks

RUN apt update \
	&& apt -y dist-upgrade \
	&& apt -y install git clang++-3.8 make zlib1g-dev \
	&& apt clean


ENV CXX=/usr/bin/clang++-3.8

RUN git clone https://github.com/phaistos-networks/TANK.git
WORKDIR /TANK
RUN make all \
	&& mv tank app /usr/local/bin \
	&& rm -rf /TANK

RUN mkdir -p /data
WORKDIR /data

CMD ["tank","-p",".","-l",":11011"]
