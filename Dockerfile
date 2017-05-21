FROM alpine:latest
MAINTAINER Phaistos Networks

RUN apk add --update \
	    zlib-dev \
	    make \
	    g++ \
	    jemalloc \
	    && rm -rf /var/cache/apk/*

RUN mkdir -p /TANK
ADD Makefile *.cpp *.h /TANK/
ADD Switch /TANK/Switch/

RUN cd /TANK \
	&& sed -i -e s#-Wno-invalid-source-encoding##  Makefile \
	&& make all \
	&& mv tank tank-cli /usr/local/bin \
	&& rm -rf /TANK

RUN mkdir -p /data/test/0
WORKDIR /data

CMD ["tank","-p",".","-l",":11011"]
