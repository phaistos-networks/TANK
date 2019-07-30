FROM ubuntu:18.04 AS compiler-image
MAINTAINER Phaistos Networks

RUN mkdir -p /TANK
ADD Makefile *.cpp *.h /TANK/
ADD Switch /TANK/Switch/

RUN apt-get update && apt -y --no-install-recommends install git ca-certificates make clang-8 zlib1g-dev libboost1.65-dev g++
RUN update-alternatives --install /usr/bin/clang++ clang++  /usr/bin/clang++-8 100 && \
	  update-alternatives --install /usr/bin/clang clang  /usr/bin/clang-8 100

WORKDIR /TANK
RUN make -j16 cli-tool service



FROM ubuntu:18.04
MAINTAINER Phaistos Networks
COPY --from=compiler-image /TANK/tank /usr/local/bin/
COPY --from=compiler-image /TANK/tank-cli /usr/local/bin/
CMD ["tank","-p",".","-l",":11011"]
