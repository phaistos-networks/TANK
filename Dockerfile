FROM ubuntu:20.04 AS compiler-image
MAINTAINER Phaistos Networks

RUN apt-get update && apt -y --no-install-recommends install git ca-certificates make clang-12 zlib1g-dev libboost1.71-dev g++ libc++-12-dev libc++abi1-12 libc++abi-12-dev
RUN update-alternatives --install /usr/bin/clang++ clang++  /usr/bin/clang++-12 100 && \
	  update-alternatives --install /usr/bin/clang clang  /usr/bin/clang-12 100

RUN mkdir -p /TANK
ADD Makefile *.cpp *.h /TANK/
ADD Switch /TANK/Switch/

WORKDIR /TANK
RUN make -j16 cli-tool service


FROM ubuntu:20.04
MAINTAINER Phaistos Networks
RUN apt-get update && apt -y --no-install-recommends install libc++abi1-12 libc++1-12
COPY --from=compiler-image /TANK/tank /usr/local/bin/
COPY --from=compiler-image /TANK/tank-cli /usr/local/bin/
WORKDIR /data/
CMD ["tank","-p",".","-l",":11011"]
