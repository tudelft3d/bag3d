# This dockerfile defines the expected runtime environment before the project is installed
FROM 3dfier:lastools
LABEL maintainer="Balázs Dukai <b.dukai@tudelft.nl>"
LABEL description="3dfier image which contains LAZ test files too"

USER root

RUN set -x; \
    wget -O 3dfier.tar.gz -L -q https://api.github.com/repos/tudelft3d/3dfier/tarball/feature/lastools-docker; \
    mkdir /opt/3dfier && mkdir /opt/3dfier/build; \
    tar --strip-components=1 -xf 3dfier.tar.gz -C /opt/3dfier && cd /opt/3dfier/build; \
    cmake -DCMAKE_BUILD_TYPE=Release .. \
    && make \
    && mv /opt/3dfier/build/3dfier /bin/; \
    rm -rf /var/cache/apt/* /tmp/*

RUN useradd -ms /bin/bash balazs
RUN mkdir -p /home/balazs/Development/3dbag
USER balazs
WORKDIR /home/balazs/Development/3dbag