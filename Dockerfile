FROM ubuntu:22.04

# install build tools
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    wget \
    unzip \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Install arrow for c++. Instructions from https://arrow.apache.org/install/
RUN apt-get update && apt-get install -y \
    lsb-release \
    && rm -rf /var/lib/apt/lists/*

RUN wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
RUN apt-get update && apt-get install -y \
    ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb \
    && rm -rf /var/lib/apt/lists/*

RUN apt-get update && apt-get install -y \
    libarrow-dev \
    libarrow-glib-dev \
    libarrow-dataset-dev \
    libarrow-dataset-glib-dev \
    libarrow-flight-dev \
    libarrow-flight-glib-dev \
    && rm -rf /var/lib/apt/lists/*

# entrypoint to not exit
CMD ["tail", "-f", "/dev/null"]
