#slight modification from:
#https://stackoverflow.com/questions/61272431/installing-poppler-utils-of-version-0-82-in-docker
#See also: https://github.com/quarkness/docker-poppler/blob/master/Dockerfile
FROM debian:bullseye-20230227-slim as POPPLER_BUILDER
## can't build w 22.01.0 with the current dependencies...need to figure out how to
# migrate to 22.x
ENV POPPLER_VERSION=23.03.0
ENV POPPLER_DATA_VERSION=0.4.12
RUN apt-get update && apt-get install bash wget build-essential cmake libfreetype6-dev pkg-config  \
    libfontconfig-dev libjpeg-dev libopenjp2-7-dev  \
    #these are for temurin
    apt-transport-https gnupg -y
RUN wget https://poppler.freedesktop.org/poppler-data-${POPPLER_DATA_VERSION}.tar.gz \
    && tar -xf poppler-data-${POPPLER_DATA_VERSION}.tar.gz \
    && cd poppler-data-${POPPLER_DATA_VERSION} \
    && make install \
    && cd .. \
    && wget https://poppler.freedesktop.org/poppler-${POPPLER_VERSION}.tar.xz \
    && tar -xf poppler-${POPPLER_VERSION}.tar.xz \
    && cd poppler-${POPPLER_VERSION} \
    && mkdir build \
    && cd build  \
    && cmake -DENABLE_BOOST=OFF ..\
    && make \
    && make install \
    && ldconfig

RUN wget -O - https://packages.adoptium.net/artifactory/api/gpg/key/public | apt-key add - \
    && echo "deb https://packages.adoptium.net/artifactory/deb $(awk -F= '/^VERSION_CODENAME/{print$2}' /etc/os-release) main" | tee /etc/apt/sources.list.d/adoptium.list \
    && apt-get update && apt-get install temurin-11-jre -y

COPY target/pdfinfo-1.0.0-SNAPSHOT.jar /pdfinfo-1.0.0-SNAPSHOT.jar


ENTRYPOINT ["java","-jar","/pdfinfo-1.0.0-SNAPSHOT.jar"]
#WORKDIR /work

