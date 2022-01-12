#slight modification from:
#https://stackoverflow.com/questions/61272431/installing-poppler-utils-of-version-0-82-in-docker
#See also: https://github.com/quarkness/docker-poppler/blob/master/Dockerfile
FROM debian:buster-20211220-slim as POPPLER_BUILDER
## can't build w 22.01.0 with the current dependencies...need to figure out how to
# migrate to 22.x
ENV POPPLER_VERSION=21.12.0
ENV POPPLER_DATA_VERSION=0.4.11
RUN apt-get update && apt-get install bash wget build-essential cmake libfreetype6-dev pkg-config libfontconfig-dev libjpeg-dev libopenjp2-7-dev -y
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

FROM amd64/openjdk:11.0.8-slim-buster
COPY --from=POPPLER_BUILDER /usr/lib /usr/lib
COPY --from=POPPLER_BUILDER /usr/local /usr/local

RUN apt-get update && apt-get install bash ca-certificates \
                       libjpeg62-turbo libcairo2 libxml2 \
                       fontconfig liblcms2-2 \
                       libtiff5 -y
                        # &&\
                       #libopenjpeg5
                       #libstdc++6 && \
    #addgroup -S appgroup && \
    #adduser -S appuser -G appgroup -h /work && \
    #echo "/usr/local/lib64:/lib:/usr/local/lib:/usr/lib:/usr/lib/x86_64-linux-gnu:/lib64:/lib/x86_64-linux-gnu" > /etc/ld-musl-x86_64.path

COPY target/pdfimages-1.0.0-SNAPSHOT.jar /pdfimages-1.0.0-SNAPSHOT.jar


ENTRYPOINT ["java","-jar","/pdfimages-1.0.0-SNAPSHOT.jar"]
#WORKDIR /work

