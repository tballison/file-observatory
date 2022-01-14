#slight modification from:
#https://github.com/jay-eff/mutool/blob/master/Dockerfile
FROM alpine:3 as MUPDF_BUILDER
MAINTAINER Jens Fischer

# install necessary packages and compile MuPDF, clean up afterwards
# include bash for debugging the build only

#get tags from here: http://git.ghostscript.com/?p=mupdf.git;a=summary
#versions 1.19.0 1.18.0 1.17.0, 1.16.1, 1.16.0, 1.15.0, 1.14.0, 1.13.0, 1.12.0, 1.11.1
ENV MUTOOL_VERSION 1.19.0
RUN apk add --no-cache \
        git \
        make \
        pkgconfig \
        build-base \
        bash \
	&& git clone -b ${MUTOOL_VERSION} https://github.com/ArtifexSoftware/mupdf \
        && cd mupdf \
        && git submodule update --init \
        && make HAVE_X11=no HAVE_GLUT=no prefix=/usr/local install \
        && cd / \
        && rm -r mupdf \
        && apk del \
        git \
        make \
        pkgconfig \
        build-base

FROM adoptopenjdk/openjdk11:alpine-slim
COPY --from=MUPDF_BUILDER /usr/local/bin /usr/local/bin
COPY --from=MUPDF_BUILDER /lib /lib

COPY target/mutooltext-1.0.0-SNAPSHOT.jar /mutooltext-1.0.0-SNAPSHOT.jar
ENTRYPOINT ["java","-jar","/mutooltext-1.0.0-SNAPSHOT.jar"]
#RUN apk update && apk add bash
# e.g.
# docker build -t mutool-text-image .
# docker run -i -t --name mutool-text-container -v ~/data/input:/input:ro -v ~/data/output:/output mutool-text-image /opt/java/openjdk/bin/java -jar /mutooltotext-1.0.0-SNAPSHOT.jar /input /output/txt /output/table.csv 10
