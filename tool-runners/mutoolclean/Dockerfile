#slight modification from:
#https://github.com/jay-eff/mutool/blob/master/Dockerfile
FROM alpine:3 as MUTOOL_BUILDER
MAINTAINER Jens Fischer

# install necessary packages and compile MuPDF, clean up afterwards
# include bash for debugging the build only

#get tags from here: http://git.ghostscript.com/?p=mupdf.git;a=summary
#versions 1.18.0 1.17.0, 1.16.1, 1.16.0, 1.15.0, 1.14.0, 1.13.0, 1.12.0, 1.11.1
RUN apk add --no-cache \
        git \
        make \
        pkgconfig \
        build-base \
        bash \
	&& git clone -b 1.18.0 git://git.ghostscript.com/mupdf.git \
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
COPY --from=MUTOOL_BUILDER /usr/local/bin /usr/local/bin
COPY --from=MUTOOL_BUILDER /lib /lib

COPY target/mutoolclean-1.0.0-SNAPSHOT.jar /mutoolclean-1.0.0-SNAPSHOT.jar

#e.g.
#debug: docker run -it --entrypoint /bin/bash mutooltotext-container
# docker build -t mutool-clean-image .
# docker run -i -t --name mutool-clean-container -v ~/data/input:/input:ro -v ~/data/output:/output mutool-clean-image /opt/java/openjdk/bin/java -jar /mutoolclean-1.0.0-SNAPSHOT.jar /input /output/table.csv 10