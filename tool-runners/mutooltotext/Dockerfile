#slight modification from:
#https://github.com/jay-eff/mutool/blob/master/Dockerfile
FROM alpine:3 as MUPDF_BUILDER
MAINTAINER Jens Fischer

# install necessary packages and compile MuPDF, clean up afterwards
# include bash for debugging the build only

#get tags from here: http://git.ghostscript.com/?p=mupdf.git;a=summary
#versions 1.17.0, 1.16.1, 1.16.0, 1.15.0, 1.14.0, 1.13.0, 1.12.0, 1.11.1
RUN apk add --no-cache \
        git \
        make \
        pkgconfig \
        build-base \
        bash \
	&& git clone -b 1.17.0 git://git.ghostscript.com/mupdf.git \
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

FROM maven:3.6.0-jdk-11-slim AS MAVEN_BUILD
COPY pom.xml /home/app/pom.xml

COPY src /home/app/src
RUN mvn -f /home/app/pom.xml clean package

FROM adoptopenjdk/openjdk11:alpine-slim
COPY --from=MUPDF_BUILDER /usr/local/bin /usr/local/bin
COPY --from=MUPDF_BUILDER /lib /lib

COPY --from=MAVEN_BUILD /home/app/target/mutooltotext-1.0-SNAPSHOT.jar /mutooltotext-1.0-SNAPSHOT.jar
#ENTRYPOINT ["java","-jar","/mutooltotext-1.0-SNAPSHOT.jar", "/input", "/output"]
#RUN apk update && apk add bash
#e.g.
#debug: docker run -it --entrypoint /bin/bash mutooltotext-container
#docker build -t mutooltotext-container .
