FROM python:3.9.1-slim-buster

#TODO make more efficient by factoring out a build w git, etc
RUN mkdir -p /usr/share/man/man1 /usr/share/man/man2 /pdfid

RUN apt-get update && \
apt-get install -y --no-install-recommends \
        openjdk-11-jre git

RUN cd /pdfid && \
    git clone https://github.com/DidierStevens/DidierStevensSuite.git didierstevens && \
        cd /pdfid/didierstevens && \
        git checkout 5f81a8f7a8aac15b580413f6f3a2ec3d72c5d10c

COPY target/pdfid-1.0.0-SNAPSHOT.jar /pdfid-1.0.0-SNAPSHOT.jar

ENTRYPOINT ["java","-jar","/pdfid-1.0.0-SNAPSHOT.jar"]

#for debugging
#docker run -it --entrypoint /bin/bash

