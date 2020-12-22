FROM amd64/openjdk:11.0.8-slim-buster

#wrapper around: https://www.datalogics.com/products/pdf-tools/pdf-checker/
#need to accept license, install it on linux and then tgz the binary
#directory that is installed

#I'm not including pdf-checker.tgz in my repo because of license
#requirements

RUN mkdir /pdfchecker-bin

COPY pdf-checker.tgz /pdfchecker-bin/pdf-checker.tgz
RUN cd /pdfchecker-bin && tar -xzvf pdf-checker.tgz

COPY target/pdfchecker-1.0.0-SNAPSHOT.jar /pdfchecker-1.0.0-SNAPSHOT.jar
ENTRYPOINT ["java","-jar","/pdfchecker-1.0.0-SNAPSHOT.jar"]