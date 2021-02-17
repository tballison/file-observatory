#slight modification from:
#https://stackoverflow.com/questions/61272431/installing-poppler-utils-of-version-0-82-in-docker
#See also: https://github.com/quarkness/docker-poppler/blob/master/Dockerfile
FROM amd64/openjdk:11.0.8-slim-buster as POPPLER_BUILDER
RUN mkdir /pdfchecker-bin

COPY pdf-checker.tgz /pdfchecker-bin/pdf-checker.tgz
RUN cd /pdfchecker-bin && tar -xzvf pdf-checker.tgz

RUN mkdir /tika-bin
COPY target/tika-pdfchecker-1.0.0-SNAPSHOT.jar /tika-bin/tika-pdfchecker-1.0.0-SNAPSHOT.jar

#find a more elegant way of grabbing this after we release it
COPY tika-server-core-2.0.0-SNAPSHOT.jar /tika-bin/tika-server-core-2.0.0-SNAPSHOT.jar
COPY my-tika-config.xml /tika-bin/my-tika-config.xml
ENTRYPOINT ["java","-cp","/tika-bin/*", "org.apache.tika.server.core.TikaServerCli", "-h", "0.0.0.0", "-c", "/tika-bin/my-tika-config.xml"]

#e.g.
#docker run -d -p 9998:9998