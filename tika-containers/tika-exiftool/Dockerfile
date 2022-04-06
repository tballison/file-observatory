#slight modification from: https://github.com/Miljar/exiftool-docker/blob/master/Dockerfile
FROM amd64/openjdk:11.0.8-slim-buster
ENV EXIFTOOL_VERSION=12.38
ENV TIKA_VERSION=2.2.1

RUN apk add --no-cache perl make
RUN cd /tmp \
	&& wget http://www.sno.phy.queensu.ca/~phil/exiftool/Image-ExifTool-${EXIFTOOL_VERSION}.tar.gz \
	&& tar -zxvf Image-ExifTool-${EXIFTOOL_VERSION}.tar.gz \
	&& cd Image-ExifTool-${EXIFTOOL_VERSION} \
	&& perl Makefile.PL \
	&& make test \
	&& make install \
	&& cd .. \
	&& rm -rf Image-ExifTool-${EXIFTOOL_VERSION}

RUN mkdir /tika-bin \
    && cd /tika-bin \
    && wget https://repo1.maven.org/maven2/org/apache/tika/tika-server-core/${TIKA_VERSION}/tika-server-core-{$TIKA_VERSION}.jar

COPY my-tika-config.xml /tika-bin/my-tika-config.xml
ENTRYPOINT ["java","-cp","/tika-bin/*", "org.apache.tika.server.core.TikaServerCli", "-h", "0.0.0.0", "-c", "/tika-bin/my-tika-config.xml"]

#e.g.
#docker run -d -p 9998:9998