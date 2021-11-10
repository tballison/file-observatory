#slight modification from:
#https://stackoverflow.com/questions/61272431/installing-poppler-utils-of-version-0-82-in-docker
#See also: https://github.com/quarkness/docker-poppler/blob/master/Dockerfile
FROM amd64/openjdk:11.0.8-slim-buster as POPPLER_BUILDER
#poppler/data pairs
#21.02.0/0.4.10
#20.09.0/0.4.9
#0.86.1/0.4.9

RUN apt-get update && apt-get install bash wget build-essential cmake libfreetype6-dev pkg-config libfontconfig-dev libjpeg-dev libopenjp2-7-dev -y
RUN wget https://poppler.freedesktop.org/poppler-data-0.4.11.tar.gz \
    && tar -xf poppler-data-0.4.11.tar.gz \
    && cd poppler-data-0.4.11 \
    && make install \
    && cd .. \
    && wget https://poppler.freedesktop.org/poppler-21.11.0.tar.xz \
    && tar -xf poppler-21.11.0.tar.xz \
    && cd poppler-21.11.0 \
    && mkdir build \
    && cd build \
    && cmake -DENABLE_BOOST=OFF .. \
    && make \
    && make install \
    && ldconfig
#CMD tail -f /dev/null

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

RUN mkdir /tika-bin

#find a more elegant way of grabbing this after we release it
COPY tika-server-standard-2.1.1-SNAPSHOT.jar /tika-bin/tika-server-standard-2.1.1-SNAPSHOT.jar
COPY my-tika-config.xml /tika-bin/my-tika-config.xml
ENTRYPOINT ["java","-cp","/tika-bin/*", "org.apache.tika.server.core.TikaServerCli", "-h", "0.0.0.0", "-c", "/tika-bin/my-tika-config.xml"]

#e.g.
#docker run -d -p 9998:9998