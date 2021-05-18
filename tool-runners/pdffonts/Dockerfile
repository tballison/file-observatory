#slight modification from:
#https://stackoverflow.com/questions/61272431/installing-poppler-utils-of-version-0-82-in-docker
#See also: https://github.com/quarkness/docker-poppler/blob/master/Dockerfile
FROM amd64/openjdk:11.0.8-slim-buster as POPPLER_BUILDER

#python:3.8-slim-buster as POPPLER_BUILDER
RUN apt-get update && apt-get install bash wget build-essential cmake libfreetype6-dev pkg-config libfontconfig-dev libjpeg-dev libopenjp2-7-dev -y
RUN wget https://poppler.freedesktop.org/poppler-data-0.4.9.tar.gz \
    && tar -xf poppler-data-0.4.9.tar.gz \
    && cd poppler-data-0.4.9 \
    && make install \
    && cd .. \
    && wget https://poppler.freedesktop.org/poppler-0.86.1.tar.xz \
    && tar -xf poppler-0.86.1.tar.xz \
    && cd poppler-0.86.1 \
    && mkdir build \
    && cd build \
    && cmake .. \
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

COPY target/pdffonts-1.0.0-SNAPSHOT.jar /pdffonts-1.0.0-SNAPSHOT.jar


ENTRYPOINT ["java","-jar","/pdffonts-1.0.0-SNAPSHOT.jar"]
#WORKDIR /work

#e.g.
#docker build -t pdftotext-container .
#docker run --name pdftotext-runner -v ~/data/safedocs/evalTwo/tim-staging2/docs:/input:ro -v ~/Desktop/output:/output pdftotext-container