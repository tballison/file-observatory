FROM amd64/openjdk:11.0.8-slim-buster


RUN apt-get update && apt-get install -y libfontconfig1-dev wget

RUN mkdir /pkg && cd /pkg && \
    wget https://dl.xpdfreader.com/xpdf-tools-linux-4.03.tar.gz && \
    tar -xzvf xpdf-tools-linux-4.03.tar.gz && mv xpdf-tools-linux-4.03 /opt/xpdf-tools-linux-4.03

ENV PATH "${PATH}:/opt/xpdf-tools-linux-4.03/bin64"


COPY target/xpdffonts-1.0.0-SNAPSHOT.jar /xpdffonts-1.0.0-SNAPSHOT.jar


ENTRYPOINT ["java","-jar","/xpdffonts-1.0.0-SNAPSHOT.jar"]
#WORKDIR /work

#e.g.
#docker build -t pdftotext-container .
#docker run --name pdftotext-runner -v ~/data/safedocs/evalTwo/tim-staging2/docs:/input:ro -v ~/Desktop/output:/output pdftotext-container