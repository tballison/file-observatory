FROM node:16.13.0

#make sure you have enough memory to build this --memory=900
RUN npm install -g gulp-cli
RUN apt-get update && apt-get -y install wget openjdk-11-jre

#OMG do we really have to build from scratch?
RUN mkdir /builddir && cd /builddir && \
    wget https://github.com/mozilla/pdf.js/archive/refs/tags/v2.11.338.tar.gz && \
    tar -xzvf v2.11.338.tar.gz && cd pdf.js-2.11.338 &&  \
    npm install && gulp dist-install

# figure two stage build and what we can jettison for a smaller container
RUN mkdir /tika-bin/
COPY target/tika-pdfjs-1.0.0-SNAPSHOT.jar /tika-bin/
#find a more elegant way of grabbing this after we release it
COPY tika-server-standard-2.1.1-SNAPSHOT.jar /tika-bin/
COPY my-tika-config.xml /tika-bin/my-tika-config.xml
ENTRYPOINT ["java","-cp","/tika-bin/*", "org.apache.tika.server.core.TikaServerCli", "-h", "0.0.0.0", "-c", "/tika-bin/my-tika-config.xml"]
