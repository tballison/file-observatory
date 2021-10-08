FROM amd64/openjdk:11.0.8-slim-buster as EXIFTOOL_BUILDER
RUN apt-get update && apt-get install wget -y

RUN mkdir /exiftool && cd /exiftool && wget https://exiftool.org/Image-ExifTool-12.32.tar.gz \
 && tar -xzvf Image-ExifTool-12.32.tar.gz

RUN mv /exiftool/Image-ExifTool-12.32/* /exiftool/ && rmdir /exiftool/Image-ExifTool-12.32
## app is /exiftool/exiftool



#FROM amd64/openjdk:11.0.8-slim-buster as ICC_MAX_BUILDER

#RUN apt-get update && apt-get install g++-8 gcc-8 cmake git libxml2 libtiff5 libwxbase3.0-dev -y


#TODO: need to tag specific version
#RUN git clone --depth 1 https://github.com/petervwyatt/DemoIccMAX /DemoIccMax

#RUN cd /DemoIccMax && mkdir build && cd build \
 #                     && cmake -DCMAKE_CXX_COMPILER=g++-8 -DCMAKE_INSTALL_PREFIX=$HOME/.local ../Build/Cmake \
  #                    && make && make install
#


#COPY target/tika-pdfchecker-1.0.0-SNAPSHOT.jar /tika-bin/tika-pdfchecker-1.0.0-SNAPSHOT.jar

#find a more elegant way of grabbing this after we release it
#COPY my-tika-config.xml /tika-bin/my-tika-config.xml
ENTRYPOINT ["java","-cp","/tika-bin/*", "org.apache.tika.server.core.TikaServerCli", "-h", "0.0.0.0", "-c", "/tika-bin/my-tika-config.xml"]

#e.g.
#docker run -d -p 9998:9998