FROM amd64/openjdk:11.0.8-slim-buster as EXIFTOOL_BUILDER
RUN apt-get update && apt-get install wget -y

RUN mkdir /exiftool && cd /exiftool && wget https://exiftool.org/Image-ExifTool-12.32.tar.gz \
 && tar -xzvf Image-ExifTool-12.32.tar.gz

RUN mv /exiftool/Image-ExifTool-12.32/* /exiftool/ && rmdir /exiftool/Image-ExifTool-12.32

FROM amd64/openjdk:11.0.8-slim-buster as ICC_MAX_BUILDER

RUN apt-get update && apt-get install g++ gcc cmake git libtiff-dev libxml2 libxml2-dev -y
#libxml2 libtiff5 libwxbase3.0-dev -y


#TODO: need to tag specific version
RUN git clone --depth 1 https://github.com/petervwyatt/DemoIccMAX /DemoIccMax

RUN cd /DemoIccMax && \
  mkdir Lin && cd Lin && \
    cmake ../Build/Cmake && \
    make iccDumpProfile

FROM amd64/openjdk:11.0.8-slim-buster

COPY --from=EXIFTOOL_BUILDER /exiftool /exiftool

COPY --from=ICC_MAX_BUILDER /DemoIccMax/Lin/IccProfLib /DemoIccMax/Lin/IccProfLib
COPY --from=ICC_MAX_BUILDER /DemoIccMax/Lin/Tools/IccDumpProfile/iccDumpProfile /DemoIccMax/Lin/Tools/IccDumpProfile/iccDumpProfile

RUN rm -r /DemoIccMax/Lin/IccProfLib/CMakeFiles && \
    rm /DemoIccMax/Lin/IccProfLib/Makefile && \
    rm /DemoIccMax/Lin/IccProfLib/cmake_install.cmake


## apps are /exiftool/exiftool and /IccDumpProfile/iccDumpProfile
# commandline for icc profile: iccDumpProfile -v filename.icc ALL

#find a more elegant way of grabbing this after we release it
RUN mkdir /tika-bin
COPY target/tika-pdfspelunker-1.0.0-SNAPSHOT.jar /tika-bin/
COPY tika-server-standard-2.1.1-SNAPSHOT.jar /tika-bin/
COPY my-tika-config.xml /tika-bin/my-tika-config.xml
ENTRYPOINT ["java","-cp","/tika-bin/*", "org.apache.tika.server.core.TikaServerCli", "-h", "0.0.0.0", "-c", "/tika-bin/my-tika-config.xml"]

#for debugging
# docker run -it --entrypoint /bin/bash --name a2 -v /Users/.../Desktop/tool-runner-work:/data 806db3cdfa81
