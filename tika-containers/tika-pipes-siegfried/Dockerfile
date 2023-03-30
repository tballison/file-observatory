FROM golang:1.20.2-bullseye


RUN apt-get update && apt-get install file  \
    #these are for temurin
    apt-transport-https gnupg -y
RUN go install github.com/richardlehane/siegfried/cmd/sf@latest && sf -update

RUN wget -O - https://packages.adoptium.net/artifactory/api/gpg/key/public | apt-key add - \
    && echo "deb https://packages.adoptium.net/artifactory/deb $(awk -F= '/^VERSION_CODENAME/{print$2}' /etc/os-release) main" | tee /etc/apt/sources.list.d/adoptium.list \
    && apt-get update && apt-get install temurin-11-jre -y

RUN mkdir /tika-bin
COPY target/tika-pipes-siegfried-1.0.0-SNAPSHOT.jar /tika-bin
COPY log4j2.xml /tika-bin
COPY pipes-log4j2.xml /tika-bin


ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

ENTRYPOINT ["java","-Dlog4j.configurationFile=/tika-bin/log4j2.xml", "-jar","/tika-bin/tika-pipes-siegfried-1.0.0-SNAPSHOT.jar"]
#need to specify tika-config.xml on commandline, e.g.:
#docker run -v /Users/blah/Desktop:/data -v /Users/blah/Desktop/config:/tika-config -p 2345:2345
#--name tika-pipes-container tika-pipes-pdfinfo /tika-config/my-tika-config.xml

#WORKDIR /work

