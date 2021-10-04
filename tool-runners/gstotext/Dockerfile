
FROM amd64/openjdk:11.0.8-slim-buster
RUN apt-get update && apt-get install wget -y
                        # &&\
                       #libopenjpeg5
                       #libstdc++6 && \
    #addgroup -S appgroup && \
    #adduser -S appuser -G appgroup -h /work && \
    #echo "/usr/local/lib64:/lib:/usr/local/lib:/usr/lib:/usr/lib/x86_64-linux-gnu:/lib64:/lib/x86_64-linux-gnu" > /etc/ld-musl-x86_64.path
RUN wget https://github.com/ArtifexSoftware/ghostpdl-downloads/releases/download/gs9550/ghostscript-9.55.0-linux-x86_64.tgz \
  && tar -xf ghostscript-9.55.0-linux-x86_64.tgz

COPY target/gstotext-1.0.0-SNAPSHOT.jar /gstotext-1.0.0-SNAPSHOT.jar

ENTRYPOINT ["java","-jar","/gstotext-1.0.0-SNAPSHOT.jar"]
#WORKDIR /work

