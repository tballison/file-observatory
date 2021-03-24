FROM amd64/openjdk:11.0.8-slim-buster

RUN apt-get update && \
apt-get install -y --no-install-recommends \
      pdfresurrect

COPY target/pdfresurrect-1.0.0-SNAPSHOT.jar /pdfresurrect-1.0.0-SNAPSHOT.jar

ENTRYPOINT ["java","-jar","/pdfresurrect-1.0.0-SNAPSHOT.jar"]

