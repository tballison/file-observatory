FROM python:3.9.1-slim-buster

RUN pip install pdfminer.six==20201018

RUN mkdir -p /usr/share/man/man1 /usr/share/man/man2

RUN apt-get update && \
apt-get install -y --no-install-recommends \
        openjdk-11-jre

COPY target/pdfminertext-1.0.0-SNAPSHOT.jar /pdfminertext-1.0.0-SNAPSHOT.jar

ENTRYPOINT ["java","-jar","/pdfminertext-1.0.0-SNAPSHOT.jar"]

