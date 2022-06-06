FROM python:3.10.4-slim-buster

RUN apt-get update && \
apt-get install -y --no-install-recommends \
        openjdk-11-jre

#TODO
RUN python -m pip install --upgrade pip && pip install pypdf2==2.1.0

RUN mkdir /pypdf2cli
COPY scripts/PyPDF2Cli.py /pypdf2cli
RUN chmod a+x /pypdf2cli/PyPDF2Cli.py

RUN mkdir /tika-bin

#find a more elegant way of grabbing this after we release it
COPY tika-server-standard-2.1.1-SNAPSHOT.jar /tika-bin/tika-server-standard-2.1.1-SNAPSHOT.jar
COPY my-tika-config.xml /tika-bin/my-tika-config.xml
ENTRYPOINT ["java","-cp","/tika-bin/*", "org.apache.tika.server.core.TikaServerCli", "-h", "0.0.0.0", "-c", "/tika-bin/my-tika-config.xml"]
