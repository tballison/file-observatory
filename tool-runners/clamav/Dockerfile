#slight modification from:
#https://github.com/mko-x/docker-clamav/blob/master/alpine/main/Dockerfile
FROM alpine:3.12
LABEL maintainer="Markus Kosmal <code@m-ko.de>"

RUN apk add --no-cache openjdk11 bash clamav clamav-daemon rsyslog wget clamav-libunrar

COPY conf /etc/clamav

RUN mkdir /var/run/clamav && \
    chown clamav:clamav /var/run/clamav && \
    chmod 750 /var/run/clamav
    #&& \
    #chown -R clamav:clamav bootstrap.sh check.sh /etc/clamav && \
    #chmod u+x bootstrap.sh check.sh

RUN /usr/bin/freshclam
#EXPOSE 3310/tcp

COPY target/clamav-1.0.0-SNAPSHOT.jar /clamav-1.0.0-SNAPSHOT.jar
COPY exec.sh /exec.sh
RUN ["chmod", "+x", "/exec.sh"]
CMD ["/exec.sh"]
