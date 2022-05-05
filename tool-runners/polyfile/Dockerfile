# this should be cleaned up dramatically
# I tried to build polyfile in a base container and then
# copy the right bits into the final image, but I couldn't figure
# out how to get all the dependencies...so this is backwards
# from the other docker files: build the java first, then
# copy that jar into the build container for polyfile.

FROM python:3.10.4-alpine3.15
RUN apk add --no-cache \
 #       git \
        bash \
        libffi-dev \
        zlib \
        build-base py-pip jpeg-dev zlib-dev \
        openjdk11-jre
#	&& git clone -b v0.1.6 https://github.com/trailofbits/polyfile.git


ENV LIBRARY_PATH=/lib:/usr/lib

#RUN cd polyfile && pip3 install -e .

RUN pip3 install polyfile==0.4.1

COPY target/polyfile-1.0.0-SNAPSHOT.jar /polyfile-1.0.0-SNAPSHOT.jar

ENTRYPOINT ["java","-jar","/polyfile-1.0.0-SNAPSHOT.jar"]
