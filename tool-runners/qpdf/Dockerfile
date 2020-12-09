#fortunately the latest is available prebuilt (for now)
# for future reference, start with something like this
   #curl g++ \
  ##	&& curl -o qpdf-10.0.1.tgz https://gigenet.dl.sourceforge.net/project/qpdf/qpdf/10.0.1/qpdf-10.0.1.tar.gz \
  # #   && tar -xzvf qpdf-10.0.1.tgz
  #
  ##RUN cd qpdf-10.0.1 && \
  # #   ./configure
  #
  ##RUN make install



RUN apk add --no-cache \
    qpdf=10.0.1-r0

COPY target/qpdf-1.0.0-SNAPSHOT.jar /qpdf-1.0.0-SNAPSHOT.jar

ENTRYPOINT ["java","-jar","/qpdf-1.0.0-SNAPSHOT.jar"]


# e.g.
# docker build -t qpdf-image .

# docker run --name qpdf-container --network host --env-file env.properties -v /data/docs:/input -v /data/meta/qpdf/json:/output