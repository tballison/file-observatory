FROM amd64/openjdk:11.0.8-slim-buster as GRAMMAR_CHECKER_BUILDER

RUN apt-get update && apt-get install curl g++-8 gcc-8 cmake git -y

RUN update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-8 800 --slave /usr/bin/g++ g++ /usr/bin/g++-8

RUN git clone  https://github.com/pdf-association/arlington-pdf-model /arlington-pdf-model && \
    cd /arlington-pdf-model && git checkout 35b7e14

RUN cd /arlington-pdf-model/TestGrammar && \
    cmake -B cmake-linux/debug -DPDFSDK_PDFIUM=ON -DCMAKE_BUILD_TYPE=Debug . && \
    cmake --build cmake-linux/debug --config Debug

RUN mkdir /tika-bin && cd /tika-bin && \
    curl https://repo1.maven.org/maven2/org/apache/tika/tika-server-core/2.4.1/tika-server-core-2.4.1.jar --output tika-server-core.jar


FROM amd64/openjdk:11.0.8-slim-buster

COPY --from=GRAMMAR_CHECKER_BUILDER /arlington-pdf-model/TestGrammar/bin/linux /arlington-pdf-model/bin

COPY --from=GRAMMAR_CHECKER_BUILDER /arlington-pdf-model/tsv/latest /arlington-pdf-model/tsv/latest

RUN mkdir /tika-bin
COPY --from=GRAMMAR_CHECKER_BUILDER /tika-bin/tika-server-core.jar /tika-bin/tika-server-core.jar
COPY my-tika-config.xml /tika-bin/my-tika-config.xml

#once we upgrade to > tika 2.4.1, we can get rid of this custom regex parser
COPY target/tika-arlington-1.0.0-SNAPSHOT.jar /tika-bin/tika-arlington.jar

ENTRYPOINT ["java","-cp","/tika-bin/*", "org.apache.tika.server.core.TikaServerCli", "-h", "0.0.0.0", "-c", "/tika-bin/my-tika-config.xml"]
#WORKDIR /work
# for debugging
# docker run -it --entrypoint /bin/bash --name a2 -v /Users/.../Desktop/tool-runner-work:/data 806db3cdfa81

