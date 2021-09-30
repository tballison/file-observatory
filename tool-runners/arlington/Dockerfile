FROM amd64/openjdk:11.0.8-slim-buster as GRAMMAR_CHECKER_BUILDER

RUN apt-get update && apt-get install g++-8 gcc-8 cmake git -y

RUN update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-8 800 --slave /usr/bin/g++ g++ /usr/bin/g++-8

#TODO: need to tag specific version
RUN git clone https://github.com/pdf-association/arlington-pdf-model /arlington-pdf-model

RUN cd /arlington-pdf-model/TestGrammar && mkdir LinRelease && cd LinRelease && \
    cmake -DCMAKE_BUILD_TYPE=Release .. && make


FROM amd64/openjdk:11.0.8-slim-buster

COPY --from=GRAMMAR_CHECKER_BUILDER /arlington-pdf-model/TestGrammar/bin/linux /arlington-pdf-model/bin

COPY --from=GRAMMAR_CHECKER_BUILDER /arlington-pdf-model/tsv/latest /arlington-pdf-model/tsv/latest

COPY target/arlington-1.0.0-SNAPSHOT.jar /arlington-1.0.0-SNAPSHOT.jar


ENTRYPOINT ["java","-jar","/arlington-1.0.0-SNAPSHOT.jar"]
#WORKDIR /work
# for debugging
# docker run -it --entrypoint /bin/bash --name a2 -v /Users/.../Desktop/tool-runner-work:/data 806db3cdfa81

