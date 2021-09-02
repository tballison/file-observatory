FROM amd64/openjdk:11.0.8-slim-buster as GRAMMAR_CHECKER_BUILDER

RUN apt-get update && apt-get install g++-8 gcc-8 cmake -y

RUN update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-8 800 --slave /usr/bin/g++ g++ /usr/bin/g++-8
#TODO pull from github when this is publicly available
copy grammar/TestGrammar /grammar/TestGrammar
RUN cd /grammar/TestGrammar && cmake . && make


FROM amd64/openjdk:11.0.8-slim-buster

COPY --from=GRAMMAR_CHECKER_BUILDER /grammar/TestGrammar/bin/linux /grammar/

COPY grammar/tsv/latest /grammar/tsv/latest

COPY target/arlington-1.0.0-SNAPSHOT.jar /arlington-1.0.0-SNAPSHOT.jar


ENTRYPOINT ["java","-jar","/arlington-1.0.0-SNAPSHOT.jar"]
#WORKDIR /work

