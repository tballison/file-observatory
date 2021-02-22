#slight modification from:
FROM debian:stretch as CARADOC_BUILDER
RUN apt-get update &&\
    apt-get install -y\
        ocaml\
        opam\
        zlib1g-dev\
        libgmp-dev\
        pkg-config\
        m4\
        zlib1g-dev\
        ocaml-findlib\
        libcryptokit-ocaml-dev\
        libounit-ocaml-dev\
        libcurses-ocaml-dev\
        menhir &&\
    git clone --depth=1 --single-branch https://github.com/caradoc-org/caradoc.git
WORKDIR /caradoc
RUN make


FROM amd64/openjdk:11.0.8-slim-buster
COPY --from=CARADOC_BUILDER /caradoc/_build/src/main.native /usr/local/bin/caradoc
# Install dependencies for caradoc binary
RUN apt-get update &&\
    apt-get install -y\
        libtinfo5\
        libncursesw5


COPY target/caradoc-1.0.0-SNAPSHOT.jar /caradoc-1.0.0-SNAPSHOT.jar
ENTRYPOINT ["java","-jar","/caradoc-1.0.0-SNAPSHOT.jar"]

#e.g.
#debug: docker run -it --entrypoint /bin/bash mutooltotext-container
# docker build -t mutool-clean-image .
# docker run -i -t --name mutool-clean-container -v ~/data/input:/input:ro -v ~/data/output:/output mutool-clean-image /opt/java/openjdk/bin/java -jar /mutoolclean-1.0.0-SNAPSHOT.jar /input /output/table.csv 10