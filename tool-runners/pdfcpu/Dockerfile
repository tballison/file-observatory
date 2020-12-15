# Dockerfile References: https://docs.docker.com/engine/reference/builder/

# Start from a golang base image
FROM golang:1.15.6 as builder

# install
RUN go get github.com/pdfcpu/pdfcpu/cmd/...
RUN cd $GOPATH/src/github.com/pdfcpu/pdfcpu && \
    git checkout tags/v0.3.7 -b v0.3.7-tag
WORKDIR $GOPATH/src/github.com/pdfcpu/pdfcpu/cmd/pdfcpu
RUN CGO_ENABLED=0 GOOS=linux go build -a -o pdfcpu .

######## Start a new stage from scratch #######

FROM alpine:latest

RUN apk --no-cache add ca-certificates openjdk11

WORKDIR /root/

# Copy the Pre-built binary file from the previous stage
COPY --from=builder /go/src/github.com/pdfcpu/pdfcpu/cmd/pdfcpu .

# Command to run the executable
#CMD ["./pdfcpu"]

COPY target/pdfcpu-1.0.0-SNAPSHOT.jar /pdfcpu-1.0.0-SNAPSHOT.jar

ENTRYPOINT ["java","-jar","/pdfcpu-1.0.0-SNAPSHOT.jar"]
