# Build the manager binary
FROM golang:1.21.5 AS builder

COPY . /build
WORKDIR /build
#RUN go mod download

RUN CGO_ENABLED=0 GOOS=linux go build -a -tags "gocql_debug" -o /build/goapp ./cmd/goapp
#RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -o /build/goapp ./cmd/goapp
RUN CGO_ENABLED=0 GOOS=linux go build -a -tags "gocql_debug" -o /build/goscan ./cmd/goscan

#FROM alpine:3.10
FROM alpine:latest

# Update packages
RUN apk update && apk add vim
COPY --from=builder /build/goapp /usr/local/bin/goapp
RUN chmod +x /usr/local/bin/goapp

COPY ./ClusterConfig*.json /
COPY --from=builder /build/goscan /usr/local/bin/goscan
RUN chmod +x /usr/local/bin/goscan


CMD tail -f /dev/null
