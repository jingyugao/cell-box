FROM alpine:3.21

RUN apk add --no-cache util-linux ca-certificates
COPY cellbox-node-controller /opt/cellbox/bin/cellbox-node-controller

ENTRYPOINT ["/opt/cellbox/bin/cellbox-node-controller"]
