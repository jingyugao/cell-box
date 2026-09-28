FROM alpine:3.21

RUN apk add --no-cache util-linux
COPY resumablepod-controller /usr/local/bin/resumablepod-controller
COPY resumablepod-runtime /usr/local/bin/resumablepod-runtime
COPY deploy/runtime.toml /opt/resumablepod/runtime.toml
COPY deploy/runsc.toml /opt/resumablepod/runsc.toml
COPY deploy/install-k8s-node.sh /usr/local/bin/install-k8s-node.sh

ENTRYPOINT ["/usr/local/bin/resumablepod-controller"]
