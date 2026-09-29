FROM alpine:3.21

RUN apk add --no-cache util-linux
COPY resumablepod-controller /usr/local/bin/resumablepod-controller
COPY resumablepod-runtime /usr/local/bin/resumablepod-runtime
COPY images/controller/runtime.toml /opt/resumablepod/runtime.toml
COPY images/controller/runsc.toml /opt/resumablepod/runsc.toml
COPY images/controller/install-k8s-node.sh /usr/local/bin/install-k8s-node.sh

ENTRYPOINT ["/usr/local/bin/resumablepod-controller"]
