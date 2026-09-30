FROM alpine:3.21

RUN apk add --no-cache util-linux
COPY cellbox-node-controller /usr/local/bin/cellbox-node-controller
COPY cellbox-runsc-wrapper /usr/local/bin/cellbox-runsc-wrapper
COPY images/controller/runtime.toml /opt/resumablepod/runtime.toml
COPY images/controller/runsc.toml /opt/resumablepod/runsc.toml
COPY images/controller/install-k8s-node.sh /usr/local/bin/install-k8s-node.sh

ENTRYPOINT ["/usr/local/bin/cellbox-node-controller"]
