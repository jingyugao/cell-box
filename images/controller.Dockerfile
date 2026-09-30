FROM alpine:3.21

RUN apk add --no-cache util-linux curl ca-certificates bzip2 coreutils
COPY cellbox-node-controller /opt/cellbox/bin/cellbox-node-controller
COPY cellbox-runsc-wrapper /usr/local/bin/cellbox-runsc-wrapper
COPY cellbox-runtime-config /usr/local/bin/cellbox-runtime-config
COPY images/controller/runsc.toml /opt/cellbox/runsc.toml
COPY images/controller/install-node.sh /usr/local/bin/install-node.sh
COPY images/controller/install-gvisor.sh /usr/local/bin/install-gvisor.sh

ENTRYPOINT ["/opt/cellbox/bin/cellbox-node-controller"]
