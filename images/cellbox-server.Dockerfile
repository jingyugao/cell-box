# Combined HTTP API and single-node runtime image. Build context: dist/release.
FROM moby/buildkit:v0.33.0-rootless AS buildkit
FROM alpine:3.21

ARG TARGETARCH=amd64
RUN apk add --no-cache util-linux ca-certificates \
    && apk add --no-cache --virtual .restic-download-deps bzip2 wget \
    && case "$TARGETARCH" in \
         amd64) restic_arch=amd64; restic_asset_sha256=680838f19d67151adba227e1570cdd8af12c19cf1735783ed1ba928bc41f363d; restic_binary_sha256=01143daba61a1dc8afb0cb3d03ba83e6b118f55b6b146b27473a09efc3df13d6 ;; \
         *) echo "unsupported restic target architecture: $TARGETARCH" >&2; exit 1 ;; \
       esac \
    && wget -q -O /tmp/restic.bz2 "https://github.com/restic/restic/releases/download/v0.18.1/restic_0.18.1_linux_${restic_arch}.bz2" \
    && echo "$restic_asset_sha256  /tmp/restic.bz2" | sha256sum -c - \
    && bunzip2 /tmp/restic.bz2 \
    && echo "$restic_binary_sha256  /tmp/restic" | sha256sum -c - \
    && install -m 0755 /tmp/restic /usr/bin/restic \
    && rm -f /tmp/restic \
    && apk del .restic-download-deps
COPY cellbox-server /opt/cellbox/bin/cellbox-server
COPY cellbox-container-agent /opt/cellbox/bin/cellbox-container-agent
COPY cellbox-runsc-wrapper /opt/cellbox/bin/cellbox-runsc-wrapper
COPY --from=buildkit /usr/bin/buildctl /opt/cellbox/bin/buildctl
COPY --from=buildkit /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

ENV HOME=/tmp
ENV PATH=/host/usr/local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
WORKDIR /var/lib/cellbox-api
EXPOSE 8090
ENTRYPOINT ["/opt/cellbox/bin/cellbox-server"]
