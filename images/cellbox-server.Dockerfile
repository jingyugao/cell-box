# Combined HTTP API and single-node runtime image. Build context: dist/release.
FROM moby/buildkit:v0.33.0-rootless AS buildkit
FROM alpine:3.21

RUN apk add --no-cache util-linux ca-certificates
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
