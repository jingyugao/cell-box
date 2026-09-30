# Kubernetes API image: build with dist/release as context after make build.
# The Docker provider runs on the host with the Docker CLI; this image is for Kubernetes.
FROM moby/buildkit:v0.33.0-rootless AS buildkit
FROM scratch
COPY cellbox-api /usr/local/bin/cellbox-api
COPY cellbox-container-agent /usr/local/bin/cellbox-container-agent
COPY --from=buildkit /usr/bin/buildctl /usr/local/bin/buildctl
COPY --from=buildkit /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
USER 10001:10001
ENV HOME=/tmp
WORKDIR /var/lib/cellbox
EXPOSE 8090
ENTRYPOINT ["/usr/local/bin/cellbox-api"]
