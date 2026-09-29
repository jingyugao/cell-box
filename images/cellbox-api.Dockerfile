# Kubernetes API image: build with dist/release as context after make build.
# The Docker provider runs on the host with the Docker CLI; this image is for Kubernetes.
FROM moby/buildkit:v0.33.0-rootless AS buildkit
FROM scratch
COPY cellbox /usr/local/bin/cellbox
COPY cellbox-guest /usr/local/bin/cellbox-guest
COPY --from=buildkit /usr/bin/buildctl /usr/local/bin/buildctl
USER 10001:10001
WORKDIR /var/lib/cellbox
EXPOSE 8090
ENTRYPOINT ["/usr/local/bin/cellbox"]
