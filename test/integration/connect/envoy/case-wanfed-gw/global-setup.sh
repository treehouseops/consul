#!/bin/bash

set -euo pipefail

TLS_DIR="workdir/tls/${CASE_NAME}"

rm -rf "${TLS_DIR}"
mkdir -p "${TLS_DIR}"

readonly container="consul-envoy-integ-tls-init--${CASE_NAME}"

readonly scriptlet="
mkdir /out ;
cd /out ;
consul tls ca create ;
consul tls cert create -dc=primary -server -additional-dnsname='pri.server.primary.consul' ;
consul tls cert create -dc=secondary -server -additional-dnsname='sec.server.secondary.consul'
"

docker rm -f "$container" &>/dev/null || true
docker run -i --net=none --name="$container" consul-dev:latest sh -c "${scriptlet}"
output_files=(
    consul-agent-ca-key.pem
    consul-agent-ca.pem
    primary-server-consul-0-key.pem
    primary-server-consul-0.pem
    secondary-server-consul-0-key.pem
    secondary-server-consul-0.pem
)
for f in "${output_files[@]}"; do
    echo "copying $f out of container..."
    docker cp "${container}:/out/$f" "${TLS_DIR}"
done
docker rm -f "$container" >/dev/null || true
echo "clean exit"
