#!/bin/bash

TLS_DIR="workdir/tls/${CASE_DIR}"

rm -rf "${TLS_DIR}"
mkdir -p "${TLS_DIR}"

(
set -euo pipefail
cd "${TLS_DIR}"
consul tls ca create
consul tls cert create -dc=primary -server -additional-dnsname='pri.server.primary.consul'
consul tls cert create -dc=secondary -server -additional-dnsname='sec.server.secondary.consul'
)
