#!/bin/bash

echo "RUNNING PRESETUP"

mkdir -p workdir/tls

(
set -euo pipefail
cd workdir/tls
consul tls ca create
consul tls cert create -dc=primary -server -additional-dnsname='pri.server.primary.consul'
consul tls cert create -dc=secondary -server -additional-dnsname='sec.server.secondary.consul'
)
