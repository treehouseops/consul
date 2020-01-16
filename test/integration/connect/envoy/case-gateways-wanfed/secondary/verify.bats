#!/usr/bin/env bats

load helpers

@test "gateway-secondary proxy admin is up on :19001" {
  retry_default curl -f -s localhost:19001/stats -o /dev/null
}

@test "gateway-secondary should have healthy endpoints for primary" {
   assert_upstream_has_endpoints_in_status 127.0.0.1:19001 primary HEALTHY 1
}

# @test "s2 proxy listener should be up and have right cert" {
#   assert_proxy_presents_cert_uri localhost:21000 s2 secondary
# }

# @test "s2 proxy should be healthy" {
#   assert_service_has_healthy_instances s2 1 secondary
# }

# @test "gateway-secondary is used for the upstream connection" {
#   assert_envoy_metric_at_least 127.0.0.1:19001 "cluster.s2.default.secondary.*cx_total" 1
# }
