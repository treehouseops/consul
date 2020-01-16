#!/usr/bin/env bats

load helpers

@test "gateway-primary proxy admin is up on :19000" {
  retry_default curl -f -s localhost:19000/stats -o /dev/null
}

@test "gateway-primary should have healthy endpoints for secondary" {
   assert_upstream_has_endpoints_in_status 127.0.0.1:19000 secondary HEALTHY 1
}

# @test "gateway-primary is used for the upstream connection" {
#   assert_envoy_metric_at_least 127.0.0.1:19002 "cluster.secondary.*cx_total" 1
# }
