node_name = "pri"
connect {
  enabled                            = true
  enable_mesh_gateway_wan_federation = true
}
ca_file                = "/workdir/tls/case-wanfed-gw/consul-agent-ca.pem"
cert_file              = "/workdir/tls/case-wanfed-gw/primary-server-consul-0.pem"
key_file               = "/workdir/tls/case-wanfed-gw/primary-server-consul-0-key.pem"
verify_incoming        = true
verify_outgoing        = true
verify_server_hostname = true
