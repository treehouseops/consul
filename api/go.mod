module github.com/hashicorp/consul/api

go 1.12

replace github.com/hashicorp/consul/sdk => ../sdk

// NOTE memberlist points to the wan-mgw branch
// replace github.com/hashicorp/memberlist => ../../memberlist

// NOTE serf points to the wan-mgw branch
// replace github.com/hashicorp/serf => ../../serf

require (
	github.com/hashicorp/consul/sdk v0.2.0
	github.com/hashicorp/go-cleanhttp v0.5.1
	github.com/hashicorp/go-rootcerts v1.0.0
	github.com/hashicorp/go-uuid v1.0.1
	github.com/hashicorp/serf v0.8.6-0.20200121220933-379a52af78b6
	github.com/mitchellh/mapstructure v1.1.2
	github.com/stretchr/testify v1.3.0
)
