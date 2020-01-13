package agent

import (
	"net/http"
	"strings"

	"github.com/hashicorp/consul/agent/structs"
)

func (s *HTTPServer) DatacenterConfigGet(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	datacenterName := strings.TrimPrefix(req.URL.Path, "/v1/internal/datacenter-config/")
	if datacenterName == "" {
		return nil, BadRequestError{Reason: "Missing datacenter name"}
	}

	args := structs.DatacenterConfigQuery{
		Datacenter: datacenterName,
	}
	if done := s.parse(resp, req, &args.TargetDatacenter, &args.QueryOptions); done {
		return nil, nil
	}

	var out structs.DatacenterConfigResponse
	defer setMeta(resp, &out.QueryMeta)
	if err := s.agent.RPC("DatacenterConfig.Get", &args, &out); err != nil {
		return nil, err
	}

	if out.Config == nil {
		resp.WriteHeader(http.StatusNotFound)
		return nil, nil
	}

	return out, nil
}

func (s *HTTPServer) DatacenterConfigList(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	var args structs.DCSpecificRequest
	if done := s.parse(resp, req, &args.Datacenter, &args.QueryOptions); done {
		return nil, nil
	}

	if args.Datacenter == "" {
		args.Datacenter = s.agent.config.Datacenter
	}

	var out structs.IndexedDatacenterConfigs
	if err := s.agent.RPC("DatacenterConfig.List", &args, &out); err != nil {
		return nil, err
	}

	// make sure we return an array and not nil
	if out.Configs == nil {
		out.Configs = make(structs.DatacenterConfigs, 0)
	}

	return out.Configs, nil
}
