package consul

import (
	"time"

	metrics "github.com/armon/go-metrics"
	"github.com/hashicorp/consul/acl"
	"github.com/hashicorp/consul/agent/consul/state"
	"github.com/hashicorp/consul/agent/structs"
	memdb "github.com/hashicorp/go-memdb"
)

// TODO: move these under Operator?

// TODO(docs)
type FederationState struct {
	srv *Server
}

func (c *FederationState) Apply(args *structs.FederationStateRequest, reply *bool) error {
	// Ensure that all federation state writes go to the primary datacenter. These will then
	// be replicated to all the other datacenters.
	args.Datacenter = c.srv.config.PrimaryDatacenter

	if done, err := c.srv.forward("FederationState.Apply", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"federation_state", "apply"}, time.Now())

	// Fetch the ACL token, if any.
	rule, err := c.srv.ResolveToken(args.Token)
	if err != nil {
		return err
	}
	// TODO (namespaces) use actual ent authz context
	if rule != nil && rule.OperatorWrite(nil) != acl.Allow {
		return acl.ErrPermissionDenied
	}

	if args.State.UpdatedAt.IsZero() {
		args.State.UpdatedAt = time.Now().UTC()
	}

	args.Op = structs.FederationStateUpsert
	resp, err := c.srv.raftApply(structs.FederationStateRequestType, args)
	if err != nil {
		return err
	}
	if respErr, ok := resp.(error); ok {
		return respErr
	}
	if respBool, ok := resp.(bool); ok {
		*reply = respBool
	}

	return nil
}

func (c *FederationState) Delete(args *structs.FederationStateRequest, reply *bool) error {
	// Ensure that all federation state writes go to the primary datacenter. These will then
	// be replicated to all the other datacenters.
	args.Datacenter = c.srv.config.PrimaryDatacenter

	if done, err := c.srv.forward("FederationState.Delete", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"federation_state", "delete"}, time.Now())

	// Fetch the ACL token, if any.
	rule, err := c.srv.ResolveToken(args.Token)
	if err != nil {
		return err
	}
	// TODO (namespaces) use actual ent authz context
	if rule != nil && rule.OperatorWrite(nil) != acl.Allow {
		return acl.ErrPermissionDenied
	}

	args.Op = structs.FederationStateDelete
	resp, err := c.srv.raftApply(structs.FederationStateRequestType, args)
	if err != nil {
		return err
	}
	if respErr, ok := resp.(error); ok {
		return respErr
	}
	if respBool, ok := resp.(bool); ok {
		*reply = respBool
	}

	return nil
}

func (c *FederationState) Get(args *structs.FederationStateQuery, reply *structs.FederationStateResponse) error {
	if done, err := c.srv.forward("FederationState.Get", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"federation_state", "get"}, time.Now())

	// Fetch the ACL token, if any.
	rule, err := c.srv.ResolveToken(args.Token)
	if err != nil {
		return err
	}
	// TODO (namespaces) use actual ent authz context
	if rule != nil && rule.OperatorRead(nil) != acl.Allow {
		return acl.ErrPermissionDenied
	}

	return c.srv.blockingQuery(
		&args.QueryOptions,
		&reply.QueryMeta,
		func(ws memdb.WatchSet, state *state.Store) error {
			index, fedState, err := state.FederationStateGet(ws, args.Datacenter)
			if err != nil {
				return err
			}

			reply.Index = index
			if fedState == nil {
				return nil
			}

			reply.State = fedState
			return nil
		})
}

func (c *FederationState) List(args *structs.DCSpecificRequest, reply *structs.IndexedFederationStates) error {
	if done, err := c.srv.forward("FederationState.List", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"federation_state", "list"}, time.Now())

	// Fetch the ACL token, if any.
	rule, err := c.srv.ResolveToken(args.Token)
	if err != nil {
		return err
	}
	// TODO (namespaces) use actual ent authz context
	if rule != nil && rule.OperatorRead(nil) != acl.Allow {
		return acl.ErrPermissionDenied
	}

	return c.srv.blockingQuery(
		&args.QueryOptions,
		&reply.QueryMeta,
		func(ws memdb.WatchSet, state *state.Store) error {
			index, fedStates, err := state.FederationStateList(ws)
			if err != nil {
				return err
			}

			reply.Index = index
			if len(fedStates) == 0 {
				reply.States = []*structs.FederationState{}
				return nil
			}

			reply.States = fedStates
			return nil
		})
}
