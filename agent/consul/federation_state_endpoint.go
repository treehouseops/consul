package consul

import (
	"fmt"
	"time"

	metrics "github.com/armon/go-metrics"
	"github.com/hashicorp/consul/acl"
	"github.com/hashicorp/consul/agent/consul/state"
	"github.com/hashicorp/consul/agent/structs"
	memdb "github.com/hashicorp/go-memdb"
)

// FederationState endpoint is used to manipulate federation states from all
// datacenters.
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
	if rule != nil && rule.OperatorWrite(nil) != acl.Allow {
		return acl.ErrPermissionDenied
	}

	if args.State == nil || args.State.Datacenter == "" {
		return fmt.Errorf("invalid request: missing federation state datacenter")
	}

	switch args.Op {
	case structs.FederationStateUpsert:
		if !c.srv.router.HasDatacenter(args.State.Datacenter) &&
			!c.srv.config.DisableFederationStateDatacenterNameValidation {
			return fmt.Errorf("cannot upsert federation state for datacenter that does not exist: %s", args.State.Datacenter)
		}

		if args.State.UpdatedAt.IsZero() {
			args.State.UpdatedAt = time.Now().UTC()
		}
	case structs.FederationStateDelete:
		// No validation required.
	default:
		return fmt.Errorf("Invalid federation state operation: %v", args.Op)
	}

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

			if len(fedStates) == 0 {
				fedStates = []*structs.FederationState{}
			}

			reply.Index = index
			reply.States = fedStates

			return nil
		})
}
