package consul

import (
	"context"
	"fmt"
	"time"

	"github.com/hashicorp/consul/agent/consul/state"
	"github.com/hashicorp/consul/agent/structs"
	memdb "github.com/hashicorp/go-memdb"
)

// TODO: prune configs in the primary when the corresponding datacenter drops out of the catalog

func (s *Server) startFederationStateAntiEntropy() {
	s.leaderRoutineManager.Start(federationStateAntiEntropyRoutineName, s.federationStateAntiEntropySync)
}

func (s *Server) stopFederationStateAntiEntropy() {
	s.leaderRoutineManager.Stop(federationStateAntiEntropyRoutineName)
}

func (s *Server) federationStateAntiEntropySync(ctx context.Context) error {
	var (
		lastFetchIndex         uint64
		lastPrimaryModifyIndex uint64
	)

	retryLoopBackoff(ctx.Done(), func() error {
		idx, primaryModifyIndex, err := s.federationStateAntiEntropyMaybeSync(lastFetchIndex, lastPrimaryModifyIndex)
		if err != nil {
			return err
		}

		lastFetchIndex = idx
		lastPrimaryModifyIndex = primaryModifyIndex
		return nil
	}, func(err error) {
		s.logger.Printf("[ERR] leader: error performing anti-entropy sync of federation state: %v", err)
	})

	return nil
}

func (s *Server) federationStateAntiEntropyMaybeSync(lastFetchIndex, lastPrimaryModifyIndex uint64) (idx, primaryIdx uint64, err error) {
	queryOpts := &structs.QueryOptions{
		MinQueryIndex:     lastFetchIndex,
		RequireConsistent: true,
	}

	idx, prev, curr, err := s.fetchFederationStateAntiEntropyDetails(queryOpts)
	if err != nil {
		return 0, 0, err
	}

	var prevPrimaryModifyIndex uint64
	if prev != nil {
		prevPrimaryModifyIndex = prev.PrimaryModifyIndex
	}

	if prev != nil && prev.IsSame(curr) {
		s.logger.Printf("[DEBUG] leader: federation state anti-entropy sync skipped; already up to date")
		return idx, prevPrimaryModifyIndex, nil
	}

	if lastPrimaryModifyIndex > 0 && prevPrimaryModifyIndex > 0 &&
		prevPrimaryModifyIndex == lastPrimaryModifyIndex {
		s.logger.Printf("[DEBUG] leader: federation state anti-entropy sync skipped; waiting for replication to locally apply changes lastIdx=%d prev=%d",
			lastPrimaryModifyIndex, prevPrimaryModifyIndex)
		return idx, prevPrimaryModifyIndex, nil
	}

	curr.UpdatedAt = time.Now().UTC()

	args := structs.FederationStateRequest{
		Config: curr,
	}
	ignored := false
	if err := s.forwardDC("FederationState.Apply", s.config.PrimaryDatacenter, &args, &ignored); err != nil {
		return 0, 0, fmt.Errorf("error performing federation state anti-entropy sync: %v", err)
	}

	s.logger.Printf("[INFO] leader: federation state anti-entropy synced")

	return idx, prevPrimaryModifyIndex, nil
}

func (s *Server) fetchFederationStateAntiEntropyDetails(
	queryOpts *structs.QueryOptions,
) (uint64, *structs.FederationState, *structs.FederationState, error) {
	var (
		prevConfig, currConfig *structs.FederationState
		queryMeta              structs.QueryMeta
	)
	err := s.blockingQuery(
		queryOpts,
		&queryMeta,
		func(ws memdb.WatchSet, state *state.Store) error {
			// Get the existing stored version of this config that has replicated down.
			// We could phone home to get this but that would incur extra WAN traffic
			// when we already have enough information locally to figure it out
			// (assuming that our replicator is still functioning).
			idx1, prev, err := state.FederationStateGet(ws, s.config.Datacenter)
			if err != nil {
				return err
			}

			// Fetch our current list of all mesh gateways.
			entMeta := structs.WildcardEnterpriseMeta()
			idx2, raw, err := state.ServiceDump(ws, structs.ServiceKindMeshGateway, true, entMeta)
			if err != nil {
				return err
			}

			curr := &structs.FederationState{
				Datacenter:   s.config.Datacenter,
				MeshGateways: raw,
			}

			if idx2 > idx1 {
				queryMeta.Index = idx2
			} else {
				queryMeta.Index = idx1
			}

			prevConfig = prev
			currConfig = curr

			return nil
		})
	if err != nil {
		return 0, nil, nil, err
	}

	return queryMeta.Index, prevConfig, currConfig, nil
}
