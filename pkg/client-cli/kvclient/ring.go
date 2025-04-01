package kvclient

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	pb "Distributed-Key-Value-Store/kvstore/proto"
	"Distributed-Key-Value-Store/pkg/consistenthash"
)

// updateRingState fetches the current ring state from any available server
func (c *Client) updateRingState() error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Try to get ring state from any available server
	var lastErr error
	for addr, client := range c.clients {
		ctx, cancel := context.WithTimeout(context.Background(), c.config.Timeout)
		resp, err := client.GetRingState(ctx, &pb.RingStateRequest{})
		cancel()

		if err != nil {
			lastErr = err
			log.Printf("Failed to get ring state from %s: %v", addr, err)
			continue
		}

		// Successfully got ring state
		c.ringMu.Lock()

		// Only update if this is a newer version
		if resp.Version > c.ringVersion {
			// Create a new ring instead of resetting the existing one
			c.ring = consistenthash.NewRing(10)

			// Add all nodes to the ring
			for nodeID := range resp.Nodes {
				c.ring.AddNode(nodeID)

				// If we don't have this node's address yet, use a default one
				// This will be updated when we connect to the node
				if _, exists := c.nodeAddresses[nodeID]; !exists {
					c.nodeAddresses[nodeID] = c.findBestAddressForNode(nodeID)
				}
			}

			c.ringVersion = resp.Version
			// log.Printf("Updated ring state: version=%d, nodes=%d",
			// 	c.ringVersion, len(resp.Nodes))
		} else {
			log.Printf("Ignoring ring state update: current version %d >= received version %d",
				c.ringVersion, resp.Version)
		}

		c.ringMu.Unlock()
		return nil
	}

	if lastErr != nil {
		return fmt.Errorf("failed to get ring state from any server: %v", lastErr)
	}
	return fmt.Errorf("no servers available to get ring state")
}

// findBestAddressForNode tries to find the best server address for a node ID
func (c *Client) findBestAddressForNode(nodeID string) string {
	// Try to extract address from our connections
	found := false
	var bestAddr string

	// Remove all "node-" prefixes to get the clean node ID
	cleanID := nodeID
	for strings.HasPrefix(cleanID, "node-") {
		cleanID = strings.TrimPrefix(cleanID, "node-")
	}

	for serverAddr := range c.clients {
		// This is a simplistic approach - in a real system you'd have
		// a more robust way to map node IDs to addresses
		if strings.Contains(serverAddr, cleanID) {
			bestAddr = serverAddr
			found = true
			break
		}
	}

	if !found {
		// Use any server as fallback
		for serverAddr := range c.clients {
			bestAddr = serverAddr
			break
		}
	}

	return bestAddr
}

// ringStateUpdater periodically updates the ring state in the background
func (c *Client) ringStateUpdater() {
	// Initially check more frequently to ensure we have the latest ring state
	initialTicker := time.NewTicker(2 * time.Second)
	defer initialTicker.Stop()

	// After initial period, switch to normal frequency
	regularTicker := time.NewTicker(30 * time.Second)
	defer regularTicker.Stop()

	// Use initial ticker for first 30 seconds
	initialPhase := true
	initialPhaseTimer := time.NewTimer(30 * time.Second)

	for {
		select {
		case <-initialTicker.C:
			if initialPhase {
				if err := c.updateRingState(); err != nil {
					log.Printf("Failed to update ring state (initial phase): %v", err)
				}
			}
		case <-regularTicker.C:
			if !initialPhase {
				if err := c.updateRingState(); err != nil {
					log.Printf("Failed to update ring state: %v", err)
				}
			}
		case <-initialPhaseTimer.C:
			initialPhase = false
			log.Printf("Switching to regular ring state update frequency")
		case <-c.ctx.Done():
			return
		}
	}
}
