// Copyright 2026 mesh-relay authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package config

// RelayConfig configures the native inter-server relay (mesh-relay Phase F).
// Fork-only: this file does not exist upstream, so it never conflicts on merges.
//
// Phase F topology is one static link, mirroring mesh-relay Phase 1: a node with
// peer_address dials and exports its locally published tracks (origin side); a node
// with listen_address accepts links and receives relayed tracks (edge side). A node
// may set both. Discovery (Phase 4) replaces the static addresses with a roster.
type RelayConfig struct {
	Enabled bool `yaml:"enabled,omitempty"`
	// ListenAddress is the UDP host:port to accept peer QUIC links on (edge side).
	ListenAddress string `yaml:"listen_address,omitempty"`
	// PeerAddress is the peer's relay listen address to dial (origin side).
	PeerAddress string `yaml:"peer_address,omitempty"`
}
