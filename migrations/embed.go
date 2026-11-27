package migrations

import _ "embed"

// InitSQL is the initial PostgreSQL schema for Kepler Durable Runtime.
//
//go:embed 001_init.sql
var InitSQL string

// GatewaySQL is the PostgreSQL schema for MCP Gateway idempotency and audit logging.
//
//go:embed 002_gateway.sql
var GatewaySQL string

// All returns all embedded SQL migrations in execution order.
func All() []string {
	return []string{InitSQL, GatewaySQL}
}
