package migrations

import _ "embed"

// InitSQL is the initial PostgreSQL schema for Kepler Durable Runtime.
//
//go:embed 001_init.sql
var InitSQL string
