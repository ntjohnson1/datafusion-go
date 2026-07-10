package datafusion

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"unsafe"
)

// RegisteredTable is a handle to a table registered on a connection by
// RegisterFFITableProvider. Call Deregister to remove the table before the
// producing library tears down; closing the underlying *sql.Conn also releases
// it. The handle is safe for concurrent use.
//
// Dropping the handle does not deregister the table: the table's lifetime is
// owned by the connection, not by this handle, and a caller may legitimately
// keep querying the table after dropping the handle. Deregistration is
// therefore always explicit.
type RegisteredTable struct {
	sqlConn *sql.Conn
	name    string

	mu   sync.Mutex
	done bool
}

// RegisterFFITableProvider registers a foreign datafusion-ffi FFI_TableProvider,
// produced by another library, as a queryable table named tableName on the
// DataFusion connection backing sqlConn. Once registered, SQL executed on
// sqlConn can scan the table, and projection/filter predicates are pushed down
// into the foreign provider.
//
// provider must point to a valid, initialized datafusion-ffi FFI_TableProvider.
// providerDataFusionVersion is the datafusion version the library that produced
// provider was built against; obtain it from that library (not from
// DataFusionVersion). Registration fails with an error if it does not equal this
// package's DataFusionVersion — the two sides must link the same datafusion
// version for the provider's memory layout to be compatible. This check happens
// before provider is dereferenced, so a version mismatch is a clean error rather
// than a crash; it is cooperative and cannot detect a mislabeled provider.
//
// Lifetime: registration clones the provider (bumping its internal refcount), so
// the caller still owns the original FFI_TableProvider pointer and may free it
// through its producing library once this call returns. The producing library
// itself, however, must stay loaded for as long as the table remains registered
// (until Deregister or until sqlConn is closed): the registered table calls back
// into the provider's function pointers on every scan, and unloading the library
// leaves them dangling. The DataFusion session backing the provider should also
// outlive the registration; if it does not, queries against the table fail with
// a clean error rather than crashing.
func RegisterFFITableProvider(ctx context.Context, sqlConn *sql.Conn, tableName string, provider unsafe.Pointer, providerDataFusionVersion string) (*RegisteredTable, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if sqlConn == nil {
		return nil, fmt.Errorf("datafusion sql connection is nil")
	}
	if tableName == "" {
		return nil, fmt.Errorf("datafusion ffi table name is empty")
	}
	if provider == nil {
		return nil, fmt.Errorf("datafusion ffi table provider is nil")
	}
	if providerDataFusionVersion == "" {
		return nil, fmt.Errorf("datafusion ffi provider datafusion version is empty")
	}

	if err := withDataFusionConn(sqlConn, func(conn *Conn) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return conn.conn.RegisterFFITableProvider(tableName, provider, providerDataFusionVersion)
	}); err != nil {
		return nil, err
	}

	return &RegisteredTable{sqlConn: sqlConn, name: tableName}, nil
}

// Name returns the table name this handle refers to.
func (t *RegisteredTable) Name() string {
	return t.name
}

// Deregister removes the table from the connection's session. It is idempotent:
// calling it more than once, or after the connection is closed, is a no-op that
// returns nil. Deregister on a name that is no longer registered is not an error.
func (t *RegisteredTable) Deregister(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.done {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	if err := withDataFusionConn(t.sqlConn, func(conn *Conn) error {
		return conn.conn.DeregisterTable(t.name)
	}); err != nil {
		return err
	}

	t.done = true
	return nil
}
