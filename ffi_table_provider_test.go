//go:build cgo

package datafusion

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"unsafe"
)

// The positive round-trip (register a real FFI_TableProvider and query it) is
// covered by the Rust test `registers_and_queries_ffi_table_provider`, which
// can mint a provider in-process; a Go-level positive test would require a
// second library to produce one. Here we verify the public binding routes
// through to the native validation.
func TestRegisterFFITableProviderRejectsNil(t *testing.T) {
	db, err := sql.Open("datafusion", "")
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, db)

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoError(t, conn)

	var got error
	if err := conn.Raw(func(dc any) error {
		got = dc.(*Conn).RegisterFFITableProvider("t", unsafe.Pointer(nil))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got == nil || !strings.Contains(got.Error(), "nil") {
		t.Fatalf("got %v, want an error mentioning a nil provider", got)
	}
}
