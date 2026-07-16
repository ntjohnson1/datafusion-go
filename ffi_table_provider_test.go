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
// second library to produce one. Here we verify the public function validates
// its arguments before dereferencing the provider pointer.
func TestRegisterFFITableProviderRejectsNilProvider(t *testing.T) {
	conn := openTestConn(t)

	_, err := RegisterFFITableProvider(context.Background(), conn, "t", unsafe.Pointer(nil), DataFusionVersion)
	if err == nil || !strings.Contains(err.Error(), "nil") {
		t.Fatalf("got %v, want an error mentioning a nil provider", err)
	}
}

func TestRegisterFFITableProviderRejectsEmptyName(t *testing.T) {
	conn := openTestConn(t)

	_, err := RegisterFFITableProvider(context.Background(), conn, "", validProviderPtr(), DataFusionVersion)
	if err == nil || !strings.Contains(err.Error(), "name") {
		t.Fatalf("got %v, want an error mentioning an empty table name", err)
	}
}

func TestRegisterFFITableProviderRejectsEmptyVersion(t *testing.T) {
	conn := openTestConn(t)

	_, err := RegisterFFITableProvider(context.Background(), conn, "t", validProviderPtr(), "")
	if err == nil || !strings.Contains(err.Error(), "version") {
		t.Fatalf("got %v, want an error mentioning an empty version", err)
	}
}

// A version mismatch must be rejected *before* the provider is dereferenced.
// We pass a bogus (but readable) pointer that is not a real FFI_TableProvider:
// if the version check did not gate the dereference, the native side would read
// garbage as a vtable and crash. Reaching a clean error proves the ordering.
func TestRegisterFFITableProviderRejectsVersionMismatch(t *testing.T) {
	conn := openTestConn(t)

	_, err := RegisterFFITableProvider(context.Background(), conn, "t", validProviderPtr(), "0.0.0-not-a-real-version")
	if err == nil || !strings.Contains(err.Error(), "0.0.0-not-a-real-version") {
		t.Fatalf("got %v, want a datafusion version-mismatch error", err)
	}
}

// validProviderPtr returns a non-nil pointer to readable memory that is never
// dereferenced by tests exercising the pre-dereference validation paths.
func validProviderPtr() unsafe.Pointer {
	return unsafe.Pointer(new([64]byte))
}

func openTestConn(t *testing.T) *sql.Conn {
	t.Helper()

	db, err := sql.Open("datafusion", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeNoError(t, db) })

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeNoError(t, conn) })

	return conn
}
