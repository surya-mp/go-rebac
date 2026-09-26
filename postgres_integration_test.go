package rebac

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

func TestPostgresRevisionIntegration(t *testing.T) {
	dsn := os.Getenv("REBAC_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set REBAC_TEST_POSTGRES_DSN to run PostgreSQL integration tests")
	}
	driverName := os.Getenv("REBAC_TEST_POSTGRES_DRIVER")
	if driverName == "" {
		driverName = "pgx"
	}
	if !registeredDriver(driverName) {
		t.Skipf("link database/sql driver %q in the CI integration-test harness", driverName)
	}
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	if err := db.PingContext(context.Background()); err != nil {
		t.Fatal(err)
	}

	schema := fmt.Sprintf("rebac_test_%d", time.Now().UnixNano())
	if _, err := db.Exec("CREATE SCHEMA " + schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = db.Exec("DROP SCHEMA " + schema + " CASCADE") })
	if _, err := db.Exec("SET search_path TO " + schema); err != nil {
		t.Fatal(err)
	}
	migration, err := os.ReadFile("migrations/001_initial.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range strings.Split(string(migration), ";") {
		if strings.TrimSpace(statement) == "" {
			continue
		}
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}

	engine, err := NewEngine(NewPostgresStorage(db), testModel())
	if err != nil {
		t.Fatal(err)
	}
	tuple := RelationTuple{TenantID: "acme", Namespace: "document", ObjectID: "plan", Relation: "viewer", User: "user:alice"}
	created, err := engine.WriteTupleWithRevision(context.Background(), tuple)
	if err != nil {
		t.Fatal(err)
	}
	allowed, revision, err := engine.CheckWithRevision(context.Background(), created, "acme", "user:alice", "viewer", "document", "plan")
	if err != nil || !allowed || revision != created {
		t.Fatalf("CheckWithRevision() = %v, %q, %v", allowed, revision, err)
	}
	deleted, err := engine.DeleteTupleWithRevision(context.Background(), tuple)
	if err != nil {
		t.Fatal(err)
	}
	allowed, _, err = engine.CheckWithRevision(context.Background(), created, "acme", "user:alice", "viewer", "document", "plan")
	if err != nil || !allowed {
		t.Fatalf("old revision lost access: %v, %v", allowed, err)
	}
	allowed, _, err = engine.CheckWithRevision(context.Background(), deleted, "acme", "user:alice", "viewer", "document", "plan")
	if err != nil || allowed {
		t.Fatalf("deleted revision retained access: %v, %v", allowed, err)
	}
}

func registeredDriver(name string) bool {
	for _, driver := range sql.Drivers() {
		if driver == name {
			return true
		}
	}
	return false
}
