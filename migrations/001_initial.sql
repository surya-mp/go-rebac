-- go-rebac schema version 1. Apply this migration with the application's
-- migration tool; go-rebac never opens or migrates the application's database.

CREATE TABLE rebac_revisions (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE relation_tuples (
    tenant_id TEXT NOT NULL,
    namespace TEXT NOT NULL,
    object_id TEXT NOT NULL,
    relation TEXT NOT NULL,
    subject TEXT NOT NULL,
    caveat TEXT NOT NULL DEFAULT '',
    caveat_context JSONB,
    created_revision BIGINT NOT NULL REFERENCES rebac_revisions(id),
    deleted_revision BIGINT REFERENCES rebac_revisions(id),
    PRIMARY KEY (tenant_id, namespace, object_id, relation, subject, created_revision),
    CHECK (deleted_revision IS NULL OR deleted_revision > created_revision)
);

CREATE UNIQUE INDEX relation_tuples_live_unique
    ON relation_tuples (tenant_id, namespace, object_id, relation, subject)
    WHERE deleted_revision IS NULL;

CREATE INDEX relation_tuples_lookup
    ON relation_tuples (tenant_id, namespace, object_id, relation, created_revision);

CREATE TABLE rebac_changes (
    revision BIGINT NOT NULL REFERENCES rebac_revisions(id),
    ordinal INTEGER NOT NULL,
    tenant_id TEXT NOT NULL,
    operation TEXT NOT NULL CHECK (operation IN ('write', 'delete')),
    namespace TEXT NOT NULL,
    object_id TEXT NOT NULL,
    relation TEXT NOT NULL,
    subject TEXT NOT NULL,
    caveat TEXT NOT NULL DEFAULT '',
    caveat_context JSONB,
    PRIMARY KEY (revision, ordinal)
);

CREATE INDEX rebac_changes_watch
    ON rebac_changes (tenant_id, revision, ordinal);
