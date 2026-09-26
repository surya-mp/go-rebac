package rebac

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// PostgreSQL schema is in migrations/001_initial.sql. Applications apply it
// through their normal migration runner before constructing PostgresStorage.

// PostgresStorage stores tuples through database/sql. Import a PostgreSQL
// driver in the application that opens the *sql.DB (for example pgx or lib/pq).
type PostgresStorage struct {
	db *sql.DB
}

// NewPostgresStorage wraps an existing PostgreSQL database handle.
func NewPostgresStorage(db *sql.DB) *PostgresStorage {
	return &PostgresStorage{db: db}
}

// QueryTuples returns tuples matching every non-empty filter field.
func (s *PostgresStorage) QueryTuples(ctx context.Context, filter RelationTuple) ([]RelationTuple, error) {
	return queryTuples(ctx, s.db, filter, nil)
}

func (s *PostgresStorage) QueryTuplesBatch(ctx context.Context, filters []RelationTuple) ([][]RelationTuple, error) {
	return queryTuplesBatch(ctx, s.db, filters, nil)
}

func (s *PostgresStorage) QueryTuplesPage(ctx context.Context, filter RelationTuple, limit, offset int) ([]RelationTuple, bool, error) {
	return queryTuplesPage(ctx, s.db, filter, nil, limit, offset)
}

// Snapshot creates a read-only latest snapshot for one Check call.
func (s *PostgresStorage) Snapshot(ctx context.Context) (TupleReader, func() error, error) {
	reader, _, release, err := s.SnapshotAt(ctx, "")
	return reader, release, err
}

// SnapshotAt creates a read-only repeatable-read view at revision. It does
// not own or close the application's database handle.
func (s *PostgresStorage) SnapshotAt(ctx context.Context, requested Revision) (TupleReader, Revision, func() error, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return nil, "", nil, err
	}
	revision, err := readRevision(ctx, tx, requested)
	if err != nil {
		_ = tx.Rollback()
		return nil, "", nil, err
	}
	return postgresReader{queryer: tx, revision: revision}, Revision(strconv.FormatInt(revision, 10)), tx.Rollback, nil
}

type queryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

type postgresReader struct {
	queryer  queryer
	revision int64
}

func (r postgresReader) QueryTuples(ctx context.Context, filter RelationTuple) ([]RelationTuple, error) {
	return queryTuples(ctx, r.queryer, filter, &r.revision)
}

func (r postgresReader) QueryTuplesBatch(ctx context.Context, filters []RelationTuple) ([][]RelationTuple, error) {
	return queryTuplesBatch(ctx, r.queryer, filters, &r.revision)
}

func (r postgresReader) QueryTuplesPage(ctx context.Context, filter RelationTuple, limit, offset int) ([]RelationTuple, bool, error) {
	return queryTuplesPage(ctx, r.queryer, filter, &r.revision, limit, offset)
}

func queryTuples(ctx context.Context, db queryer, filter RelationTuple, revision *int64) ([]RelationTuple, error) {
	where, args := tupleWhere(filter)
	where, args = tupleVisibility(where, args, revision, "")
	query := "SELECT tenant_id, namespace, object_id, relation, subject, caveat, caveat_context FROM relation_tuples"
	if len(where) != 0 {
		query += " WHERE " + strings.Join(where, " AND ")
	}

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tuples []RelationTuple
	for rows.Next() {
		var tuple RelationTuple
		var caveatContext []byte
		if err := rows.Scan(&tuple.TenantID, &tuple.Namespace, &tuple.ObjectID, &tuple.Relation, &tuple.User, &tuple.Caveat, &caveatContext); err != nil {
			return nil, err
		}
		if len(caveatContext) != 0 {
			if err := json.Unmarshal(caveatContext, &tuple.CaveatContext); err != nil {
				return nil, err
			}
		}
		tuples = append(tuples, tuple)
	}
	return tuples, rows.Err()
}

func queryTuplesPage(ctx context.Context, db queryer, filter RelationTuple, revision *int64, limit, offset int) ([]RelationTuple, bool, error) {
	where, args := tupleWhere(filter)
	where, args = tupleVisibility(where, args, revision, "")
	args = append(args, limit+1, offset)
	query := "SELECT tenant_id, namespace, object_id, relation, subject, caveat, caveat_context FROM relation_tuples WHERE " + strings.Join(where, " AND ") +
		" ORDER BY namespace, object_id, relation, subject LIMIT $" + strconv.Itoa(len(args)-1) + " OFFSET $" + strconv.Itoa(len(args))
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	tuples, err := scanTuples(rows)
	if err != nil {
		return nil, false, err
	}
	if len(tuples) <= limit {
		return tuples, false, nil
	}
	return tuples[:limit], true, nil
}

func queryTuplesBatch(ctx context.Context, db queryer, filters []RelationTuple, revision *int64) ([][]RelationTuple, error) {
	result := make([][]RelationTuple, len(filters))
	if len(filters) == 0 {
		return result, nil
	}
	args := make([]any, 0, len(filters)*6+1)
	values := make([]string, 0, len(filters))
	for index, filter := range filters {
		args = append(args, index, filter.TenantID, filter.Namespace, filter.ObjectID, filter.Relation, filter.User)
		positions := make([]string, 6)
		for i := range positions {
			positions[i] = "$" + strconv.Itoa(len(args)-5+i)
		}
		values = append(values, "("+strings.Join(positions, ", ")+")")
	}
	visibility := "t.deleted_revision IS NULL"
	if revision != nil {
		args = append(args, *revision)
		position := "$" + strconv.Itoa(len(args))
		visibility = "t.created_revision <= " + position + " AND (t.deleted_revision IS NULL OR t.deleted_revision > " + position + ")"
	}
	query := "WITH filters (idx, tenant_id, namespace, object_id, relation, subject) AS (VALUES " + strings.Join(values, ", ") + ") " +
		"SELECT f.idx, t.tenant_id, t.namespace, t.object_id, t.relation, t.subject, t.caveat, t.caveat_context FROM filters f JOIN relation_tuples t ON " +
		"t.tenant_id = f.tenant_id AND (f.namespace = '' OR t.namespace = f.namespace) AND (f.object_id = '' OR t.object_id = f.object_id) AND (f.relation = '' OR t.relation = f.relation) AND (f.subject = '' OR t.subject = f.subject) AND " + visibility +
		" ORDER BY f.idx, t.namespace, t.object_id, t.relation, t.subject"
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var index int
		tuple, err := scanTuple(rows, &index)
		if err != nil {
			return nil, err
		}
		result[index] = append(result[index], tuple)
	}
	return result, rows.Err()
}

func tupleVisibility(where []string, args []any, revision *int64, prefix string) ([]string, []any) {
	column := func(name string) string { return prefix + name }
	if revision == nil {
		return append(where, column("deleted_revision")+" IS NULL"), args
	}
	args = append(args, *revision)
	position := "$" + strconv.Itoa(len(args))
	return append(where, column("created_revision")+" <= "+position, "("+column("deleted_revision")+" IS NULL OR "+column("deleted_revision")+" > "+position+")"), args
}

func scanTuples(rows *sql.Rows) ([]RelationTuple, error) {
	var tuples []RelationTuple
	for rows.Next() {
		tuple, err := scanTuple(rows)
		if err != nil {
			return nil, err
		}
		tuples = append(tuples, tuple)
	}
	return tuples, rows.Err()
}

func scanTuple(rows *sql.Rows, prefix ...any) (RelationTuple, error) {
	var tuple RelationTuple
	var caveatContext []byte
	values := append(prefix, &tuple.TenantID, &tuple.Namespace, &tuple.ObjectID, &tuple.Relation, &tuple.User, &tuple.Caveat, &caveatContext)
	if err := rows.Scan(values...); err != nil {
		return RelationTuple{}, err
	}
	if len(caveatContext) != 0 {
		if err := json.Unmarshal(caveatContext, &tuple.CaveatContext); err != nil {
			return RelationTuple{}, err
		}
	}
	return tuple, nil
}

func (s *PostgresStorage) WriteTuple(ctx context.Context, tuple RelationTuple) error {
	_, err := s.WriteTupleWithRevision(ctx, tuple)
	return err
}

// WriteTupleWithRevision writes a tuple and returns the committed revision.
func (s *PostgresStorage) WriteTupleWithRevision(ctx context.Context, tuple RelationTuple) (Revision, error) {
	return s.Mutate(ctx, []TupleChange{{Operation: WriteOperation, Tuple: tuple}}, nil)
}

func (s *PostgresStorage) DeleteTuple(ctx context.Context, tuple RelationTuple) error {
	_, err := s.DeleteTupleWithRevision(ctx, tuple)
	return err
}

// DeleteTupleWithRevision tombstones a tuple and returns the committed revision.
func (s *PostgresStorage) DeleteTupleWithRevision(ctx context.Context, tuple RelationTuple) (Revision, error) {
	return s.Mutate(ctx, []TupleChange{{Operation: DeleteOperation, Tuple: tuple}}, nil)
}

// Mutate applies every change at one revision inside one PostgreSQL transaction.
func (s *PostgresStorage) Mutate(ctx context.Context, changes []TupleChange, preconditions []Precondition) (Revision, error) {
	// Serializable isolation makes a precondition race fail instead of allowing
	// another writer to slip between its check and this mutation's commit.
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return "", err
	}
	for _, precondition := range preconditions {
		exists, err := tupleExists(ctx, tx, precondition.Tuple)
		if err != nil {
			_ = tx.Rollback()
			return "", err
		}
		if exists != precondition.MustExist {
			_ = tx.Rollback()
			return "", fmt.Errorf("%w: %s#%s", ErrPreconditionFailed, precondition.Tuple.Namespace, precondition.Tuple.Relation)
		}
	}
	revision, err := nextRevision(ctx, tx)
	if err == nil {
		for ordinal, change := range changes {
			caveatContext, marshalErr := json.Marshal(change.Tuple.CaveatContext)
			if marshalErr != nil {
				err = marshalErr
				break
			}
			switch change.Operation {
			case WriteOperation:
				_, err = tx.ExecContext(ctx,
					"INSERT INTO relation_tuples (tenant_id, namespace, object_id, relation, subject, caveat, caveat_context, created_revision) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT DO NOTHING",
					change.Tuple.TenantID, change.Tuple.Namespace, change.Tuple.ObjectID, change.Tuple.Relation, change.Tuple.User, change.Tuple.Caveat, caveatContext, revision,
				)
			case DeleteOperation:
				_, err = tx.ExecContext(ctx,
					"UPDATE relation_tuples SET deleted_revision = $1 WHERE tenant_id = $2 AND namespace = $3 AND object_id = $4 AND relation = $5 AND subject = $6 AND deleted_revision IS NULL",
					revision, change.Tuple.TenantID, change.Tuple.Namespace, change.Tuple.ObjectID, change.Tuple.Relation, change.Tuple.User,
				)
			default:
				err = fmt.Errorf("rebac: invalid tuple operation %q", change.Operation)
			}
			if err != nil {
				break
			}
			_, err = tx.ExecContext(ctx,
				"INSERT INTO rebac_changes (revision, ordinal, tenant_id, operation, namespace, object_id, relation, subject, caveat, caveat_context) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
				revision, ordinal, change.Tuple.TenantID, change.Operation, change.Tuple.Namespace, change.Tuple.ObjectID, change.Tuple.Relation, change.Tuple.User, change.Tuple.Caveat, caveatContext,
			)
			if err != nil {
				break
			}
		}
	}
	if err != nil {
		_ = tx.Rollback()
		return "", err
	}
	if err := tx.Commit(); err != nil {
		return "", err
	}
	return Revision(strconv.FormatInt(revision, 10)), nil
}

const postgresWatchInterval = 250 * time.Millisecond

// Watch polls the durable change log. It consumes the application's existing
// database handle and stops when ctx is canceled.
func (s *PostgresStorage) Watch(ctx context.Context, tenantID string, after Revision) (<-chan WatchEvent, <-chan error) {
	events := make(chan WatchEvent)
	errorsCh := make(chan error, 1)
	start, err := parseRevision(after)
	if err != nil {
		errorsCh <- err
		close(events)
		close(errorsCh)
		return events, errorsCh
	}
	go func() {
		defer close(events)
		defer close(errorsCh)
		last := start
		ticker := time.NewTicker(postgresWatchInterval)
		defer ticker.Stop()
		for {
			changes, err := s.watchChanges(ctx, tenantID, last)
			if err != nil {
				if ctx.Err() == nil {
					errorsCh <- err
				}
				return
			}
			for _, event := range changes {
				select {
				case events <- event:
					last, _ = parseRevision(event.Revision)
				case <-ctx.Done():
					return
				}
			}
			select {
			case <-ticker.C:
			case <-ctx.Done():
				return
			}
		}
	}()
	return events, errorsCh
}

func (s *PostgresStorage) watchChanges(ctx context.Context, tenantID string, after int64) ([]WatchEvent, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT revision, operation, namespace, object_id, relation, subject, caveat, caveat_context FROM rebac_changes WHERE tenant_id = $1 AND revision > $2 ORDER BY revision, ordinal",
		tenantID, after,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []WatchEvent
	for rows.Next() {
		var revision int64
		var operation TupleOperation
		var tuple RelationTuple
		var caveatContext []byte
		if err := rows.Scan(&revision, &operation, &tuple.Namespace, &tuple.ObjectID, &tuple.Relation, &tuple.User, &tuple.Caveat, &caveatContext); err != nil {
			return nil, err
		}
		tuple.TenantID = tenantID
		if len(caveatContext) != 0 {
			if err := json.Unmarshal(caveatContext, &tuple.CaveatContext); err != nil {
				return nil, err
			}
		}
		current := Revision(strconv.FormatInt(revision, 10))
		if len(events) == 0 || events[len(events)-1].Revision != current {
			events = append(events, WatchEvent{Revision: current, TenantID: tenantID})
		}
		index := len(events) - 1
		events[index].Changes = append(events[index].Changes, TupleChange{Operation: operation, Tuple: tuple})
	}
	return events, rows.Err()
}

func tupleExists(ctx context.Context, tx *sql.Tx, tuple RelationTuple) (bool, error) {
	var exists bool
	err := tx.QueryRowContext(ctx,
		"SELECT EXISTS (SELECT 1 FROM relation_tuples WHERE tenant_id = $1 AND namespace = $2 AND object_id = $3 AND relation = $4 AND subject = $5 AND deleted_revision IS NULL)",
		tuple.TenantID, tuple.Namespace, tuple.ObjectID, tuple.Relation, tuple.User,
	).Scan(&exists)
	return exists, err
}

func nextRevision(ctx context.Context, tx *sql.Tx) (int64, error) {
	var revision int64
	err := tx.QueryRowContext(ctx, "INSERT INTO rebac_revisions DEFAULT VALUES RETURNING id").Scan(&revision)
	return revision, err
}

func readRevision(ctx context.Context, tx *sql.Tx, requested Revision) (int64, error) {
	var latest int64
	if err := tx.QueryRowContext(ctx, "SELECT COALESCE(MAX(id), 0) FROM rebac_revisions").Scan(&latest); err != nil {
		return 0, err
	}
	if requested == "" {
		return latest, nil
	}
	revision, err := parseRevision(requested)
	if err != nil || revision > latest {
		return 0, fmt.Errorf("%w: %q", ErrInvalidRevision, requested)
	}
	return revision, nil
}

func parseRevision(revision Revision) (int64, error) {
	if revision == "" {
		return 0, nil
	}
	parsed, err := strconv.ParseInt(string(revision), 10, 64)
	if err != nil || parsed < 0 {
		return 0, fmt.Errorf("%w: %q", ErrInvalidRevision, revision)
	}
	return parsed, nil
}

func tupleWhere(filter RelationTuple) ([]string, []any) {
	fields := []struct {
		column, value string
	}{
		{"tenant_id", filter.TenantID},
		{"namespace", filter.Namespace},
		{"object_id", filter.ObjectID},
		{"relation", filter.Relation},
		{"subject", filter.User},
	}
	where := make([]string, 0, len(fields))
	args := make([]any, 0, len(fields))
	for _, field := range fields {
		if field.value == "" {
			continue
		}
		args = append(args, field.value)
		where = append(where, field.column+" = $"+strconv.Itoa(len(args)))
	}
	return where, args
}
