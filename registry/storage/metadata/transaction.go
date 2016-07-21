package metadata

import (
	"fmt"

	"errors"
	"github.com/docker/distribution"
	"github.com/docker/distribution/context"
	"github.com/docker/distribution/uuid"
)

// TxFunc runs a set of operations within a transaction
type MetadataUpdateFunc func(context.Context) error

var ErrTransactionCanRetry = errors.New("transaction can be retried")

// Update takes a function which makes a series of mutations and tries
// to commit it
func Update(ctx context.Context, repo distribution.Repository, f MetadataUpdateFunc) error {
	metadataRepo, support := repo.(Metadatable)
	if !support {
		// no metadata store configured
		return f(ctx)
	}

	tx, err := BeginTx(metadataRepo.MetadataService())
	if err != nil {
		return err
	}
	ctx = WithTx(ctx, tx)

	var attempts int
try:
	// Prepare the transaction
	err = f(ctx)
	if err != nil {
		return err
	}

	var txErr error
	if attempts == 1 {
		return fmt.Errorf("transaction failed: %s", txErr)
	}

	if txErr = tx.Commit(ctx); txErr != nil {
		if canRetry(txErr) {
			attempts++
			goto try
		}
	}

	return txErr
}

func canRetry(err error) bool {
	return err == ErrTransactionCanRetry
}

type Transaction struct {
	id        uuid.UUID
	store     MetadataService
	prepared  map[string]MetadataUpdateRecord
	committed bool
}

// todo: rollback state

func BeginTx(store MetadataService) (*Transaction, error) {
	return &Transaction{
		id:       uuid.Generate(),
		prepared: make(map[string]MetadataUpdateRecord),
		store:    store,
	}, nil
}

func (t *Transaction) Update(ctx context.Context, key string, val interface{}) error {
	if t.committed {
		return fmt.Errorf("Unable to update %s, already committed", t.id)
	}

	expected, err := t.store.Get(ctx, key)
	if err != nil {
		return err
	}

	u := MetadataUpdateRecord{Actual: val, Expected: expected}
	t.prepared[key] = u

	valstr := fmt.Sprint(val)
	if len(valstr) > 20 {
		valstr = valstr[:20] + "..."
	}
	curstr := fmt.Sprint(expected)
	if len(curstr) > 20 {
		curstr = curstr[:20] + "..."
	}
	fmt.Printf("Updated transaction %s with %q=%s, cur=%s\n", t.id, key, valstr, curstr)
	return nil
}

func (t *Transaction) Commit(ctx context.Context) error {
	if t.committed {
		return fmt.Errorf("Unable to commit %s, already committed", t.id)
	}

	err := t.store.BatchPut(ctx, t.prepared)
	if err != nil {
		return err
	}

	commitMsg := fmt.Sprintf("Transaction committed (%s) {\n", t.id)
	for k, v := range t.prepared {
		if v.Actual == nil {
			commitMsg += fmt.Sprintf("  delete: %s\n", k)
		} else {
			commitMsg += fmt.Sprintf("  update: %s=%s\n", k, v.Actual)
		}
	}
	commitMsg += "}"
	fmt.Println(commitMsg)
	t.committed = true
	return nil
}

func (t *Transaction) Rollback() error {
	if t.committed {
		return fmt.Errorf("Unable to rollback %s, already committed", t.id)
	}
	t.prepared = make(map[string]MetadataUpdateRecord)
	return nil
}

// withTxContext is a context which carries a transaction.  Only one transaction
// is supported
type withTxContext struct {
	context.Context
	tx *Transaction
}

func (w withTxContext) Value(key interface{}) interface{} {
	if key == "tx" {
		return w.tx
	}
	return w.Context.Value(key)
}

func WithTx(ctx context.Context, tx *Transaction) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}

	txContext := withTxContext{
		Context: ctx,
		tx:      tx,
	}
	return txContext
}

func GetTx(ctx context.Context) *Transaction {
	if tx, ok := ctx.Value("tx").(*Transaction); ok {
		return tx
	}
	return nil
}
