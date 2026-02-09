package harestore

import (
	"context"
	"sync"

	"cloud.google.com/go/datastore"
)

var (
	// defaultRawClient holds the raw datastore client shared across the app.
	defaultRawClient *datastore.Client

	globalOptions   []ClientOption
	globalOptionsMu sync.RWMutex
)

// Init sets the default client.
func Init(c *datastore.Client) {
	defaultRawClient = c
}

// SetGlobalOptions
func SetGlobalOptions(opts ...ClientOption) {
	globalOptionsMu.Lock()

	defer globalOptionsMu.Unlock()

	globalOptions = make([]ClientOption, len(opts))

	copy(globalOptions, opts)
}

// getGlobalOptions
func getGlobalOptions() []ClientOption {
	globalOptionsMu.RLock()

	defer globalOptionsMu.RUnlock()

	return globalOptions
}

// RunInTransaction starts a transaction.
func RunInTransaction(ctx context.Context, f func(ctx context.Context) error) error {
	if _, ok := extractTransactionFromContext(ctx); ok {
		// 既存のコンテキストのまま実行
		err := f(ctx)
		if err != nil {
			return err
		}

		return nil
	}

	_, err := defaultRawClient.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		ctxWithTx := context.WithValue(ctx, contextKeyTransaction{}, tx)

		return f(ctxWithTx)
	})

	return err
}

// GetByID retrieves one entity by specifying id.
func GetByID[T any, PT PEntity[T]](ctx context.Context, id string) (*T, error) {
	opts := getGlobalOptions()

	return NewClient[T, PT](defaultRawClient, opts...).GetByID(ctx, id)
}

// Insert registers one entity.
func Insert[T any, PT PEntity[T]](ctx context.Context, entity *T) (string, error) {
	opts := getGlobalOptions()

	return NewClient[T, PT](defaultRawClient, opts...).Insert(ctx, entity)
}

// Update updates one entity.
func Update[T any, PT PEntity[T]](ctx context.Context, entity *T) error {
	opts := getGlobalOptions()

	return NewClient[T, PT](defaultRawClient, opts...).Update(ctx, entity)
}

// DeleteByID deletes one entity by specifying id.
func DeleteByID[T any, PT PEntity[T]](ctx context.Context, id string) error {
	opts := getGlobalOptions()

	return NewClient[T, PT](defaultRawClient, opts...).DeleteByID(ctx, id)
}

// Delete deletes the specifying entity.
func Delete[T any, PT PEntity[T]](ctx context.Context, entity *T) error {
	opts := getGlobalOptions()

	return NewClient[T, PT](defaultRawClient, opts...).Delete(ctx, entity)
}

// GetMultiByID retrieves the entities by specifing ids.
func GetMultiByID[T any, PT PEntity[T]](ctx context.Context, ids []string) ([]*T, error) {
	opts := getGlobalOptions()

	return NewClient[T, PT](defaultRawClient, opts...).GetMultiByID(ctx, ids)
}

// InsertMulti inserts the specifing entities.
func InsertMulti[T any, PT PEntity[T]](ctx context.Context, entities []*T) ([]string, error) {
	opts := getGlobalOptions()

	return NewClient[T, PT](defaultRawClient, opts...).InsertMulti(ctx, entities)
}

// UpdateMulti updates the specifing entities.
func UpdateMulti[T any, PT PEntity[T]](ctx context.Context, entities []*T) error {
	opts := getGlobalOptions()

	return NewClient[T, PT](defaultRawClient, opts...).UpdateMulti(ctx, entities)
}

// DeleteMultiByID deletes the entities by specifing ids.
func DeleteMultiByID[T any, PT PEntity[T]](ctx context.Context, ids []string) error {
	opts := getGlobalOptions()

	return NewClient[T, PT](defaultRawClient, opts...).DeleteMultiByID(ctx, ids)
}

// DeleteMulti deletes the entities.
func DeleteMulti[T any, PT PEntity[T]](ctx context.Context, entities []*T) error {
	opts := getGlobalOptions()

	return NewClient[T, PT](defaultRawClient, opts...).DeleteMulti(ctx, entities)
}

// RunRawQuery executes the query.
func RunRawQuery[T any, PT PEntity[T]](ctx context.Context, q *datastore.Query) ([]*T, error) {
	opts := getGlobalOptions()

	return NewClient[T, PT](defaultRawClient, opts...).RunRawQuery(ctx, q)
}

// DeleteByRawQuery deletes entities retrieved by executing a query.
func DeleteByRawQuery[T any, PT PEntity[T]](ctx context.Context, q *datastore.Query) error {
	opts := getGlobalOptions()

	return NewClient[T, PT](defaultRawClient, opts...).DeleteByRawQuery(ctx, q)
}
