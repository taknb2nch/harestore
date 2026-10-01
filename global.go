package harestore

import (
	"context"
	"sync"

	"cloud.google.com/go/datastore"
)

var (
	defaultClient *Client
	globalMu      sync.RWMutex
)

// Init sets the default client and global options.
func Init(c *datastore.Client, opts ...ClientOption) {
	globalMu.Lock()
	defer globalMu.Unlock()

	defaultClient = NewClient(c, opts...)
}

func getDefaultClient() *Client {
	globalMu.RLock()
	defer globalMu.RUnlock()

	if defaultClient == nil {
		panic("harestore: default client is not initialized. Call harestore.Init() first")
	}

	return defaultClient
}

// RunInTransaction starts a transaction.
func RunInTransaction(ctx context.Context, f func(ctx context.Context) error) error {
	return getDefaultClient().RunInTransaction(ctx, f)
}

// Get retrieves one entity by specifying id.
func Get[T any, PT PEntity[T]](ctx context.Context, id string) (*T, error) {
	return getDefaultClient().Get[T, PT](ctx, id)
}

// Insert registers one entity.
func Insert[T any, PT PEntity[T]](ctx context.Context, entity *T) (string, error) {
	return getDefaultClient().Insert[T, PT](ctx, entity)
}

// Update updates one entity.
func Update[T any, PT PEntity[T]](ctx context.Context, entity *T) error {
	return getDefaultClient().Update[T, PT](ctx, entity)
}

// DeleteByID deletes one entity by specifying id.
func DeleteByID[T any, PT PEntity[T]](ctx context.Context, id string) error {
	return getDefaultClient().DeleteByID[T, PT](ctx, id)
}

// Delete deletes the specifying entity.
func Delete[T any, PT PEntity[T]](ctx context.Context, entity *T) error {
	return getDefaultClient().Delete[T, PT](ctx, entity)
}

// GetMulti retrieves the entities by specifing ids.
func GetMulti[T any, PT PEntity[T]](ctx context.Context, ids []string) ([]*T, error) {
	return getDefaultClient().GetMulti[T, PT](ctx, ids)
}

// InsertMulti inserts the specifing entities.
func InsertMulti[T any, PT PEntity[T]](ctx context.Context, entities []*T) ([]string, error) {
	return getDefaultClient().InsertMulti[T, PT](ctx, entities)
}

// UpdateMulti updates the specifing entities.
func UpdateMulti[T any, PT PEntity[T]](ctx context.Context, entities []*T) error {
	return getDefaultClient().UpdateMulti[T, PT](ctx, entities)
}

// DeleteMultiByID deletes the entities by specifing ids.
func DeleteMultiByID[T any, PT PEntity[T]](ctx context.Context, ids []string) error {
	return getDefaultClient().DeleteMultiByID[T, PT](ctx, ids)
}

// DeleteMulti deletes the entities.
func DeleteMulti[T any, PT PEntity[T]](ctx context.Context, entities []*T) error {
	return getDefaultClient().DeleteMulti[T, PT](ctx, entities)
}

// RunQuery executes the query.
func RunQuery[T any, PT PEntity[T]](ctx context.Context, q *datastore.Query) ([]*T, error) {
	return getDefaultClient().RunQuery[T, PT](ctx, q)
}

// RunQueryWithCursor executes the query.
func RunQueryWithCursor[T any, PT PEntity[T]](ctx context.Context, q *datastore.Query, cursor string) ([]*T, string, error) {
	return getDefaultClient().RunQueryWithCursor[T, PT](ctx, q, cursor)
}

// DeleteByQuery deletes entities retrieved by executing a query.
func DeleteByQuery(ctx context.Context, q *datastore.Query) error {
	return getDefaultClient().DeleteByQuery(ctx, q)
}
