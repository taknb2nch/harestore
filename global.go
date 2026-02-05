package harestore

import (
	"context"

	"cloud.google.com/go/datastore"
)

// defaultRawClient holds the raw datastore client shared across the app.
var defaultRawClient *datastore.Client

// Init sets the default client.
func Init(c *datastore.Client) {
	defaultRawClient = c
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
	return NewClient[T, PT](defaultRawClient).GetByID(ctx, id)
}

// Insert registers one entity.
func Insert[T any, PT PEntity[T]](ctx context.Context, entity *T) (string, error) {
	return NewClient[T, PT](defaultRawClient).Insert(ctx, entity)
}

// Update updates one entity.
func Update[T any, PT PEntity[T]](ctx context.Context, entity *T) error {
	return NewClient[T, PT](defaultRawClient).Update(ctx, entity)
}

// DeleteByID deletes one entity by specifying id.
func DeleteByID[T any, PT PEntity[T]](ctx context.Context, id string) error {
	return NewClient[T, PT](defaultRawClient).DeleteByID(ctx, id)
}

// Delete deletes the specifying entity.
func Delete[T any, PT PEntity[T]](ctx context.Context, entity *T) error {
	return NewClient[T, PT](defaultRawClient).Delete(ctx, entity)
}

// GetMultiByID retrieves the entities by specifing ids.
func GetMultiByID[T any, PT PEntity[T]](ctx context.Context, ids []string) ([]*T, error) {
	return NewClient[T, PT](defaultRawClient).GetMultiByID(ctx, ids)
}

// InsertMulti inserts the specifing entities.
func InsertMulti[T any, PT PEntity[T]](ctx context.Context, entities []*T) ([]string, error) {
	return NewClient[T, PT](defaultRawClient).InsertMulti(ctx, entities)
}

// UpdateMulti updates the specifing entities.
func UpdateMulti[T any, PT PEntity[T]](ctx context.Context, entities []*T) error {
	return NewClient[T, PT](defaultRawClient).UpdateMulti(ctx, entities)
}

// DeleteMultiByID deletes the entities by specifing ids.
func DeleteMultiByID[T any, PT PEntity[T]](ctx context.Context, ids []string) error {
	return NewClient[T, PT](defaultRawClient).DeleteMultiByID(ctx, ids)
}

// DeleteMulti deletes the entities.
func DeleteMulti[T any, PT PEntity[T]](ctx context.Context, entities []*T) error {
	return NewClient[T, PT](defaultRawClient).DeleteMulti(ctx, entities)
}

// RunRawQuery executes the query.
func RunRawQuery[T any, PT PEntity[T]](ctx context.Context, q *datastore.Query) ([]*T, error) {
	return NewClient[T, PT](defaultRawClient).RunRawQuery(ctx, q)
}

// DeleteByRawQuery deletes entities retrieved by executing a query.
func DeleteByRawQuery[T any, PT PEntity[T]](ctx context.Context, q *datastore.Query) error {
	return NewClient[T, PT](defaultRawClient).DeleteByRawQuery(ctx, q)
}
