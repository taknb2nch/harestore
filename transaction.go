package harestore

import (
	"context"

	"cloud.google.com/go/datastore"
)

// Transaction defines the interface for datastore transaction operations.
type Transaction interface {
	GetMulti(keys []*datastore.Key, dst any) error
	PutMulti(keys []*datastore.Key, src any) (ret []*datastore.PendingKey, err error)
	DeleteMulti(keys []*datastore.Key) (err error)
}

type contextKeyTransaction = struct{}

// WithTransaction injects a transaction into the context.
func WithTransaction(ctx context.Context, tx Transaction) context.Context {
	return context.WithValue(ctx, contextKeyTransaction{}, tx)
}

// extractTransactionFromContext extracts transacton from context.
func extractTransactionFromContext(ctx context.Context) (Transaction, bool) {
	trans, ok := ctx.Value(contextKeyTransaction{}).(Transaction)
	if !ok {
		return nil, false
	}

	if trans == nil {
		return nil, false
	}

	return trans, true
}
