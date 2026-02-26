package harestore

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/google/uuid"
	"google.golang.org/api/iterator"
)

// batch size (limit for Datastore)
const (
	batchSizeRead   = 1000
	batchSizeMutate = 500
	maxErrorCount   = 500

	defaultMaxConcurrency = 32
	defaultGlobalTimeout  = 0
	defaultBatchTimeout   = 0
)

type clientConfig struct {
	maxConcurrency int
	globalTimeout  time.Duration
	batchTimeout   time.Duration
}

// ClientOption
type ClientOption func(*clientConfig)

// WithMaxConcurrency
func WithMaxConcurrency(n int) ClientOption {
	return func(c *clientConfig) {
		if n > 0 {
			c.maxConcurrency = n
		}
	}
}

// WithGlobalTimeout
func WithGlobalTimeout(d time.Duration) ClientOption {
	return func(c *clientConfig) {
		c.globalTimeout = d
	}
}

// WithBatchTimeout
func WithBatchTimeout(d time.Duration) ClientOption {
	return func(c *clientConfig) {
		c.batchTimeout = d
	}
}

var (
	ErrNilEntity     = errors.New("harestore: entity is nil")
	ErrInvalidID     = errors.New("harestore: id cannot be empty")
	ErrInvalidKind   = errors.New("harestore: kind cannot be empty")
	ErrInvalidQuery  = errors.New("harestore: query cannot be nil")
	ErrInvalidEntity = errors.New("harestore: entity cannot be nil")
)

// Entity is the minimal requirement for an object to be stored in Datastore.
type Entity interface {
	KindName() string
	GetID() string
	SetID(id string)
}

// Creator allows the entity to automatically record its creation time.
type Creator interface {
	SetCreatedAt(createdAt time.Time)
}

// Updater allows the entity to automatically record its last update time.
type Updater interface {
	SetUpdatedAt(updatedAt time.Time)
}

// Versioner enables optimistic concurrency control for the entity.
type Versioner interface {
	SetVersion(version int)
	GetVersion() int
}

// PEntity indicates that the pointer of T implements Entity.
type PEntity[T any] interface {
	Entity
	*T
}

// GenerateUUID creates a UUID that is collision resistant.
func GenerateUUID() string {
	return uuid.New().String()
}

// newID creates a new Key.
func newID(kind string, id string) *datastore.Key {
	return datastore.NameKey(kind, id, nil)
}

// Client provides methods to interact with Google Cloud Datastore.
type Client[T any, PT PEntity[T]] struct {
	Raw    *datastore.Client
	config clientConfig
}

// NewClient creates a new Repository instance.
func NewClient[T any, PT PEntity[T]](client *datastore.Client, opts ...ClientOption) *Client[T, PT] {
	cfg := clientConfig{
		maxConcurrency: defaultMaxConcurrency,
		globalTimeout:  defaultGlobalTimeout,
		batchTimeout:   defaultBatchTimeout,
	}

	for _, opt := range opts {
		opt(&cfg)
	}

	return &Client[T, PT]{
		Raw:    client,
		config: cfg,
	}
}

// RunInTransaction starts a transaction.
func (c *Client[T, PT]) RunInTransaction(ctx context.Context, f func(ctxWithTransaction context.Context) error) error {
	if _, ok := extractTransactionFromContext(ctx); ok {
		err := f(ctx)
		if err != nil {
			return err
		}

		return nil
	}

	_, err := c.Raw.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		ctxWithTx := WithTransaction(ctx, tx)

		return f(ctxWithTx)
	})

	return err
}

// GetByID retrieves one entity by specifying id.
func (c *Client[T, PT]) GetByID(ctx context.Context, id string) (*T, error) {
	if id == "" {
		return nil, ErrInvalidID
	}

	entities, err := c.GetMultiByID(ctx, []string{id})

	if err != nil {
		if merr, ok := err.(datastore.MultiError); ok {
			return nil, fmt.Errorf("harestore: failed to get %T (id=%s): %w", new(T), id, merr[0])
		}

		return nil, fmt.Errorf("harestore: failed to get %T (id=%s): %w", new(T), id, err)
	}

	if len(entities) == 0 || entities[0] == nil {
		return nil, fmt.Errorf("harestore: failed to get %T (id=%s): %w", new(T), id, datastore.ErrNoSuchEntity)
	}

	return entities[0], nil
}

// Insert registers one entity.
func (c *Client[T, PT]) Insert(ctx context.Context, entity *T) (string, error) {
	ids, err := c.InsertMulti(ctx, []*T{entity})
	if err != nil {
		if merr, ok := err.(datastore.MultiError); ok {
			return "", fmt.Errorf("harestore: failed to insert %T: %w", new(T), merr[0])
		}

		return "", fmt.Errorf("harestore: failed to insert %T: %w", new(T), err)
	}

	if len(ids) == 0 || ids[0] == "" {
		return "", fmt.Errorf("harestore: failed to insert %T: %w", new(T), err)
	}

	return ids[0], nil
}

// Update updates one entity.
func (c *Client[T, PT]) Update(ctx context.Context, entity *T) error {
	if entity == nil {
		return ErrInvalidEntity
	}

	err := c.UpdateMulti(ctx, []*T{entity})
	if err != nil {
		id := PT(entity).GetID()

		if merr, ok := err.(datastore.MultiError); ok {
			return fmt.Errorf("harestore: failed to update %T (id=%s): %w", new(T), id, merr[0])
		}

		return fmt.Errorf("harestore: failed to update %T (id=%s): %w", new(T), id, err)
	}

	return nil
}

// DeleteByID deletes one entity by specifying id.
func (c *Client[T, PT]) DeleteByID(ctx context.Context, id string) error {
	if id == "" {
		return ErrInvalidID
	}

	err := c.DeleteMultiByID(ctx, []string{id})
	if err != nil {
		if merr, ok := err.(datastore.MultiError); ok {
			return fmt.Errorf("harestore: failed to delete %T (id=%s): %w", new(T), id, merr[0])
		}

		return fmt.Errorf("harestore: failed to delete %T (id=%s): %w", new(T), id, err)
	}

	return nil
}

// Delete deletes the specifying entity.
func (c *Client[T, PT]) Delete(ctx context.Context, entity *T) error {
	if entity == nil {
		return ErrInvalidEntity
	}

	err := c.DeleteMulti(ctx, []*T{entity})
	if err != nil {
		id := PT(entity).GetID()

		if merr, ok := err.(datastore.MultiError); ok {
			return fmt.Errorf("harestore: failed to delete %T (id=%s): %w", new(T), id, merr[0])
		}

		return fmt.Errorf("harestore: failed to delete %T (id=%s): %w", new(T), id, err)
	}

	return nil
}

// GetMultiByID retrieves the entities by specifing ids.
func (c *Client[T, PT]) GetMultiByID(ctx context.Context, ids []string) ([]*T, error) {
	if len(ids) == 0 {
		return make([]*T, 0), nil
	}

	var t T

	entity := PT(&t)

	if entity.KindName() == "" {
		return nil, ErrInvalidKind
	}

	keys := make([]*datastore.Key, 0, len(ids))

	for _, id := range ids {
		if id == "" {
			return nil, ErrInvalidID
		}

		keys = append(keys, newID(entity.KindName(), id))
	}

	allEntities := make([]*T, len(keys))
	combinedErr := make(datastore.MultiError, len(keys))

	var hasError int32 = 0

	sem := make(chan struct{}, c.config.maxConcurrency)
	var wg sync.WaitGroup

	for i := 0; i < len(keys); i += batchSizeRead {
		start := i
		end := min(i+batchSizeRead, len(keys))

		wg.Go(func() {
			sem <- struct{}{}
			defer func() { <-sem }()

			targetErrSlice := combinedErr[start:end]
			targetEntitySlice := allEntities[start:end]

			batchKeys := keys[start:end]

			batchEntities := make([]*T, len(batchKeys))

			for j := range batchEntities {
				batchEntities[j] = new(T)
			}

			var err error

			if tx, ok := extractTransactionFromContext(ctx); ok {
				err = tx.GetMulti(batchKeys, batchEntities)
			} else {
				err = c.Raw.GetMulti(ctx, batchKeys, batchEntities)
			}

			if err != nil {
				atomic.StoreInt32(&hasError, 1)

				if merr, ok := err.(datastore.MultiError); ok {
					copy(targetErrSlice, merr)
				} else {
					wrappedErr := fmt.Errorf("harestore: failed to execute batch get: %w", err)

					for k := range batchKeys {
						targetErrSlice[k] = wrappedErr
					}
				}
			}

			for k, e := range batchEntities {
				if targetErrSlice[k] == nil {
					targetEntitySlice[k] = e

					if e != nil {
						PT(e).SetID(batchKeys[k].Name)
					}
				}
			}
		})
	}

	wg.Wait()

	if atomic.LoadInt32(&hasError) == 1 {
		return allEntities, combinedErr
	}

	return allEntities, nil
}

// InsertMulti inserts the specifing entities.
func (c *Client[T, PT]) InsertMulti(ctx context.Context, entities []*T) ([]string, error) {
	if len(entities) == 0 {
		return []string{}, nil
	}

	if c.config.globalTimeout > 0 {
		var cancel context.CancelFunc

		ctx, cancel = context.WithTimeout(ctx, c.config.globalTimeout)

		defer cancel()
	}

	allIDs := make([]string, len(entities))
	combinedErr := make(datastore.MultiError, len(entities))

	var hasError int32 = 0

	sem := make(chan struct{}, c.config.maxConcurrency)

	var wg sync.WaitGroup

	now := time.Now()

	for i := 0; i < len(entities); i += batchSizeMutate {
		start := i
		end := min(i+batchSizeMutate, len(entities))

		wg.Go(func() {
			targetErrSlice := combinedErr[start:end]
			targetIDs := allIDs[start:end]
			batchEntities := entities[start:end]

			select {
			case sem <- struct{}{}:
				defer func() {
					<-sem
				}()

			case <-ctx.Done():
				atomic.StoreInt32(&hasError, 1)

				err := ctx.Err()

				for i := range targetErrSlice {
					targetErrSlice[i] = err
				}

				return
			}

			validKeys := make([]*datastore.Key, 0, len(batchEntities))
			validEntities := make([]*T, 0, len(batchEntities))
			validIndices := make([]int, 0, len(batchEntities))

			for idx := range batchEntities {
				entity := PT(batchEntities[idx])

				if entity == nil {
					atomic.StoreInt32(&hasError, 1)

					targetErrSlice[idx] = ErrNilEntity

					continue
				}

				if entity.KindName() == "" {
					atomic.StoreInt32(&hasError, 1)

					targetErrSlice[idx] = ErrInvalidKind

					continue
				}

				if entity.GetID() == "" {
					entity.SetID(GenerateUUID())
				}

				if v, ok := any(entity).(Creator); ok {
					v.SetCreatedAt(now)
				}

				if v, ok := any(entity).(Versioner); ok {
					v.SetVersion(1)
				}

				key := newID(entity.KindName(), entity.GetID())

				validKeys = append(validKeys, key)
				validEntities = append(validEntities, entity)
				validIndices = append(validIndices, idx)
			}

			if len(validKeys) == 0 {
				return
			}

			err := c.executePutBatch(ctx, validKeys, validEntities)
			if err != nil {
				atomic.StoreInt32(&hasError, 1)

				if merr, ok := err.(datastore.MultiError); ok {
					for k, e := range merr {
						if e != nil {
							originalIndex := validIndices[k]
							targetErrSlice[originalIndex] = e
						}
					}
				} else {
					wrappedErr := fmt.Errorf("harestore: failed to insert batch: %w", err)

					for _, originalIndex := range validIndices {
						targetErrSlice[originalIndex] = wrappedErr
					}
				}
			}

			for k, key := range validKeys {
				originalIndex := validIndices[k]

				if targetErrSlice[originalIndex] == nil {
					targetIDs[originalIndex] = key.Name
				}
			}
		})
	}

	wg.Wait()

	if atomic.LoadInt32(&hasError) == 1 {
		return allIDs, combinedErr
	}

	return allIDs, nil
}

// UpdateMulti updates the specifing entities.
func (c *Client[T, PT]) UpdateMulti(ctx context.Context, entities []*T) error {
	if len(entities) == 0 {
		return nil
	}

	if c.config.globalTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.config.globalTimeout)

		defer cancel()
	}

	combinedErr := make(datastore.MultiError, len(entities))
	var hasError int32 = 0

	sem := make(chan struct{}, c.config.maxConcurrency)
	var wg sync.WaitGroup

	now := time.Now()

	for i := 0; i < len(entities); i += batchSizeMutate {
		start := i
		end := min(i+batchSizeMutate, len(entities))

		wg.Go(func() {
			targetErrSlice := combinedErr[start:end]
			batchEntities := entities[start:end]

			select {
			case sem <- struct{}{}:
				defer func() {
					<-sem
				}()

			case <-ctx.Done():
				atomic.StoreInt32(&hasError, 1)

				err := ctx.Err()

				for i := range targetErrSlice {
					targetErrSlice[i] = err
				}

				return
			}

			validKeys := make([]*datastore.Key, 0, len(batchEntities))
			validEntities := make([]*T, 0, len(batchEntities))
			validIndices := make([]int, 0, len(batchEntities))

			for idx := range batchEntities {
				entity := PT(batchEntities[idx])

				if entity == nil {
					atomic.StoreInt32(&hasError, 1)

					targetErrSlice[idx] = ErrNilEntity

					continue
				}

				if entity.KindName() == "" {
					atomic.StoreInt32(&hasError, 1)

					targetErrSlice[idx] = ErrInvalidKind

					continue
				}

				if entity.GetID() == "" {
					atomic.StoreInt32(&hasError, 1)

					targetErrSlice[idx] = ErrInvalidID

					continue
				}

				if v, ok := any(entity).(Updater); ok {
					v.SetUpdatedAt(now)
				}

				if v, ok := any(entity).(Versioner); ok {
					v.SetVersion(v.GetVersion() + 1)
				}

				key := newID(entity.KindName(), entity.GetID())

				validKeys = append(validKeys, key)
				validEntities = append(validEntities, entity)
				validIndices = append(validIndices, idx)
			}

			if len(validKeys) == 0 {
				return
			}

			err := c.executePutBatch(ctx, validKeys, validEntities)
			if err != nil {
				atomic.StoreInt32(&hasError, 1)

				if merr, ok := err.(datastore.MultiError); ok {
					for k, e := range merr {
						if e != nil {
							originalIndex := validIndices[k]
							targetErrSlice[originalIndex] = e
						}
					}
				} else {
					wrappedErr := fmt.Errorf("harestore: failed to update batch: %w", err)

					for _, originalIndex := range validIndices {
						targetErrSlice[originalIndex] = wrappedErr
					}
				}
			}
		})
	}

	wg.Wait()

	if atomic.LoadInt32(&hasError) == 1 {
		return combinedErr
	}

	return nil
}

// DeleteMultiByID deletes the entities by specifing ids.
func (c *Client[T, PT]) DeleteMultiByID(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}

	if c.config.globalTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.config.globalTimeout)

		defer cancel()
	}

	var t T

	entity := PT(&t)

	kind := entity.KindName()

	if kind == "" {
		return ErrInvalidKind
	}

	combinedErr := make(datastore.MultiError, len(ids))
	var hasError int32 = 0

	sem := make(chan struct{}, c.config.maxConcurrency)
	var wg sync.WaitGroup

	for i := 0; i < len(ids); i += batchSizeMutate {
		start := i
		end := min(i+batchSizeMutate, len(ids))

		wg.Go(func() {
			targetErrSlice := combinedErr[start:end]
			batchIds := ids[start:end]

			select {
			case sem <- struct{}{}:
				defer func() {
					<-sem
				}()

			case <-ctx.Done():
				atomic.StoreInt32(&hasError, 1)

				err := ctx.Err()

				for i := range targetErrSlice {
					targetErrSlice[i] = err
				}

				return
			}

			validKeys := make([]*datastore.Key, 0, len(batchIds))
			validIndices := make([]int, 0, len(batchIds))

			for idx, id := range batchIds {
				if id == "" {
					atomic.StoreInt32(&hasError, 1)

					targetErrSlice[idx] = ErrInvalidID
					continue
				}

				key := newID(kind, id)

				validKeys = append(validKeys, key)
				validIndices = append(validIndices, idx)
			}

			if len(validKeys) == 0 {
				return
			}

			err := c.executeDeleteBatch(ctx, validKeys)
			if err != nil {
				atomic.StoreInt32(&hasError, 1)

				if merr, ok := err.(datastore.MultiError); ok {
					for k, e := range merr {
						if e != nil {
							originalIndex := validIndices[k]
							targetErrSlice[originalIndex] = e
						}
					}
				} else {
					wrappedErr := fmt.Errorf("harestore: failed to delete batch: %w", err)

					for _, originalIndex := range validIndices {
						targetErrSlice[originalIndex] = wrappedErr
					}
				}
			}
		})
	}

	wg.Wait()

	if atomic.LoadInt32(&hasError) == 1 {
		return combinedErr
	}

	return nil
}

// DeleteMulti deletes the entities.
func (c *Client[T, PT]) DeleteMulti(ctx context.Context, entities []*T) error {
	if len(entities) == 0 {
		return nil
	}

	if c.config.globalTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.config.globalTimeout)

		defer cancel()
	}

	combinedErr := make(datastore.MultiError, len(entities))
	var hasError int32 = 0

	sem := make(chan struct{}, c.config.maxConcurrency)
	var wg sync.WaitGroup

	// ループ方式を統一: i += batchSizeMutate
	for i := 0; i < len(entities); i += batchSizeMutate {
		start := i
		end := min(i+batchSizeMutate, len(entities))

		wg.Go(func() {
			targetErrSlice := combinedErr[start:end]
			batchEntities := entities[start:end]

			select {
			case sem <- struct{}{}:
				defer func() {
					<-sem
				}()

			case <-ctx.Done():
				atomic.StoreInt32(&hasError, 1)

				err := ctx.Err()

				for i := range targetErrSlice {
					targetErrSlice[i] = err
				}

				return
			}

			validKeys := make([]*datastore.Key, 0, len(batchEntities))
			validIndices := make([]int, 0, len(batchEntities))

			for idx := range batchEntities {
				entity := PT(batchEntities[idx])

				if entity == nil {
					atomic.StoreInt32(&hasError, 1)

					targetErrSlice[idx] = ErrNilEntity
					continue
				}

				if entity.KindName() == "" {
					atomic.StoreInt32(&hasError, 1)

					targetErrSlice[idx] = ErrInvalidKind

					continue
				}

				if entity.GetID() == "" {
					atomic.StoreInt32(&hasError, 1)

					targetErrSlice[idx] = ErrInvalidKind

					continue
				}

				key := newID(entity.KindName(), entity.GetID())

				validKeys = append(validKeys, key)
				validIndices = append(validIndices, idx)
			}

			if len(validKeys) == 0 {
				return
			}

			err := c.executeDeleteBatch(ctx, validKeys)
			if err != nil {
				atomic.StoreInt32(&hasError, 1)

				if merr, ok := err.(datastore.MultiError); ok {
					for k, e := range merr {
						if e != nil {
							originalIndex := validIndices[k]
							targetErrSlice[originalIndex] = e
						}
					}
				} else {
					wrappedErr := fmt.Errorf("harestore: failed to update batch: %w", err)

					for _, originalIndex := range validIndices {
						targetErrSlice[originalIndex] = wrappedErr
					}
				}
			}
		})
	}

	wg.Wait()

	if atomic.LoadInt32(&hasError) == 1 {
		return combinedErr
	}

	return nil
}

// RunRawQuery executes the query.
func (c *Client[T, PT]) RunRawQuery(ctx context.Context, q *datastore.Query) ([]*T, error) {
	if q == nil {
		return nil, ErrInvalidQuery
	}

	if tx, ok := extractTransactionFromContext(ctx); ok {
		if tx, ok := tx.(*datastore.Transaction); ok {
			q = q.Transaction(tx)
		}
	}

	it := c.Raw.Run(ctx, q)

	entities := make([]*T, 0)

	for {
		var entity T

		key, err := it.Next(&entity)
		if err == iterator.Done {
			break
		} else if err != nil {
			return nil, fmt.Errorf("could not execute iterator.Next: %w", err)
		}

		pt := PT(&entity)
		pt.SetID(key.Name)

		entities = append(entities, &entity)
	}

	return entities, nil
}

// DeleteByRawQuery deletes entities retrieved by executing a query.
func (c *Client[T, PT]) DeleteByRawQuery(ctx context.Context, q *datastore.Query) error {
	if q == nil {
		return ErrInvalidQuery
	}

	if c.config.globalTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.config.globalTimeout)

		defer cancel()
	}

	q = q.KeysOnly()

	if tx, ok := extractTransactionFromContext(ctx); ok {
		if tx, ok := tx.(*datastore.Transaction); ok {
			q = q.Transaction(tx)
		}
	}

	it := c.Raw.Run(ctx, q)

	sem := make(chan struct{}, c.config.maxConcurrency)
	var wg sync.WaitGroup

	errChan := make(chan error, c.config.maxConcurrency*2)
	var stopSignal int32

	resultChan := make(chan datastore.MultiError, 1)

	go func() {
		var allErrors datastore.MultiError
		var errorCount int

		for err := range errChan {
			if merr, ok := err.(datastore.MultiError); ok {
				for _, e := range merr {
					if e == nil {
						continue
					}

					errorCount++

					if len(allErrors) < maxErrorCount {
						allErrors = append(allErrors, e)
					}
				}
			} else if _, ok := err.(datastore.MultiError); !ok {
				atomic.StoreInt32(&stopSignal, 1)

				errorCount++

				if len(allErrors) < maxErrorCount {
					allErrors = append(allErrors, err)
				}
			}
		}

		if errorCount > len(allErrors) {
			remaining := errorCount - len(allErrors)
			allErrors = append(allErrors, fmt.Errorf("...and %d more errors (total %d failures)", remaining, errorCount))
		}

		resultChan <- allErrors
	}()

	executeBatch := func(keys []*datastore.Key) {
		defer wg.Done()

		// 既に誰かが致命的なエラーを踏んでいたら、処理せず帰る（無駄な抵抗はしない）
		if atomic.LoadInt32(&stopSignal) == 1 {
			return
		}

		select {
		case sem <- struct{}{}:
			defer func() {
				<-sem
			}()

		case <-ctx.Done():
			select {
			case errChan <- ctx.Err():
			default:
			}

			return
		}

		if atomic.LoadInt32(&stopSignal) == 1 {
			return
		}

		err := c.executeDeleteBatch(ctx, keys)
		if err != nil {
			errChan <- err
		}
	}

	batchKeys := make([]*datastore.Key, 0, batchSizeMutate)

	for {
		if atomic.LoadInt32(&stopSignal) == 1 {
			break
		}

		key, err := it.Next(nil)

		if err == iterator.Done {
			break
		} else if err != nil {
			errChan <- fmt.Errorf("harestore: iterator failed: %w", err)

			break
		}

		batchKeys = append(batchKeys, key)

		if len(batchKeys) >= batchSizeMutate {
			keysToProcess := make([]*datastore.Key, len(batchKeys))

			copy(keysToProcess, batchKeys)

			wg.Add(1)

			go executeBatch(keysToProcess)

			batchKeys = batchKeys[:0]
		}
	}

	if len(batchKeys) > 0 && atomic.LoadInt32(&stopSignal) == 0 {
		wg.Add(1)
		go executeBatch(batchKeys)
	}

	wg.Wait()

	close(errChan)

	finalErrors := <-resultChan

	if len(finalErrors) > 0 {
		return finalErrors
	}

	return nil
}

// executePutBatch executes put operation for keys and entities.
func (c *Client[T, PT]) executePutBatch(ctx context.Context, keys []*datastore.Key, entities []*T) error {
	if len(keys) != len(entities) {
		return fmt.Errorf("keys and entities count mismatch : %d and %d", len(keys), len(entities))
	}

	if len(keys) == 0 {
		return nil
	}

	if c.config.batchTimeout > 0 {
		var cancel context.CancelFunc

		ctx, cancel = context.WithTimeout(ctx, c.config.batchTimeout)

		defer cancel()
	}

	var err error

	if tx, ok := extractTransactionFromContext(ctx); ok {
		_, err = tx.PutMulti(keys, entities)
	} else {
		_, err = c.Raw.PutMulti(ctx, keys, entities)
	}

	if err != nil {
		if merr, ok := err.(datastore.MultiError); ok {
			return merr
		}

		return fmt.Errorf("harestore: failed to execute batch put: %w", err)
	}

	return nil
}

// executeDeleteBatch executes delete operation for keys.
func (c *Client[T, PT]) executeDeleteBatch(ctx context.Context, keys []*datastore.Key) error {
	if len(keys) == 0 {
		return nil
	}

	if c.config.batchTimeout > 0 {
		var cancel context.CancelFunc

		ctx, cancel = context.WithTimeout(ctx, c.config.batchTimeout)

		defer cancel()
	}

	var err error

	if tx, ok := extractTransactionFromContext(ctx); ok {
		err = tx.DeleteMulti(keys)
	} else {
		err = c.Raw.DeleteMulti(ctx, keys)
	}

	if err != nil {
		if merr, ok := err.(datastore.MultiError); ok {
			return merr
		}

		return fmt.Errorf("harestore: failed to execute batch delete: %w", err)
	}

	return nil
}
