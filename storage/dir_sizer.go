package storage

import (
	"context"
	"sync"
)

// Result represents the Size function result
type Result struct {
	// Total Size of File objects
	Size int64
	// Count is a count of File objects processed
	Count int64
}

type DirSizer interface {
	// Size calculate a size of given Dir, receive a ctx and the root Dir instance
	// will return Result or error if happened
	Size(ctx context.Context, d Dir) (Result, error)
}

// sizer implement the DirSizer interface
type sizer struct {
	// maxWorkersCount number of workers for asynchronous run
	maxWorkersCount int

	// TODO: add other fields as you wish
}

// NewSizer returns new DirSizer instance
func NewSizer() DirSizer {
	return &sizer{5}
}

func (a *sizer) Size(ctx context.Context, d Dir) (Result, error) {
	sem := make(chan struct{}, a.maxWorkersCount)

	var wg sync.WaitGroup
	var mu sync.Mutex

	var finalRes Result
	var firstErr error

	var walk func(ctx context.Context, d Dir)

	walk = func(ctx context.Context, d Dir) {
		defer wg.Done()

		select {
		case <-ctx.Done():
			return
		default:
		}

		subdirs, files, err := d.Ls(ctx)

		if err != nil {
			mu.Lock()
			if firstErr == nil {
				firstErr = err
			}
			mu.Unlock()
			return
		}

		var localRes Result

		for _, f := range files {
			select {
			case <-ctx.Done():
				return
			default:
			}

			size, err := f.Stat(ctx)
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
				return
			}

			localRes.Size += size
			localRes.Count++
		}

		mu.Lock()
		finalRes.Count += localRes.Count
		finalRes.Size += localRes.Size
		mu.Unlock()

		for _, sd := range subdirs {
			select {
			case <-ctx.Done():
				return
			default:
			}

			wg.Add(1)
			sem <- struct{}{}
			go func(sd Dir) {
				defer func() { <-sem }()
				walk(ctx, sd)
			}(sd)

			mu.Lock()
			if firstErr != nil {
				mu.Unlock()
				return
			}
			mu.Unlock()
		}

	}

	wg.Add(1)
	sem <- struct{}{}
	go func() {
		defer func() { <-sem }()
		walk(ctx, d)
	}()
	wg.Wait()

	if firstErr != nil {
		return Result{}, firstErr
	}

	return finalRes, nil
}
