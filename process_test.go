package process_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thrawn01/process"
)

func BenchmarkSpawnProcess(b *testing.B) {
	logrus.SetLevel(logrus.InfoLevel)

	p, err := process.SpawnProcess(process.Config{
		Args:    []string{"run", "cmd/child/main.go"},
		Command: "go",
	})
	if err != nil {
		b.Errorf("SpawnProcess: %s", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	b.Run("HelloWorld", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			var resp process.HelloResp
			err = p.ExecuteJSON(ctx, process.HelloCmd, process.HelloReq{Name: "World"}, &resp)
			if err != nil {
				b.Errorf("ExecuteJSON: %s", err)
			}
		}
	})

	err = p.Shutdown(ctx)
	if err != nil {
		b.Errorf("Shutdown: %s", err)
	}
}

func TestPool(t *testing.T) {
	pool, err := process.NewPool(process.Config{
		Args:    []string{"run", "cmd/child/main.go"},
		Command: "go",
	}, 10)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	respCh := make(chan process.HelloResp)

	// Demonstrate concurrent access to the pool
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			p, err := pool.Get(ctx)
			require.NoError(t, err)
			var resp process.HelloResp
			err = p.ExecuteJSON(ctx, process.HelloCmd, process.HelloReq{Name: "World"}, &resp)
			require.NoError(t, err)
			respCh <- resp
			_ = pool.Put(p)
			wg.Done()
		}()
	}

	go func() {
		wg.Wait()
		close(respCh)
	}()

	var count int
	for r := range respCh {
		assert.Equal(t, "Hello, World", r.Message)
		count++
	}
	assert.Equal(t, count, 10)
}

func TestKill(t *testing.T) {
	p, err := process.SpawnProcess(process.Config{
		Args:    []string{"run", "cmd/child/main.go"},
		Command: "go",
	})
	require.NoError(t, err)
	require.NotNil(t, p)
	require.True(t, p.IsRunning())

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// Attempt to execute a command
	err = p.ExecuteJSON(ctx, "SLEEP", nil, nil)
	require.Error(t, err)

	// It should timeout
	assert.Equal(t, context.DeadlineExceeded, errors.Unwrap(err))

	// Attempt to shutdown the process
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	err = p.Shutdown(ctx)

	// It should timeout
	assert.Equal(t, context.DeadlineExceeded, errors.Unwrap(err))
	require.True(t, p.IsRunning())

	// Kill the child process
	p.Kill()

	// The process go routines should register the command
	// exited and mark the process as not running.
	time.Sleep(3 * time.Second)
	require.False(t, p.IsRunning())
}
