package process

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/sirupsen/logrus"
)

type Pool interface {
	// Shutdown all processes, this method blocks until all processes have
	// gracefully terminated or context is cancelled.
	Shutdown(context.Context) error

	// Kill all processes in the pool immediately, call this if Shutdown()
	// does not finish in a timely manner.
	Kill()

	// Get a Process from the pool, blocks until a process is available
	// or context is cancelled.
	Get(context.Context) (Process, error)

	// Put a Process back in the pool, restarting the process if necessary.
	Put(Process) error
}

var _ Pool = &pool{}

type Config struct {
	StartTimeout time.Duration
	Command      string
	Env          []string
	Dir          string
	Args         []string
	Log          logrus.FieldLogger
}

// A Pool of active Process
type pool struct {
	poolCh chan Process
	conf   Config
}

// NewPool creates a new pool filling it with processes defined in config
func NewPool(conf Config, size int) (*pool, error) {
	p := &pool{
		poolCh: make(chan Process, size),
		conf:   conf,
	}

	for i := 0; i < size; i++ {
		ins, err := SpawnProcess(conf)
		if err != nil {
			return p, err
		}
		// Wait here for slow CPU to start subprocess
		for !ins.IsRunning() {
			time.Sleep(time.Millisecond * 100)
		}
		p.poolCh <- ins
	}
	return p, nil
}

func (p *pool) Shutdown(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		for c := range p.poolCh {
			if err := c.Shutdown(ctx); err != nil {
				log.Printf("during shutdown: %s", err)
			}
		}
		close(done)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}

func (p *pool) Kill() {
	for c := range p.poolCh {
		c.Kill()
	}
}

func (p *pool) Get(ctx context.Context) (Process, error) {
	select {
	case ins := <-p.poolCh:
		return ins, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *pool) Put(ins Process) error {
	if ins.IsRunning() {
		p.poolCh <- ins
		return nil
	}

	ins, err := SpawnProcess(p.conf)
	if err != nil {
		return fmt.Errorf("while respawning process process")
	}
	p.poolCh <- ins
	return nil
}
