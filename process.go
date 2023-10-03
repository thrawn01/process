package process

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/mailgun/holster/v4/setter"
	"github.com/mailgun/holster/v4/syncutil"
	"github.com/sirupsen/logrus"
)

var (
	ShutdownCmd   = "SHUTDOWN"
	GreetingCmd   = "GREETING"
	HelloCmd      = "HELLO" // Used for testing
	OKCmd         = "OK"
	ERRORCmd      = "ERROR"
	ErrNotStarted = errors.New("process not running")

	idCounter int64
)

// HelloReq is used for testing a simple hello world process
type HelloReq struct {
	Name string `json:"name"`
}

// HelloResp is used for testing a simple hello world process
type HelloResp struct {
	Message string `json:"message"`
}

type Payload struct {
	Cmd  string
	Data []byte
}

// Process is your handle for interacting with the running process.
//
// Interaction is performed via a simple protocol. Process sends messages to the child process a line
// at a time (stream of bytes terminated by a `\n`) where each line is a single method invocation.
//
// The message consists of a `METHOD` followed by a space then the payload. Which looks like this
// `HELLO THIS IS MY PAYLOAD\n`
// "HELLO" is the method, everything after the space until the '\n' is the payload which can be any
//
//	serialization format you wish.
//
// The following is an example interaction between Process and the child process where `HELLO` is the
// method the child should execute.
// --> GREETING
// <-- OK
// --> HELLO {"name":"Thrawn"}
// <-- OK {"message":"Hello, Thrawn"}
// --> SHUTDOWN
// <-- OK
// <child process exits>
//
// You can use what ever serialization you wish for the payload. ExecuteJSON is provided to illustrate
// how you can use Send() and implement your own serialization method.
type Process interface {
	// ExecuteJSON sends an `METHOD <JSON object>\n` to a process via stdin, blocking until a JSON
	// response via stdout is received or context is cancelled
	ExecuteJSON(ctx context.Context, method string, in interface{}, out interface{}) error

	// Send a raw payload to the child, blocks until a response is received. `in.Data` MUST NOT be
	// terminated with a `\n`
	Send(ctx context.Context, in *Payload, out *Payload) error

	// Shutdown the process in a graceful manner.
	Shutdown(ctx context.Context) error

	// Kill the process immediately, only use this if Shutdown
	// does not finish in a timely manner.
	Kill()

	// IsRunning returns true if the command is running. Used by Pool.Put() to
	// determine if the process should be restarted.
	IsRunning() bool
}

var _ Process = &process{}

type process struct {
	wg       syncutil.WaitGroup
	stdout   *bufio.Reader
	shutdown chan struct{}
	proc     *os.Process
	mutex    sync.Mutex
	stdin    io.Writer
	conf     Config
	running  int64
}

// SpawnProcess creates a new process
func SpawnProcess(conf Config) (*process, error) {
	setter.SetDefault(&conf.StartTimeout, 10*time.Second)
	setter.SetDefault(&conf.Log, logrus.WithField("id", atomic.AddInt64(&idCounter, 1)))

	if conf.Command == "" {
		return nil, errors.New("conf.Command cannot be empty")
	}

	c := &process{
		conf: conf,
	}

	if err := c.spawnChild(); err != nil {
		return nil, fmt.Errorf("while spawning child process: %w", err)
	}
	return c, nil
}

func (p *process) spawnChild() error {
	errCh := make(chan error)

	// Run the child in a go routine
	p.wg.Go(func() { p.runChild(errCh) })

	err := <-errCh
	if err != nil {
		return fmt.Errorf("while spawning child: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), p.conf.StartTimeout)
	defer cancel()

	var resp Payload
	if err := p.Send(ctx, &Payload{Cmd: GreetingCmd}, &resp); err != nil {
		return fmt.Errorf("while sending initial greeting to child process: %w", err)
	}

	// Process should send us an `OK` when it's ready to receive commands
	if resp.Cmd != OKCmd {
		return fmt.Errorf("expected OK during greeting, got %#v instead", resp.Cmd)
	}

	return nil
}

func (p *process) runChild(errCh chan error) {
	cmd := exec.Command(p.conf.Command, p.conf.Args...)

	// Will spawn the child command into the same group so SIGTERM/SIGKILL will reap our
	// direct child process sub processes the child spawned. Should work for darwin and linux.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	p.stdin, _ = cmd.StdinPipe()
	stdoutR, _ := cmd.StdoutPipe()
	stderr, _ := cmd.StderrPipe()

	// Set our IO handles
	p.stdout = bufio.NewReader(stdoutR)

	p.wg.Go(func() {
		p.logStderr(stderr)
	})

	if err := cmd.Start(); err != nil {
		errCh <- fmt.Errorf("while running cmd.Start(): %w", err)
		return
	}

	p.proc = cmd.Process
	close(errCh)
	p.shutdown = make(chan struct{})
	atomic.StoreInt64(&p.running, 1)

	// Wait until the process exits
	_ = cmd.Wait()

	// Not sure if we actually need to collect the status code?
	//var exitErr *exec.ExitError
	//if err != nil {
	//} errors.As(err, &exitErr) {
	//	if waitStatus, ok := exitErr.Sys().(syscall.WaitStatus); ok {
	//		exitCode := waitStatus.ExitStatus() // -1 if signaled
	//		p.conf.Log.Warnf("child process terminated with non-zero status: '%d'", exitCode)
	//		//if waitStatus.Signaled() {
	//		//	err = errors.New(exitErr.Error()) // "signal: terminated"
	//		//}
	//	}
	//}

	close(p.shutdown)
	atomic.StoreInt64(&p.running, 0)
}

func (p *process) logStderr(r io.Reader) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		p.conf.Log.Error(scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		if err != io.EOF {
			p.conf.Log.WithError(err).Error("stderr reader ended with error")
		}
	}
}

// SendLine to the child process and receive a line, blocks until context is cancelled or line is received.
func (p *process) SendLine(ctx context.Context, in []byte, out io.Writer) error {
	errCh := make(chan error)
	var err error

	if atomic.LoadInt64(&p.running) == 0 {
		return ErrNotStarted
	}

	go func() {
		defer p.mutex.Unlock()
		p.mutex.Lock()

		if _, err := p.stdin.Write(in); err != nil {
			errCh <- fmt.Errorf("while writing to process: %w", err)
		}
		errCh <- nil
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err = <-errCh:
		if err != nil {
			return err
		}
	}

	go func() {
		defer p.mutex.Unlock()
		p.mutex.Lock()

		b, err := p.stdout.ReadBytes('\n')
		if err != nil {
			errCh <- fmt.Errorf("while reading from process: %w", err)
		}
		if _, err := out.Write(b); err != nil {
			errCh <- fmt.Errorf("while writing to io.Writer: %w", err)
		}
		close(errCh)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err = <-errCh:
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *process) Send(ctx context.Context, in *Payload, out *Payload) error {
	outBuf := bytes.Buffer{}
	outBuf.WriteString(in.Cmd)
	outBuf.WriteString(" ")
	outBuf.Write(in.Data)
	outBuf.WriteRune('\n')

	var inBuf bytes.Buffer
	if err := p.SendLine(ctx, outBuf.Bytes(), &inBuf); err != nil {
		return err
	}

	// Response should be in the form `OK <data>\n` or `OK\n`
	cmd, err := inBuf.ReadString(' ')
	if err != nil && err != io.EOF {
		return fmt.Errorf("while reading response from child: %w", err)
	}

	// TODO: We could continue to read until we get an OK response in case the child has a third-party
	//  library that spews noise to stdout occasionally.
	if len(cmd) <= 1 {
		return fmt.Errorf("expected OK message from child but got: %#v", cmd)
	}

	out.Cmd = strings.TrimRight(cmd, " \n")
	switch out.Cmd {
	case OKCmd, ERRORCmd:
	default:
		return fmt.Errorf("expected OK or ERROR message from child but got: %#v", cmd)
	}

	// The remainder of the line is our data minus the LF
	out.Data = inBuf.Bytes()[:inBuf.Len()-1]
	return nil
}

func (p *process) ExecuteJSON(ctx context.Context, command string, in interface{}, out interface{}) error {
	b, err := json.Marshal(in)
	if err != nil {
		return fmt.Errorf("while encoding json: %w", err)
	}

	var recv Payload
	//fmt.Printf("Send: %#v\n", string(b))
	if err := p.Send(ctx, &Payload{Cmd: command, Data: b}, &recv); err != nil {
		return fmt.Errorf("while sending command to child: %w", err)
	}

	if err := json.Unmarshal(recv.Data, out); err != nil {
		return fmt.Errorf("while unmarshalling process response: %w", err)
	}
	return nil
}

func (p *process) Shutdown(ctx context.Context) error {
	if atomic.LoadInt64(&p.running) == 0 {
		return ErrNotStarted
	}

	var resp Payload
	if err := p.Send(ctx, &Payload{Cmd: ShutdownCmd}, &resp); err != nil {
		return fmt.Errorf("while sending shutdown message: %w", err)
	}

	if resp.Cmd != OKCmd {
		return fmt.Errorf("expected OK during shutdown process, got %#v - %#v instead", resp.Cmd, string(resp.Data))
	}

	// Read any remaining data until EOF
	b, err := io.ReadAll(p.stdout)
	if err != nil {
		return fmt.Errorf("while checking for remaining data to read: %w", err)
	}

	if len(b) != 0 {
		p.conf.Log.Warnf("Got unexpected data from process after shutdown %#v", string(b))
	}

	// Wait here until 'runChild()' returns or we timeout
	select {
	case <-p.shutdown:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func (p *process) Kill() {
	if p.proc == nil {
		return
	}
	_ = p.proc.Kill()
}

func (p *process) IsRunning() bool {
	return atomic.LoadInt64(&p.running) == 1
}
