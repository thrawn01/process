// An example file process
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/thrawn01/process"
)

var HelloCmd = "HELLO"
var SleepCmd = "SLEEP"

type HelloReq struct {
	Name string `json:"name"`
}

type HelloResp struct {
	Message string `json:"message"`
}

var fd io.Writer

func main() {
	var err error
	fd, err = os.Create("trace.io")
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to create trace.io\n")
		return
	}

	r := bufio.NewReader(os.Stdin)
	for {
		b, err := r.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				fmt.Fprintf(os.Stderr, "terminated, stdin got EOF\n")
				return
			}
			fmt.Fprintf(os.Stderr, "while reading line: '%s'\n", err)
			continue
		}
		fmt.Fprintf(fd, "--> %#v\n", string(b))

		buf := bytes.NewBuffer(b)
		method, err := buf.ReadString(' ')
		if err != nil && err != io.EOF {
			fmt.Fprintf(os.Stderr, "while reading method: '%s'\n", err)
			continue
		}

		if len(method) <= 1 {
			sendErr(fmt.Errorf("malformed method: '%#v'\n", method))
			continue
		}

		// Remove the trailing space and LF
		method = strings.TrimRight(method, " \n")

		switch method {
		// All processes should implement these two methods
		case process.GreetingCmd, process.ShutdownCmd:
			if err := sendOK(nil); err != nil {
				fmt.Fprintf(os.Stderr, "while writting to stdout: '%s'\n", err)
			}

			if method == process.ShutdownCmd {
				return
			}

		// HELLO {"name": "Thrawn"}
		case HelloCmd:
			var req HelloReq
			if err := json.Unmarshal(buf.Bytes(), &req); err != nil {
				sendErr(fmt.Errorf("while unmarshalling HelloReq %w\n", err))
				continue
			}
			b, err := json.Marshal(HelloResp{Message: fmt.Sprintf("Hello, %s", req.Name)})
			if err != nil {
				sendErr(fmt.Errorf("while marshalling ShutdownResp %w\n", err))
				continue
			}
			if err := sendOK(b); err != nil {
				fmt.Fprintf(os.Stderr, "while writting to stdout: %s\n", err)
				continue
			}
		// Used in tests so we can force kill the process
		case SleepCmd:
			time.Sleep(30 * time.Second)
			if err := sendOK(nil); err != nil {
				fmt.Fprintf(os.Stderr, "while writting to stdout: %s\n", err)
				continue
			}
		default:
			sendErr(fmt.Errorf("unknown method: '%s'", method))
		}

	}
}

func sendErr(err error) {
	fmt.Fprintf(os.Stdout, "%s %s\n", process.ERRORCmd, err)
	fmt.Fprintf(fd, "<-- %s %s\n", process.ERRORCmd, err)
}

func sendOK(msg []byte) error {
	var buf bytes.Buffer
	buf.Write([]byte(process.OKCmd + " "))
	buf.Write(msg)
	buf.WriteRune('\n')
	fmt.Fprintf(fd, "<-- %#v\n", buf.String())
	_, err := os.Stdout.Write(buf.Bytes())
	return err
}
