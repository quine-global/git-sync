/*
Copyright 2019 The Kubernetes Authors All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package hook

import (
	"context"
	"fmt"
	"testing"
	"time"

	"k8s.io/git-sync/pkg/logging"
)

const (
	hash1 = "1111111111111111111111111111111111111111"
	hash2 = "2222222222222222222222222222222222222222"
)

func TestHookData(t *testing.T) {
	t.Run("hook consumes first hash value", func(t *testing.T) {
		hd := NewHookData()

		hd.send(hash1)

		<-hd.events()

		hash := hd.get()
		if hash1 != hash {
			t.Fatalf("expected hash %s but got %s", hash1, hash)
		}
	})

	t.Run("last update wins when channel buffer is full", func(t *testing.T) {
		hd := NewHookData()

		for i := range 10 {
			h := fmt.Sprintf("111111111111111111111111111111111111111%d", i)
			hd.send(h)
		}
		hd.send(hash2)

		<-hd.events()

		hash := hd.get()
		if hash2 != hash {
			t.Fatalf("expected hash %s but got %s", hash2, hash)
		}
	})

	t.Run("same hash value", func(t *testing.T) {
		hd := NewHookData()
		events := hd.events()

		hd.send(hash1)
		<-events

		hash := hd.get()
		if hash1 != hash {
			t.Fatalf("expected hash %s but got %s", hash1, hash)
		}

		hd.send(hash1)
		<-events

		hash = hd.get()
		if hash1 != hash {
			t.Fatalf("expected hash %s but got %s", hash1, hash)
		}
	})
}

type recordingHook struct{ ran chan string }

func (h recordingHook) Name() string { return "recording" }
func (h recordingHook) Do(_ context.Context, hash string) error {
	h.ran <- hash
	return nil
}

func TestHookRunnerOnlyIf(t *testing.T) {
	hash3 := "3333333333333333333333333333333333333333"
	h := recordingHook{ran: make(chan string, 3)}
	asked := make(chan [2]string, 3)
	r := NewHookRunner(h, time.Millisecond, NewHookData(), logging.New("", "", 0), false)
	r.OnlyIf(func(_ context.Context, from, to string) bool {
		asked <- [2]string{from, to}
		return to == hash3
	})
	go r.Run(context.Background())

	wait := func(ch any) any {
		select {
		case v := <-ch.(chan string):
			return v
		case <-time.After(5 * time.Second):
			t.Fatal("timed out")
		}
		return nil
	}

	// The first hash runs without asking.
	r.Send(hash1)
	if got := wait(h.ran); got != hash1 {
		t.Fatalf("ran %v, want %s", got, hash1)
	}
	// hash2 changes nothing watched: skipped.
	r.Send(hash2)
	if got := <-asked; got != [2]string{hash1, hash2} {
		t.Fatalf("asked %v", got)
	}
	// hash3 does, and is compared with hash2, the skipped one.
	r.Send(hash3)
	if got := <-asked; got != [2]string{hash2, hash3} {
		t.Fatalf("asked %v", got)
	}
	if got := wait(h.ran); got != hash3 {
		t.Fatalf("ran %v, want %s", got, hash3)
	}
}
