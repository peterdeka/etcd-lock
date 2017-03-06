/*
Copyright 2014 Datawise Systems Inc. All rights reserved.

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

package etcdlock

import (
	"flag"
	"fmt"
	"time"

	etcdCli "github.com/coreos/etcd/client"
	"golang.org/x/net/context"
)

// IsEtcdNotFound checks if the err is a not found error.
func IsEtcdNotFound(err error) bool {
	etcdErr, ok := err.(etcdCli.Error)
	return ok && etcdErr.Code == etcdCli.ErrorCodeKeyNotFound
}

// IsEtcdWatchStoppedByUser checks if err indicates watch stopped by user.
func IsEtcdWatchStoppedByUser(err error) bool {
	return err == context.Canceled
}

func IsEtcdEventIndexCleared(err error) bool {
	etcdErr, ok := err.(etcdCli.Error)
	return ok && etcdErr.Code == etcdCli.ErrorCodeEventIndexCleared
}

// Etcd client interface.
type Registry interface {
	// Add a new file with a random etcd-generated key under the given path
	//****AddChild(key string, value string, ttl uint64) (*etcd.Response, error)

	// Get gets the file or directory associated with the given key.
	// If the key points to a directory, files and directories under
	// it will be returned in sorted or unsorted order, depending on
	// the sort flag.
	// If recursive is set to false, contents under child directories
	// will not be returned.
	// If recursive is set to true, all the contents will be returned.
	Get(key string, sort, recursive bool) (*etcdCli.Response, error)

	// Set sets the given key to the given value.
	// It will create a new key value pair or replace the old one.
	// It will not replace a existing directory.
	Set(key string, value string, ttl time.Duration) (*etcdCli.Response, error)

	// Update updates the given key to the given value. It succeeds only if the given key
	// already exists.
	//****Update(key string, value string, ttl uint64) (*etcd.Response, error)

	// Create creates a file with the given value under the given key. It succeeds
	// only if the given key does not yet exist.
	Create(key string, value string, ttl time.Duration) (*etcdCli.Response, error)

	// CreateInOrder creates a file with a key that's guaranteed to be higher than other
	// keys in the given directory. It is useful for creating queues.
	//****CreateInOrder(dir string, value string, ttl uint64) (*etcd.Response, error)

	// CreateDir create a driectory. It succeeds only if the given key
	// does not yet exist.
	//*******CreateDir(key string, ttl uint64) (*etcd.Response, error)

	// Compare and swap only if prevValue & prevIndex match
	CompareAndSwap(key string, value string, ttl time.Duration, prevValue string,
		prevIndex uint64) (*etcdCli.Response, error)

	// Delete deletes the given key.
	// When recursive set to false, if the key points to a
	// directory the method will fail.
	// When recursive set to true, if the key points to a file,
	// the file will be deleted; if the key points to a directory,
	// then everything under the directory (including all child directories)
	// will be deleted.
	Delete(key string, recursive bool) (*etcdCli.Response, error)

	// If recursive is set to true the watch returns the first change under the
	// given prefix since the given index.
	// If recursive is set to false the watch returns the first change to the
	// given key since the given index.
	// To watch for the latest change, set waitIndex = 0.
	// If a receiver channel is given, it will be a long-term watch. Watch will
	// block at the channel. After someone receives the channel, it will go on
	// to watch that prefix. If a stop channel is given, the client can close
	// long-term watch using the stop channel.
	Watch(prefix string, waitIndex uint64, recursive bool,
		receiver chan *etcdCli.Response, stop chan bool) (*etcdCli.Response, error)
}

var etcdServer = flag.String("etcd-server", "http://127.0.0.1:4001",
	"Etcd service location")

func NewEtcdRegistry(servers []string) Registry {
	cfg := etcdCli.Config{
		Endpoints: servers,
		Transport: etcdCli.DefaultTransport,
	}
	c, err := etcdCli.New(cfg)
	if err != nil {
		panic(err)
	}
	return NewEtcdRegistryFromClient(c)
}

func NewEtcdRegistryFromClient(cli etcdCli.Client) Registry {

	api := etcdCli.NewKeysAPI(cli)
	return &etcdRegistry{
		client:  cli,
		keysApi: api,
	}
}

type etcdRegistry struct {
	client  etcdCli.Client
	keysApi etcdCli.KeysAPI
}

func (r *etcdRegistry) Get(key string, sort, recursive bool) (*etcdCli.Response, error) {
	resp, err := r.keysApi.Get(context.Background(), key, &etcdCli.GetOptions{Sort: sort, Recursive: recursive})

	return resp, err
}

func (r *etcdRegistry) Create(key string, value string, ttl time.Duration) (*etcdCli.Response, error) {
	opts := etcdCli.SetOptions{
		PrevExist: etcdCli.PrevNoExist,
		TTL:       ttl,
	}
	resp, err := r.keysApi.Set(context.Background(), key, value, &opts)
	fmt.Println(err)
	return resp, err
}

func (r *etcdRegistry) Delete(key string, recursive bool) (*etcdCli.Response, error) {
	return r.keysApi.Delete(context.Background(), key, &etcdCli.DeleteOptions{Recursive: recursive})
}

func (r *etcdRegistry) CompareAndSwap(key string, value string, ttl time.Duration, prevValue string,
	prevIndex uint64) (*etcdCli.Response, error) {
	opts := etcdCli.SetOptions{
		PrevValue: prevValue,
		PrevIndex: prevIndex,
		PrevExist: etcdCli.PrevExist,
	}
	return r.keysApi.Set(context.Background(), key, value, &opts)
}

func (r *etcdRegistry) Watch(prefix string, waitIndex uint64, recursive bool,
	receiver chan *etcdCli.Response, stop chan bool) (*etcdCli.Response, error) {
	opts := etcdCli.WatcherOptions{
		AfterIndex: waitIndex,
		Recursive:  recursive,
	}
	ctx, cancel := context.WithCancel(context.Background())
	//start context cancelling goroutine that listens for stop signal
	go func() {
		<-stop
		cancel()
	}()
	//create and use the watcher
	w := r.keysApi.Watcher(prefix, &opts)
	//no channel, once execution
	if receiver == nil {
		return w.Next(ctx)
	}
	//else loop and forward resps to channel
	defer close(receiver)
	for {
		resp, err := w.Next(ctx)
		if err != nil {
			return nil, err
		}
		receiver <- resp
	}
}

func (r *etcdRegistry) Set(key string, value string, ttl time.Duration) (*etcdCli.Response, error) {
	return r.keysApi.Set(context.Background(), key, value, &etcdCli.SetOptions{TTL: ttl, PrevExist: etcdCli.PrevIgnore})
}
