package viper_consul

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"sync"

	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/api/watch"
	"github.com/spf13/viper"
)

var (
	logger = log.New(os.Stderr, "viper-consul", log.LstdFlags)
)

func init() {
	viper.RemoteConfig = newConsulConfigProvider()
}

func newConsulConfigProvider() *consulConfigProvider {
	return &consulConfigProvider{
		mu: &sync.Mutex{},
		m:  make(map[string]*api.Client),
	}
}

type consulConfigProvider struct {
	mu *sync.Mutex
	m  map[string]*api.Client
}

func (cp *consulConfigProvider) ensureClient(rp viper.RemoteProvider) (*api.Client, error) {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	var (
		c  *api.Client
		ok bool
	)

	c, ok = cp.m[rp.Endpoint()]
	if !ok {
		cfg := api.DefaultConfig()
		cfg.Address = rp.Endpoint()
		var err error
		c, err = api.NewClient(cfg)
		if err != nil {
			return nil, err
		}
		cp.m[rp.Endpoint()] = c
	}

	return c, nil
}

func (cp *consulConfigProvider) Get(rp viper.RemoteProvider) (io.Reader, error) {
	c, err := cp.ensureClient(rp)
	if err != nil {
		return nil, err
	}
	kv, _, err := c.KV().Get(rp.Path(), nil)
	if err != nil {
		return nil, err
	}
	if kv == nil {
		return nil, fmt.Errorf("Key ( %s ) was not found", rp.Path())
	}
	return bytes.NewReader(kv.Value), nil
}

func (cp *consulConfigProvider) Watch(rp viper.RemoteProvider) (io.Reader, error) {
	resp, quit := cp.WatchChannel(rp)
	v := <-resp
	close(quit)
	if v.Error != nil {
		return nil, v.Error
	}
	return bytes.NewReader(v.Value), nil
}

func (cp *consulConfigProvider) WatchChannel(rp viper.RemoteProvider) (<-chan *viper.RemoteResponse, chan bool) {
	var (
		c      *api.Client
		err    error
		params = map[string]interface{}{
			"type": "key",
			"key":  rp.Path(),
		}
		plan *watch.Plan
		resp = make(chan *viper.RemoteResponse, 1)
		quit = make(chan bool, 0)
	)

	c, err = cp.ensureClient(rp)
	if err != nil {
		// fail to connect
		resp <- &viper.RemoteResponse{Error: err}
		close(resp)
		return resp, quit
	}

	plan, err = watch.Parse(params)
	if err != nil {
		// dont't panic here
		resp <- &viper.RemoteResponse{Error: err}
		close(resp)
		return resp, quit
	}

	plan.Handler = func(idx uint64, raw interface{}) {
		var v *api.KVPair
		var vv []byte
		if raw == nil { // nil is a valid return value
			v = nil
			vv = nil
		} else {
			var ok bool
			if v, ok = raw.(*api.KVPair); !ok {
				return // ignore
			}
			vv = v.Value
		}
		select {
		case resp <- &viper.RemoteResponse{Value: vv}:
		case <-quit:
		}
	}
	go func() {
		<-quit
		plan.Stop()
	}()
	go func() {
		//{
		//	var (
		//		r   io.Reader
		//		err error
		//		b   []byte
		//	)
		//	r, err = cp.Get(rp)
		//	if err == nil && r != nil {
		//		b, err = ioutil.ReadAll(r)
		//	}
		//	resp <- &viper.RemoteResponse{
		//		Value: b,
		//		Error: err,
		//	}
		//	if err != nil {
		//		close(resp)
		//		return
		//	}
		//}
		if err := plan.RunWithClientAndLogger(c, logger); err != nil {
			logger.Printf("WatchChannel fail, %v", err)
		}
		close(resp)
	}()
	return resp, quit
}
