package viper_consul

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/spf13/viper"
)

type configS struct {
	Key   string
	Value string
}

func TestConsulConfigProvider(t *testing.T) {
	c, err := api.NewClient(api.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.KV().DeleteTree("", nil)
	if err != nil {
		t.Fatal(err)
	}
	config := &configS{
		Key:   "key",
		Value: "value",
	}
	b, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.KV().Put(&api.KVPair{
		Key:   "config.json",
		Value: b,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	viper.AddRemoteProvider("consul", "127.0.0.1:8500", "config.json")
	viper.SetConfigFile("config.json")
	err = viper.ReadRemoteConfig()
	if err != nil {
		t.Fatal(err)
	}

	newConfig := &configS{}
	err = viper.Unmarshal(newConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(newConfig)
}

func remoteProvider(path string) viper.RemoteProvider {
	return &defaultRemoteProvider{
		provider: "consul",
		endpoint: "127.0.0.1:8500",
		path:     path,
	}
}

func initConsul(t *testing.T, init bool) {
	c, err := api.NewClient(api.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.KV().DeleteTree("", nil)
	if err != nil {
		t.Fatal(err)
	}
	if init {
		_, err = c.KV().Put(&api.KVPair{
			Key:   "key1",
			Value: []byte("value"),
		}, nil)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestConsulConfigProvider_Get(t *testing.T) {
	initConsul(t, true)
	cp := newConsulConfigProvider()
	rp := remoteProvider("key1")
	r, err := cp.Get(rp)
	if err != nil {
		t.Fatal(err)
	}
	b, err := ioutil.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "value" {
		t.Errorf("Get key1, expect value1, got %v", string(b))
	}
}

func TestConsulConfigProvider_Watch(t *testing.T) {
	initConsul(t, false)
	cp := newConsulConfigProvider()
	rp := remoteProvider("key1")

	cp.Get(rp)

	c, err := api.NewClient(api.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.KV().Put(&api.KVPair{
		Key:   "key1",
		Value: []byte("valueb"),
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	r, err := cp.Watch(rp)
	if err != nil {
		t.Fatal(err)
	}
	if r == nil {
		t.Fatalf("watch return nil")
	}
	b, err := ioutil.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "valueb" {
		t.Errorf("Watch expect valueb, got %v", string(b))
	}
}

func TestConsulConfigProvider_WatchChannel(t *testing.T) {
	initConsul(t, true)
	cp := newConsulConfigProvider()
	rp := remoteProvider("key1")
	resp, quit := cp.WatchChannel(rp)

	size := 5
	go func() {
		c, err := api.NewClient(api.DefaultConfig())
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < size; i++ {
			_, err := c.KV().Put(&api.KVPair{
				Key:   "key1",
				Value: []byte(fmt.Sprintf("value%d", i)),
			}, nil)
			if err != nil {
				t.Fatal(err)
			}
		}
		time.Sleep(time.Second * 1)
		close(quit)
	}()

	var last []byte
	for r := range resp {
		if r.Error != nil {
			t.Fatal(r.Error)
		}
		last = r.Value
	}
	if string(last) != fmt.Sprintf("value%d", size-1) {
		t.Errorf("expect %v, got %v", fmt.Sprintf("value%d", size-1), string(last))
	}
}

type defaultRemoteProvider struct {
	provider string
	endpoint string
	path     string
}

func (rp defaultRemoteProvider) Provider() string {
	return rp.provider
}

func (rp defaultRemoteProvider) Endpoint() string {
	return rp.endpoint
}

func (rp defaultRemoteProvider) Path() string {
	return rp.path
}

func (rp defaultRemoteProvider) SecretKeyring() string {
	return ""
}
