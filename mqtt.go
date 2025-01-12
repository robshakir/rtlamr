package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/bemasher/rtlamr/protocol"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

const (
	topic        = "meters"
	mqttClientID = "rtlamr-robjs"
	user         = "homeassistant"
	pwd          = "SECRET"
)

type MQTT struct {
	c  mqtt.Client
	id atomic.Uint32
}

func NewMQTT(addr string) (*MQTT, error) {
	log.Println("creating MQTT encoder")

	opts := mqtt.NewClientOptions().AddBroker(addr).SetClientID(mqttClientID)
	opts.SetKeepAlive(2 * time.Second)
	opts.SetPingTimeout(1 * time.Second)
	opts.SetUsername(user)
	opts.SetPassword(pwd)

	c := mqtt.NewClient(opts)
	if token := c.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	return &MQTT{c: c}, nil
}

func (m *MQTT) Q(i protocol.Message) error {
	log.Printf("received message %v (type %T)", i, i)

	js, err := json.Marshal(i)
	if err != nil {
		return fmt.Errorf("cannot marshal message to JSON, msg: %v, err: %v", i, err)
	}

	if err := m.q(js); err != nil {
		return err
	}

	return nil
}

func (m *MQTT) q(js []byte) error {
	defer m.id.Add(1)

	log.Printf("sending %s to MQTT", js)

	m.c.Publish(topic, 0, false, js).Wait()

	return nil
}

func (m *MQTT) Disconnect() {
	m.c.Disconnect(100)
}
