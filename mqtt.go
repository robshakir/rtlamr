package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync/atomic"

	"github.com/bemasher/rtlamr/protocol"
	"github.com/mdzio/go-mqtt/message"
	"github.com/mdzio/go-mqtt/service"
)

const topic = "meters"

type MQTT struct {
	c  *service.Client
	id atomic.Uint32
}

func NewMQTT(addr string) (*MQTT, error) {
	log.Println("creating MQTT encoder")

	c := &service.Client{}
	cm := message.NewConnectMessage()
	cm.SetVersion(4)
	cm.SetCleanSession(true)
	cm.SetClientID([]byte("rtlamr-mqtt"))
	cm.SetKeepAlive(10)

	if err := c.Connect(addr, cm); err != nil {
		return nil, fmt.Errorf("cannot connect to MQTT server, %v", err)
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
	pm := message.NewPublishMessage()
	pm.SetPacketID(uint16(m.id.Load()))
	pm.SetTopic([]byte(topic))
	pm.SetPayload(js)

	if err := m.c.Publish(pm, nil); err != nil {
		return fmt.Errorf("cannot publish message, %v", err)
	}

	return nil
}

func (m *MQTT) Disconnect() {
	m.c.Disconnect()
}
