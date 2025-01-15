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
	mqttClientID = "rtlamr-robjs"
	user         = "homeassistant"
	pwd          = "SECRET"
)

type MQTT struct {
	c           mqtt.Client
	id          atomic.Uint32
	knownMeters map[uint32]struct{} // Stores a map of known meters to know whether we should create them in HomeAssistant.
}

func NewMQTT(addr string) (*MQTT, error) {
	log.Println("creating MQTT encoder")

	opts := mqtt.NewClientOptions().AddBroker(addr).SetClientID(mqttClientID)
	opts.SetKeepAlive(2 * time.Second)
	opts.SetPingTimeout(1 * time.Second)
	opts.SetUsername(user)
	opts.SetPassword(pwd)
	opts.SetAutoReconnect(true)

	c := mqtt.NewClient(opts)
	if token := c.Connect(); token.Wait() && token.Error() != nil {
		return nil, token.Error()
	}
	log.Println("created MQTT encoder")

	return &MQTT{c: c, knownMeters: map[uint32]struct{}{}}, nil
}

func (m *MQTT) Q(i protocol.Message) error {
	log.Printf("received message %v (type %T)", i, i)

	js, err := json.Marshal(i)
	if err != nil {
		return fmt.Errorf("cannot marshal message to JSON, msg: %v, err: %v", i, err)
	}

	if _, ok := m.knownMeters[i.MeterID()]; !ok {
		// This is a new meter that we didn't know about before, so we need to send
		// a HomeAssistant discovery topic message.
		dT, d, err := haDeviceJSON(i)
		if err != nil {
			return err
		}
		if err := m.q(dT, d); err != nil {
			return err
		}
		m.knownMeters[i.MeterID()] = struct{}{}
	}

	if err := m.q(fmt.Sprintf("meters/%d/state", i.MeterID()), js); err != nil {
		return err
	}

	return nil
}

func (m *MQTT) q(topic string, js []byte) error {
	defer m.id.Add(1)

	log.Printf("sending %s to MQTT", js)
	m.c.Publish(topic, 0, false, js).Wait()

	return nil
}

func (m *MQTT) Disconnect() {
	m.c.Disconnect(100)
}

func haDeviceJSON(i protocol.Message) (string, []byte, error) {
	var devClass, devUnit string
	switch i.MsgType() {
	case "R900":
		devClass = "water"
		devUnit = "gal"
	default:
		devClass = "energy"
		devUnit = "kwH"
	}

	d := &HomeAssistantDiscovery{
		Device: &HomeAssistantDevice{
			ID:   fmt.Sprintf("%d", i.MeterID()),
			Name: fmt.Sprintf("%d Meter (%s)", i.MeterID(), i.MsgType()),
		},
		Origin: &HomeAssistantOrigin{
			Name:     "rtlamr_mqtt",
			Software: "v0.01",
			URL:      "https://github.com/robshakir/rtlamr",
		},
		Components: map[string]*HomeAssistantComponent{
			"meter_0": {
				Platform:    "sensor",
				DeviceClass: devClass,
				Unit:        devUnit,
				ValTemplate: "{{ value_json.Message.Consumption }}",
			},
		},
	}

	js, err := json.Marshal(d)
	if err != nil {
		return "", nil, fmt.Errorf("cannot marshal discovery JSON, %v", err)
	}

	return fmt.Sprintf("homeassistant/device/%d/config", i.MeterID()), js, nil
}

type HomeAssistantDiscovery struct {
	Device     *HomeAssistantDevice               `json:"dev,omitempty"`
	Origin     *HomeAssistantOrigin               `json:"o,omitempty"`
	Components map[string]*HomeAssistantComponent `json:"cmps,omitempty"`
	StateTopic string                             `json:"state_topic,omitempty"`
	QOS        int                                `json:"qos,omitempty"`
}

type HomeAssistantDevice struct {
	ID   string `json:"ids,omitempty"`
	Name string `json:"name,omitempty"`
}

type HomeAssistantOrigin struct {
	Name     string `json:"name,omitempty"`
	Software string `json:"sw,omitempty"`
	URL      string `json:"url,omitempty"`
}

type HomeAssistantComponent struct {
	Platform    string `json:"p,omitempty"`
	DeviceClass string `json:"device_class,omitempty"`
	Unit        string `json:"unit_of_measurement,omitempty"`
	ValTemplate string `json:"value_template,omitempty"`
	UniqueID    string `json:"unique_id,omitempty"`
}
