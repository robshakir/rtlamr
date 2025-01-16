package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/bemasher/rtlamr/protocol"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

const (
	// mqttClientID is the identifier used for this client when connecting to
	// the MQTT broker.
	mqttClientID = "rtlamr-robjs"
	// user is the username to be used to connect to mqtt. It is usually "homeassistant".
	user = "homeassistant"
	// pwd is the plaintext password used to connect to the mqtt broker. It can be found
	// in plaintext in the yaml homeassistant config.
	pwd = "SECRET"
)

// MQTT is a wrapper for a client connecting to HomeAssistant's MQTT broker.
type MQTT struct {
	c mqtt.Client
	// knownMeters stores the set of meter IDs that the client is monitoring.
	// This is used to know whether we should create a meter via discovery in
	// HomeAssistant as the first message is received.
	knownMeters map[uint32][]*spec
}

// NewMQTT returns a new HomeAssistant MQTT client, connected to an external
// MQTT broker.
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

	return &MQTT{c: c, knownMeters: map[uint32][]*spec{}}, nil
}

// Q enqueues a protocol message received from RTLAMR to the relevant MQTT topic.
// If the meter isn't a known meter, it sends a discovery message to ensure that
// HomeAssistant knows about the meter and has the relevant state topic as the
// place to monitor it.
func (m *MQTT) Q(i protocol.Message) error {
	log.Printf("received message %v (type %T)", i, i)

	js, err := json.Marshal(i)
	if err != nil {
		return fmt.Errorf("cannot marshal message to JSON, msg: %v, err: %v", i, err)
	}

	if _, ok := m.knownMeters[i.MeterID()]; !ok {
		// This is a new meter that we didn't know about before, so we need to send
		// a HomeAssistant discovery topic message.
		dT, d, tspec, err := haDeviceJSON(i)
		if err != nil {
			return err
		}
		// Synchronously send here, since we want HA to discover the meter
		// before we send our readings.
		m.q(dT, d)
		m.knownMeters[i.MeterID()] = tspec
	}

	pJS := map[string]any{}
	if err := json.Unmarshal(js, &pJS); err != nil {
		return fmt.Errorf("cannot unmarshal JSON, sending unmodified, %v", err)
	}

	for _, s := range m.knownMeters[i.MeterID()] {
		fk, ok := pJS[s.OriginJSONKey]
		if !ok {
			log.Printf("cannot transform key %s, not found in %s", s.NewJSONKey, js)
			continue
		}
		v, ok := fk.(float64)
		if !ok {
			log.Printf("cannot transform key %s, not an integer, was %T in %s", s.NewJSONKey, fk, js)
			continue
		}
		pJS[s.NewJSONKey] = s.TransformFn(v)
	}
	nJS, err := json.Marshal(pJS)
	switch {
	case err != nil:
		log.Printf("cannot marshal transformed JSON, %v", err)
	default:
		js = nJS
	}

	go m.q(fmt.Sprintf("meters/%d/state", i.MeterID()), js)

	return nil
}

// q enqueues the JSON message js to the specified MQTT topic. It does
// not return an error so that it can be fired into a new goroutine.
func (m *MQTT) q(topic string, js []byte) {
	log.Printf("sending %s to MQTT", js)
	m.c.Publish(topic, 0, false, js).Wait()
}

// Disconnect disconnects
func (m *MQTT) Disconnect() {
	m.c.Disconnect(100)
}

type spec struct {
	OriginJSONKey string
	NewJSONKey    string
	TransformFn   func(float64) float64
}

// haDeviceJSON produces JSON required to create a new device in HomeAssistant. It returns
// a string that is the topic that should be written to, a byte slice of the JSON that is
// to be used, and a map of JSON key name to a function to transform that value.
func haDeviceJSON(i protocol.Message) (string, []byte, []*spec, error) {
	extraCmps := map[string]*HomeAssistantComponent{}
	specs := []*spec{}
	var devClass, devUnit string
	switch i.MsgType() {
	case "R900":
		devClass = "water"
		devUnit = "gal"

		extraCmps[fmt.Sprintf("meter%d_leak_status", i.MeterID())] = &HomeAssistantComponent{
			Platform:    "binary_sensor",
			DeviceClass: "water",
			ValTemplate: "{{ value_json.LeakNow }}",
			UniqueID:    fmt.Sprintf("meter%d_leaknow", i.MeterID()),
		}

		extraCmps[fmt.Sprintf("meter%d_leak_count", i.MeterID())] = &HomeAssistantComponent{
			Platform:    "sensor",
			DeviceClass: "water",
			ValTemplate: "{{ value_json.Leak }}",
			UniqueID:    fmt.Sprintf("meter%d_leak_count", i.MeterID()),
		}

		specs = append(specs, &spec{
			OriginJSONKey: "Consumption",
			NewJSONKey:    "Consumption",
			TransformFn: func(i float64) float64 {
				// Normalise to being in gal, rather than 0.1gal.
				return float64(i) / 10.0
			},
		})
	default:
		devClass = "gas"
		devUnit = "ft³"

		kwhID := fmt.Sprintf("meter%d_kwh", i.MeterID())
		extraCmps["meter_kwh"] = &HomeAssistantComponent{
			Platform:    "sensor",
			DeviceClass: "energy",
			Unit:        "kWh",
			ValTemplate: "{{ value_json.ConsumptionKWH }}",
			UniqueID:    kwhID,
		}

		specs = append(specs, &spec{
			OriginJSONKey: "Consumption",
			NewJSONKey:    "ConsumptionKWH",
			TransformFn: func(i float64) float64 {
				fmt.Printf("transformer was called\n")
				return float64(i) * 0.913422 * 1.062033 * 29.3001
			},
		})
	}

	cmps := map[string]*HomeAssistantComponent{
		"meter_0": {
			Platform:    "sensor",
			DeviceClass: devClass,
			Unit:        devUnit,
			ValTemplate: "{{ value_json.Consumption }}",
			UniqueID:    fmt.Sprintf("meter%d_%s", i.MeterID(), devClass),
		},
	}
	for k, v := range extraCmps {
		cmps[k] = v
	}

	d := &HomeAssistantDiscovery{
		StateTopic: fmt.Sprintf("meters/%d/state", i.MeterID()),
		QOS:        2,
		Device: &HomeAssistantDevice{
			ID:   fmt.Sprintf("%d", i.MeterID()),
			Name: fmt.Sprintf("%d Meter (%s)", i.MeterID(), i.MsgType()),
		},
		Origin: &HomeAssistantOrigin{
			Name:     "rtlamr_mqtt",
			Software: "v0.01",
			URL:      "https://github.com/robshakir/rtlamr",
		},
		Components: cmps,
	}

	js, err := json.MarshalIndent(d, "", "  ")
	if err != nil {
		return "", nil, nil, fmt.Errorf("cannot marshal discovery JSON, %v", err)
	}

	return fmt.Sprintf("homeassistant/device/%d/config", i.MeterID()), js, specs, nil
}

// HomeAssistantDiscovery describes the root message of a message that is sent
// to HomeAssistant to discover a new device. Some documentation is available
// at https://www.home-assistant.io/integrations/mqtt/#mqtt-discovery
type HomeAssistantDiscovery struct {
	Device     *HomeAssistantDevice               `json:"dev,omitempty"`
	Origin     *HomeAssistantOrigin               `json:"o,omitempty"`
	Components map[string]*HomeAssistantComponent `json:"cmps,omitempty"`
	StateTopic string                             `json:"state_topic,omitempty"`
	QOS        int                                `json:"qos,omitempty"`
}

// HomeAssistantDevice describes a device to HomeAssistant.
type HomeAssistantDevice struct {
	ID   string `json:"ids,omitempty"`
	Name string `json:"name,omitempty"`
}

// HomeAssistantOrigin describes the origin of data to HomeAssistant.
type HomeAssistantOrigin struct {
	Name     string `json:"name,omitempty"`
	Software string `json:"sw,omitempty"`
	URL      string `json:"url,omitempty"`
}

// HomeAssistantComponent describes a component within the HomeAssistant device.
type HomeAssistantComponent struct {
	Platform    string `json:"p,omitempty"`
	DeviceClass string `json:"device_class,omitempty"`
	Unit        string `json:"unit_of_measurement,omitempty"`
	ValTemplate string `json:"value_template,omitempty"`
	UniqueID    string `json:"unique_id,omitempty"`
}
