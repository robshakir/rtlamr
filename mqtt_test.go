package main

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/bemasher/rtlamr/protocol"
	"github.com/bemasher/rtlamr/r900"
	"github.com/bemasher/rtlamr/scm"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/mdzio/go-mqtt/message"
	"github.com/mdzio/go-mqtt/service"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"
)

func newServer(t *testing.T, addr string) (*mqtt.Server, chan error) {
	errCh := make(chan error)

	// Create the new MQTT Server.
	server := mqtt.New(nil)

	// Allow all connections.
	_ = server.AddHook(new(auth.AllowHook), nil)

	// Create a TCP listener on a standard port.
	tcp := listeners.NewTCP(listeners.Config{ID: "t1", Address: addr})
	err := server.AddListener(tcp)
	if err != nil {
		t.Fatalf("cannot listen, %v", err)
	}

	go func() {
		err := server.Serve()
		if err != nil {
			errCh <- err
		}
	}()

	return server, errCh
}

func TestQ(t *testing.T) {
	tests := []struct {
		desc              string
		inMsgs            []protocol.Message
		wantMQTT          [][]byte
		wantMeters        map[uint32]struct{}
		wantDiscoveryMsgs [][]byte
	}{{
		desc: "SCM",
		inMsgs: []protocol.Message{
			scm.SCM{
				ID:          42,
				Type:        1,
				TamperPhy:   1,
				TamperEnc:   1,
				Consumption: 42,
				ChecksumVal: 1,
			},
		},
		wantMQTT: [][]byte{
			func() []byte {
				js, err := json.Marshal(scm.SCM{
					ID:          42,
					Type:        1,
					TamperPhy:   1,
					TamperEnc:   1,
					Consumption: 42,
					ChecksumVal: 1,
				})
				if err != nil {
					t.Fatalf("cannot build example, %v", err)
				}
				return js
			}(),
		},
		wantMeters: map[uint32]struct{}{
			42: {},
		},
		wantDiscoveryMsgs: [][]byte{
			func() []byte {

				d := &HomeAssistantDiscovery{
					StateTopic: "meters/42/state",
					QOS:        2,
					Device: &HomeAssistantDevice{
						ID:   "42",
						Name: "42 Meter (SCM)",
					},
					Origin: &HomeAssistantOrigin{
						Name:     "rtlamr_mqtt",
						Software: "v0.01",
						URL:      "https://github.com/robshakir/rtlamr",
					},
					Components: map[string]*HomeAssistantComponent{
						"meter_0": {
							Platform:    "sensor",
							DeviceClass: "volume",
							Unit:        "ft³",
							ValTemplate: "{{ value_json.Consumption }}",
							UniqueID:    "meter42_volume",
						},
					},
				}
				js, err := json.Marshal(d)
				if err != nil {
					t.Fatalf("cannot create test data, %v", err)
				}
				return js
			}(),
		},
	}, {
		desc: "SCM",
		inMsgs: []protocol.Message{
			scm.SCM{
				ID:          42,
				Type:        1,
				TamperPhy:   1,
				TamperEnc:   1,
				Consumption: 42,
				ChecksumVal: 1,
			},
		},
		wantMQTT: [][]byte{
			func() []byte {
				js, err := json.Marshal(scm.SCM{
					ID:          42,
					Type:        1,
					TamperPhy:   1,
					TamperEnc:   1,
					Consumption: 42,
					ChecksumVal: 1,
				})
				if err != nil {
					t.Fatalf("cannot build example, %v", err)
				}
				return js
			}(),
		},
		wantMeters: map[uint32]struct{}{
			42: {},
		},
		wantDiscoveryMsgs: [][]byte{
			func() []byte {

				d := &HomeAssistantDiscovery{
					StateTopic: "meters/42/state",
					QOS:        2,
					Device: &HomeAssistantDevice{
						ID:   "42",
						Name: "42 Meter (SCM)",
					},
					Origin: &HomeAssistantOrigin{
						Name:     "rtlamr_mqtt",
						Software: "v0.01",
						URL:      "https://github.com/robshakir/rtlamr",
					},
					Components: map[string]*HomeAssistantComponent{
						"meter_0": {
							Platform:    "sensor",
							DeviceClass: "volume",
							Unit:        "ft³",
							ValTemplate: "{{ value_json.Consumption }}",
							UniqueID:    "meter42_volume",
						},
					},
				}
				js, err := json.Marshal(d)
				if err != nil {
					t.Fatalf("cannot create test data, %v", err)
				}
				return js
			}(),
		},
	}, {
		desc: "R900",
		inMsgs: []protocol.Message{
			r900.R900{
				ID:          42,
				Unkn1:       1,
				NoUse:       1,
				BackFlow:    2,
				Consumption: 33,
				Unkn3:       1,
				Leak:        1,
				LeakNow:     1,
			},
		},
		wantMQTT: [][]byte{
			func() []byte {
				js, err := json.Marshal(r900.R900{
					ID:          42,
					Unkn1:       1,
					NoUse:       1,
					BackFlow:    2,
					Consumption: 33,
					Unkn3:       1,
					Leak:        1,
					LeakNow:     1,
				})
				if err != nil {
					t.Fatalf("cannot build example, %v", err)
				}
				return js
			}(),
		},
		wantMeters: map[uint32]struct{}{
			42: {},
		},

		wantDiscoveryMsgs: [][]byte{
			func() []byte {
				d := &HomeAssistantDiscovery{
					StateTopic: "meters/42/state",
					QOS:        2,
					Device: &HomeAssistantDevice{
						ID:   "42",
						Name: "42 Meter (R900)",
					},
					Origin: &HomeAssistantOrigin{
						Name:     "rtlamr_mqtt",
						Software: "v0.01",
						URL:      "https://github.com/robshakir/rtlamr",
					},
					Components: map[string]*HomeAssistantComponent{
						"meter_0": {
							Platform:    "sensor",
							DeviceClass: "water",
							Unit:        "gal",
							ValTemplate: "{{ value_json.Consumption }}",
							UniqueID:    "meter42_water",
						},
					},
				}
				js, err := json.Marshal(d)
				if err != nil {
					t.Fatalf("cannot create test data, %v", err)
				}
				return js
			}(),
		},
	}, {
		desc: "two messages",
		inMsgs: []protocol.Message{
			r900.R900{
				ID:          42,
				Unkn1:       1,
				NoUse:       1,
				BackFlow:    2,
				Consumption: 33,
				Unkn3:       1,
				Leak:        1,
				LeakNow:     1,
			},
			r900.R900{
				ID:          43,
				Unkn1:       1,
				NoUse:       1,
				BackFlow:    2,
				Consumption: 33,
				Unkn3:       1,
				Leak:        1,
				LeakNow:     1,
			},
		},
		wantMQTT: [][]byte{
			func() []byte {
				js, err := json.Marshal(r900.R900{
					ID:          42,
					Unkn1:       1,
					NoUse:       1,
					BackFlow:    2,
					Consumption: 33,
					Unkn3:       1,
					Leak:        1,
					LeakNow:     1,
				})
				if err != nil {
					t.Fatalf("cannot build example, %v", err)
				}
				return js
			}(),
			func() []byte {
				js, err := json.Marshal(r900.R900{
					ID:          43,
					Unkn1:       1,
					NoUse:       1,
					BackFlow:    2,
					Consumption: 33,
					Unkn3:       1,
					Leak:        1,
					LeakNow:     1,
				})
				if err != nil {
					t.Fatalf("cannot build example, %v", err)
				}
				return js
			}(),
		},
		wantMeters: map[uint32]struct{}{
			42: {},
			43: {},
		},
		wantDiscoveryMsgs: [][]byte{
			func() []byte {
				d := &HomeAssistantDiscovery{
					StateTopic: "meters/42/state",
					QOS:        2,
					Device: &HomeAssistantDevice{
						ID:   "42",
						Name: "42 Meter (R900)",
					},
					Origin: &HomeAssistantOrigin{
						Name:     "rtlamr_mqtt",
						Software: "v0.01",
						URL:      "https://github.com/robshakir/rtlamr",
					},
					Components: map[string]*HomeAssistantComponent{
						"meter_0": {
							Platform:    "sensor",
							DeviceClass: "water",
							Unit:        "gal",
							ValTemplate: "{{ value_json.Consumption }}",
							UniqueID:    "meter42_water",
						},
					},
				}
				js, err := json.Marshal(d)
				if err != nil {
					t.Fatalf("cannot create test data, %v", err)
				}
				return js
			}(),
			func() []byte {
				d := &HomeAssistantDiscovery{
					StateTopic: "meters/43/state",
					QOS:        2,
					Device: &HomeAssistantDevice{
						ID:   "43",
						Name: "43 Meter (R900)",
					},
					Origin: &HomeAssistantOrigin{
						Name:     "rtlamr_mqtt",
						Software: "v0.01",
						URL:      "https://github.com/robshakir/rtlamr",
					},
					Components: map[string]*HomeAssistantComponent{
						"meter_0": {
							Platform:    "sensor",
							DeviceClass: "water",
							Unit:        "gal",
							ValTemplate: "{{ value_json.Consumption }}",
							UniqueID:    "meter43_water",
						},
					},
				}
				js, err := json.Marshal(d)
				if err != nil {
					t.Fatalf("cannot create test data, %v", err)
				}
				return js
			}(),
		},
	}}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			srv, srvErrCh := newServer(t, ":1833")
			defer srv.Close()

			var gotMsgs [][]byte
			recvErrCh := make(chan error)
			doneCh := make(chan struct{})

			go func() {
				c := &service.Client{}
				cm := message.NewConnectMessage()
				cm.SetVersion(4)
				cm.SetCleanSession(true)
				cm.SetClientID([]byte("rtlamr-robjs-tester"))
				cm.SetKeepAlive(10)
				if err := c.Connect("tcp://localhost:1833", cm); err != nil {
					recvErrCh <- fmt.Errorf("cannot connect to MQTT server, %v", err)
					return
				}

				for m := range tt.wantMeters {
					sm := message.NewSubscribeMessage()
					topic := fmt.Sprintf("meters/%d/state", m)
					sm.AddTopic([]byte(topic), 0)
					if err := c.Subscribe(sm, nil, func(msg *message.PublishMessage) error {
						fmt.Printf("Topic: %s, got message: %v\n", topic, msg)
						gotMsgs = append(gotMsgs, msg.Payload())
						if len(gotMsgs) >= len(tt.wantMQTT) {
							doneCh <- struct{}{}
						}
						return nil
					}); err != nil {
						recvErrCh <- fmt.Errorf("cannot receive message from MQTT, %v", err)
						return
					}
				}
			}()

			discoveryMessages := [][]byte{}
			go func() {
				c := &service.Client{}
				cm := message.NewConnectMessage()
				cm.SetVersion(4)
				cm.SetCleanSession(true)
				cm.SetClientID([]byte("rtlamr-robjs-tester-discovry"))
				cm.SetKeepAlive(10)
				if err := c.Connect("tcp://localhost:1833", cm); err != nil {
					recvErrCh <- fmt.Errorf("cannot connect to MQTT server, %v", err)
					return
				}

				for m := range tt.wantMeters {
					sm := message.NewSubscribeMessage()
					topic := fmt.Sprintf("homeassistant/device/%d/config", m)
					sm.AddTopic([]byte(topic), 0)
					if err := c.Subscribe(sm, nil, func(msg *message.PublishMessage) error {
						fmt.Printf("topic: %s, got message: %v\n", topic, msg)
						discoveryMessages = append(discoveryMessages, msg.Payload())
						return nil
					}); err != nil {
						recvErrCh <- fmt.Errorf("cannot receive message from MQTT, %v", err)
						return
					}
				}
			}()

			time.Sleep(100 * time.Millisecond)

			c, err := NewMQTT("tcp://localhost:1833")
			if err != nil {
				t.Fatalf("cannot connect to MQTT server, got: %v, want: nil", err)
			}
			defer c.Disconnect()

			t.Log("enqueueing message")
			for _, m := range tt.inMsgs {
				if err := c.Q(m); err != nil {
					t.Fatalf("cannot publish message, got: %v, want: nil", err)
				}
			}

			select {
			case err := <-recvErrCh:
				t.Fatalf("got receiver error: %v", err)
			case err := <-srvErrCh:
				t.Fatalf("got server error: %v", err)
			case <-doneCh:
			}

			if diff := cmp.Diff(gotMsgs, tt.wantMQTT, cmpopts.SortSlices(func(a, b []byte) bool { return string(a) < string(b) })); diff != "" {
				t.Errorf("did not get expected messages, diff(-got,+want):\n%s", diff)
			}

			if diff := cmp.Diff(c.knownMeters, tt.wantMeters); diff != "" {
				t.Errorf("did not get expected meters, diff(-got,+want):\n%s", diff)
			}

			if diff := cmp.Diff(discoveryMessages, tt.wantDiscoveryMsgs); diff != "" {
				t.Errorf("did not get expected discovery errors, diff(-got,+want):\n%s", diff)
			}
		})
	}
}

func TestHADeviceJSON(t *testing.T) {
	tests := []struct {
		desc        string
		in          protocol.Message
		wantTopic   string
		wantPayload []byte
		wantErr     bool
	}{{
		desc: "r900 meter",
		in: r900.R900{
			ID:          42,
			Unkn1:       1,
			NoUse:       1,
			BackFlow:    2,
			Consumption: 33,
			Unkn3:       1,
			Leak:        1,
			LeakNow:     1,
		},
		wantTopic: "homeassistant/device/42/config",
		wantPayload: func() []byte {
			d := &HomeAssistantDiscovery{
				StateTopic: "meters/42/state",
				QOS:        2,
				Device: &HomeAssistantDevice{
					ID:   "42",
					Name: "42 Meter (R900)",
				},
				Origin: &HomeAssistantOrigin{
					Name:     "rtlamr_mqtt",
					Software: "v0.01",
					URL:      "https://github.com/robshakir/rtlamr",
				},
				Components: map[string]*HomeAssistantComponent{
					"meter_0": {
						Platform:    "sensor",
						DeviceClass: "water",
						Unit:        "gal",
						ValTemplate: "{{ value_json.Consumption }}",
						UniqueID:    "meter42_water",
					},
				},
			}
			js, err := json.Marshal(d)
			if err != nil {
				t.Fatalf("cannot create test data, %v", err)
			}
			return js
		}(),
	}, {
		desc: "SCM meter",
		in: scm.SCM{
			ID:          42,
			Type:        1,
			TamperPhy:   1,
			TamperEnc:   1,
			Consumption: 42,
			ChecksumVal: 1,
		},
		wantTopic: "homeassistant/device/42/config",
		wantPayload: func() []byte {
			d := &HomeAssistantDiscovery{
				StateTopic: "meters/42/state",
				QOS:        2,
				Device: &HomeAssistantDevice{
					ID:   "42",
					Name: "42 Meter (SCM)",
				},
				Origin: &HomeAssistantOrigin{
					Name:     "rtlamr_mqtt",
					Software: "v0.01",
					URL:      "https://github.com/robshakir/rtlamr",
				},
				Components: map[string]*HomeAssistantComponent{
					"meter_0": {
						Platform:    "sensor",
						DeviceClass: "volume",
						Unit:        "ft³",
						ValTemplate: "{{ value_json.Consumption }}",
						UniqueID:    "meter42_volume",
					},
				},
			}
			js, err := json.Marshal(d)
			if err != nil {
				t.Fatalf("cannot create test data, %v", err)
			}
			return js
		}(),
	}}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			gotTopic, gotPayload, gotErr := haDeviceJSON(tt.in)
			if (gotErr != nil) != tt.wantErr {
				t.Fatalf("did not get expected error, got: %v, wantErr? %v", gotErr, tt.wantErr)
			}
			if gotTopic != tt.wantTopic {
				t.Errorf("did not get expected topic, got: %s, want: %s", gotTopic, tt.wantTopic)
			}

			if diff := cmp.Diff(gotPayload, tt.wantPayload); diff != "" {
				t.Errorf("did not get expected payload, diff(-got,+want):\n%s", diff)
			}
		})
	}
}
