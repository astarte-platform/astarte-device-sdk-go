// Copyright © 2020 Ispirata Srl
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package device

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/astarte-platform/astarte-go/interfaces"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func (d *Device) getBaseTopic() string {
	return fmt.Sprintf("%s/%s", d.realm, d.deviceID)
}

func (d *Device) initializeMQTTClient(brokerAddress string) error {
	s := mqtt.NewFileStore(filepath.Join(d.persistencyDir, "mqttstore"))
	opts := mqtt.NewClientOptions()
	opts.AddBroker(brokerAddress)
	opts.SetAutoReconnect(d.AutoReconnect)
	opts.SetStore(s)
	opts.SetClientID(fmt.Sprintf("%s/%s", d.realm, d.deviceID))
	opts.SetConnectTimeout(30 * time.Second)

	tlsConfig, err := d.getTLSConfig()
	if err != nil {
		return err
	}
	opts.SetTLSConfig(tlsConfig)

	// This is our message handler
	opts.SetDefaultPublishHandler(func(client mqtt.Client, msg mqtt.Message) {
		if !strings.HasPrefix(msg.Topic(), d.getBaseTopic()) {
			if d.OnErrors != nil {
				d.OnErrors(d, fmt.Errorf("Received message on unexpected topic %s. This is an internal error", msg.Topic()))
			}
			return
		}

		// We split up to 4 since this will give us the path in the correct format.
		tokens := strings.SplitN(msg.Topic(), "/", 4)
		if len(tokens) > 2 {
			// Is it a control message?
			if tokens[2] == "control" {
				// TODO: Handle control messages
				return
			}

			// It's a data message. Grab the interface name.
			interfaceName := tokens[2]
			// Parse the payload
			parsed := map[string]interface{}{}
			if err := bson.Unmarshal(msg.Payload(), parsed); err != nil {
				if d.OnErrors != nil {
					d.OnErrors(d, err)
				}
				return
			}
			timestamp := time.Time{}
			if t, ok := parsed["t"]; ok {
				// We have a timestamp
				if pT, ok := t.(primitive.DateTime); ok {
					timestamp = pT.Time()
				}
			}

			if iface, ok := d.interfaces[interfaceName]; ok {
				// Is it individual?
				if iface.Aggregation == interfaces.IndividualAggregation && len(tokens) == 4 {
					interfacePath := "/" + tokens[3]

					// Create the message
					m := IndividualMessage{
						Interface: iface,
						Path:      interfacePath,
						Value:     parsed["v"],
						Timestamp: timestamp,
					}
					if d.OnIndividualMessageReceived != nil {
						d.OnIndividualMessageReceived(d, m)
					}
				}
			} else {
				// Something is off.
				if d.OnErrors != nil {
					d.OnErrors(d, fmt.Errorf("Received message for unregistered interface %s", interfaceName))
				}
				return
			}
		}
	})

	//	opts.SetOnConnectHandler()
	d.m = mqtt.NewClient(opts)

	return nil
}

// SendIndividualMessageWithTimestamp sends a new message towards an individual aggregation interface,
// with explicit timestamp
func (d *Device) SendIndividualMessageWithTimestamp(interfaceName, interfacePath string, value interface{}, timestamp time.Time) error {
	// Get the interface from the introspection
	iface, ok := d.interfaces[interfaceName]
	if ok {
		if iface.Aggregation != interfaces.IndividualAggregation {
			return fmt.Errorf("Interface %s hasn't individual aggregation", interfaceName)
		}
		// Validate the message
		if err := interfaces.ValidateIndividualMessage(iface, interfacePath, value); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("Interface %s not registered", interfaceName)
	}

	// We are good to go. Let's send the message.
	return d.sendMqttV1MessageInternal(iface, interfacePath, value, timestamp)
}

// SendIndividualMessage sends a new message towards an individual aggregation interface
func (d *Device) SendIndividualMessage(interfaceName, path string, value interface{}) error {
	return d.SendIndividualMessageWithTimestamp(interfaceName, path, value, time.Time{})
}

// SendAggregateMessageWithTimestamp sends a new message towards an Object Aggregated interface,
// with explicit timestamp. values must be a map containing the last tip of the endpoint, with no
// slash, as the key, and the corresponding value as value. interfacePath should match the path
// of the base endpoint, without the last tip.
// Example: if dealing with an aggregate interface with endpoints [/my/aggregate/firstValue, /my/aggregate/secondValue],
// interfacePath would be "/my/aggregate", and values would be map["firstValue": <value>, "secondValue": <value>]
func (d *Device) SendAggregateMessageWithTimestamp(interfaceName, interfacePath string, values map[string]interface{}, timestamp time.Time) error {
	// Get the interface from the introspection
	iface, ok := d.interfaces[interfaceName]
	if ok {
		if iface.Aggregation != interfaces.ObjectAggregation {
			return fmt.Errorf("Interface %s hasn't object aggregation", iface.Name)
		}
		// Validate the message
		if err := interfaces.ValidateAggregateMessage(iface, interfacePath, values); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("Interface %s not registered", interfaceName)
	}

	// We are good to go. Let's send the message.
	return d.sendMqttV1MessageInternal(iface, interfacePath, values, timestamp)
}

// SendAggregateMessage sends a new message towards an Object Aggregated interface.
// values must be a map containing the last tip of the endpoint, with no
// slash, as the key, and the corresponding value as value. interfacePath should match the path
// of the base endpoint, without the last tip.
// Example: if dealing with an aggregate interface with endpoints [/my/aggregate/firstValue, /my/aggregate/secondValue],
// interfacePath would be "/my/aggregate", and values would be map["firstValue": <value>, "secondValue": <value>]
func (d *Device) SendAggregateMessage(interfaceName, interfacePath string, values map[string]interface{}) error {
	return d.SendAggregateMessageWithTimestamp(interfaceName, interfacePath, values, time.Time{})
}

func (d *Device) sendMqttV1MessageInternal(astarteInterface interfaces.AstarteInterface, interfacePath string, values interface{}, timestamp time.Time) error {
	var qos uint8 = 2
	if astarteInterface.Type == interfaces.DatastreamType {
		// Guaranteed we won't get an error here
		mapping, _ := interfaces.InterfaceMappingFromPath(astarteInterface, interfacePath)
		switch mapping.Reliability {
		case interfaces.GuaranteedReliability:
			qos = 1
		case interfaces.UnreliableReliability:
			qos = 0
		}
	}

	payload := map[string]interface{}{"v": interfaces.NormalizePayload(values, false)}
	if !timestamp.IsZero() {
		payload["t"] = timestamp.UTC()
	}
	doc, err := bson.Marshal(payload)
	if err != nil {
		return err
	}

	topic := fmt.Sprintf("%s/%s%s", d.getBaseTopic(), astarteInterface.Name, interfacePath)
	// TODO: Handle this token
	_ = d.m.Publish(topic, qos, false, doc)

	return nil
}

func (d *Device) setupSubscriptions() error {
	subscriptions := map[string]byte{}
	for _, i := range d.interfaces {
		if i.Ownership == interfaces.ServerOwnership {
			if i.Aggregation == interfaces.ObjectAggregation {
				subscriptions[fmt.Sprintf("%s/%s", d.getBaseTopic(), i.Name)] = 2
			} else {
				subscriptions[fmt.Sprintf("%s/%s/#", d.getBaseTopic(), i.Name)] = 2
			}
		}
	}

	if len(subscriptions) > 0 {
		// Subscribe
		t := d.m.SubscribeMultiple(subscriptions, nil)
		if ok := t.WaitTimeout(10 * time.Second); !ok {
			return errors.New("Timed out while setting up subscriptions")
		}
		return t.Error()
	}

	// It's all good
	return nil
}

func (d *Device) sendIntrospection() error {
	// Set up the introspection
	introspection := ""
	for _, i := range d.interfaces {
		introspection = introspection + fmt.Sprintf("%s:%d:%d;", i.Name, i.MajorVersion, i.MinorVersion)
	}
	introspection = introspection[:len(introspection)-1]

	// Send it to the base topic
	t := d.m.Publish(d.getBaseTopic(), 2, false, introspection)
	if !t.WaitTimeout(5 * time.Second) {
		return errors.New("Timed out while sending introspection")
	}
	return t.Error()
}

func bsonRawValueToInterface(v bson.RawValue, valueType string) (interface{}, error) {
	switch valueType {
	case "string":
		var val string
		err := v.Unmarshal(val)
		return val, err
	case "double":
		var val float64
		err := v.Unmarshal(val)
		return val, err
	case "integer":
		var val int32
		err := v.Unmarshal(val)
		return val, err
	case "boolean":
		var val bool
		err := v.Unmarshal(val)
		return val, err
	case "longinteger":
		var val int64
		err := v.Unmarshal(val)
		return val, err
	case "binaryblob":
		var val []byte
		err := v.Unmarshal(val)
		return val, err
	case "datetime":
		// TODO: verify this is true.
		var val time.Time
		err := v.Unmarshal(val)
		return val, err
	case "stringarray":
		var val []string
		err := v.Unmarshal(val)
		return val, err
	case "doublearray":
		var val []float64
		err := v.Unmarshal(val)
		return val, err
	case "integerarray":
		var val []int32
		err := v.Unmarshal(val)
		return val, err
	case "booleanarray":
		var val []bool
		err := v.Unmarshal(val)
		return val, err
	case "longintegerarray":
		var val []int64
		err := v.Unmarshal(val)
		return val, err
	case "binaryblobarray":
		var val [][]byte
		err := v.Unmarshal(val)
		return val, err
	case "datetimearray":
		// TODO: verify this is true.
		var val []time.Time
		err := v.Unmarshal(val)
		return val, err
	}

	return nil, fmt.Errorf("Could not decode for type %s", valueType)
}
