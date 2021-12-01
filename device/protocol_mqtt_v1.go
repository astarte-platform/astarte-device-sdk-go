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
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"reflect"
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

func (d *Device) initializeMQTTClient() error {
	s := mqtt.NewFileStore(filepath.Join(d.persistencyDir, "mqttstore"))
	opts := mqtt.NewClientOptions()
	opts.AddBroker(d.brokerURL)
	opts.SetAutoReconnect(d.AutoReconnect)
	opts.SetStore(s)
	opts.SetClientID(fmt.Sprintf("%s/%s", d.realm, d.deviceID))
	opts.SetConnectTimeout(30 * time.Second)
	opts.SetCleanSession(false)

	tlsConfig, err := d.getTLSConfig()
	if err != nil {
		return err
	}
	opts.SetTLSConfig(tlsConfig)

	opts.SetOnConnectHandler(func(client mqtt.Client) {
		astarteOnConnectHandler(d, client)
	})

	opts.SetConnectionLostHandler(func(client mqtt.Client, err error) {
		if d.OnErrors != nil {
			d.OnErrors(d, err)
		}

		if d.OnConnectionStateChanged != nil {
			d.OnConnectionStateChanged(d, false)
		}
	})

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
				err := d.handleControlMessages(strings.Join(tokens[3:], "/"), msg.Payload())
				if err != nil {
					d.OnErrors(d, err)
				}
				return
			}

			// It's a data message. Grab the interface name.
			interfaceName := tokens[2]
			// Parse the payload
			parsed, err := parseBSONPayload(msg.Payload())
			if err != nil {
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
				switch {
				case len(tokens) != 4:
					if d.OnErrors != nil {
						d.OnErrors(d, fmt.Errorf("could not parse incoming message on topic structure %s", tokens))
					}
					return
				case iface.Aggregation == interfaces.IndividualAggregation:
					interfacePath := "/" + tokens[3]

					d.storeProperty(iface.Name, interfacePath, iface.MajorVersion, msg.Payload())

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
				case iface.Aggregation == interfaces.ObjectAggregation:
					interfacePath := "/" + tokens[3]

					if val, ok := parsed["v"].(map[string]interface{}); !ok {
						d.OnErrors(d, fmt.Errorf("could not parse aggregate message payload"))
					} else {
						// We have to check whether we have some nested arrays or not in here.
						for k, v := range val {
							if bsonArray, ok := v.(primitive.A); ok {
								// That is, in fact, the case. Convert to a generic Go slice first.
								bsonArraySlice := []interface{}(bsonArray)
								// Now reflect the heck out of it and specialize the slice
								specializedSlice := reflect.MakeSlice(reflect.SliceOf(reflect.TypeOf(bsonArraySlice[0])), len(bsonArraySlice), cap(bsonArraySlice))
								for i := 0; i < specializedSlice.Len(); i++ {
									specializedSlice.Index(i).Set(reflect.ValueOf(bsonArraySlice[i]))
								}
								val[k] = specializedSlice.Interface()
							}
						}

						// N.B.: properties with object aggregation are not yet supported by Astarte
						d.storeProperty(iface.Name, interfacePath, iface.MajorVersion, msg.Payload())

						// Create the message
						m := AggregateMessage{
							Interface: iface,
							Path:      interfacePath,
							Values:    val,
							Timestamp: timestamp,
						}

						if d.OnAggregateMessageReceived != nil {
							d.OnAggregateMessageReceived(d, m)
						}
					}
				}
			} else if d.OnErrors != nil {
				// Something is off.
				d.OnErrors(d, fmt.Errorf("Received message for unregistered interface %s", interfaceName))
			}
		}
	})

	d.m = mqtt.NewClient(opts)

	return nil
}

func (d *Device) handleControlMessages(message string, payload []byte) error {
	switch message {
	case "consumer/properties":
		return d.handlePurgeProperties(payload)
	}

	// Not handled
	return nil
}

func astarteOnConnectHandler(d *Device, client mqtt.Client) {
	// Should we run the whole Astarte after connect thing?
	if !d.sessionPresent {
		// Yes, we should: first, setup subscription
		if err := d.setupSubscriptions(); err != nil {
			errorMsg := fmt.Sprintf("Cannot setup subscriptions: %v", err)
			if d.OnErrors != nil {
				d.OnErrors(d, errors.New(errorMsg))
			}
			fmt.Println(errorMsg)
			// If we failed to execute this action it means that we got disconnected,
			// the reconnection mechanism will take care of that so we just return
			return
		}
		// Then introspection
		if err := d.sendIntrospection(); err != nil {
			errorMsg := fmt.Sprintf("Cannot send introspection: %v", err)
			if d.OnErrors != nil {

				d.OnErrors(d, errors.New(errorMsg))
			}
			fmt.Println(errorMsg)
			// If we failed to execute this action it means that we got disconnected,
			// the reconnection mechanism will take care of that so we just return
			return
		}
		// Empty cache and
		if err := d.sendEmptyCache(); err != nil {
			errorMsg := fmt.Sprintf("Cannot send empty cache: %v", err)
			if d.OnErrors != nil {
				d.OnErrors(d, errors.New(errorMsg))
			}
			fmt.Println(errorMsg)
			// If we failed to execute this action it means that we got disconnected,
			// the reconnection mechanism will take care of that so we just return
			return
		}

		// Finally, resend all properties
		if err := d.sendDeviceProperties(); err != nil {
			errorMsg := fmt.Sprintf("Cannot send device properties: %v", err)
			if d.OnErrors != nil {
				d.OnErrors(d, errors.New(errorMsg))
			}
			fmt.Println(errorMsg)
			// If we failed to execute this action it means that we got disconnected,
			// the reconnection mechanism will take care of that so we just return
			return
		}
	}

	// If a device connected for the first time, since we do not ask
	// for a clean session and do not change its clientID, we can assume
	// that after connection a session is present
	d.sessionPresent = true

	// If some messages must be retried, do so
	d.resendFailedMessages()

	if d.OnConnectionStateChanged != nil {
		d.OnConnectionStateChanged(d, true)
	}
}

// SendIndividualMessageWithTimestamp adds to the publishing channel a new message towards an individual aggregation interface,
// with explicit timestamp. This call can be blocking if the channel is full (see `MaxInflightMessages`).
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
	// We are good to go. Let's add the message to the channel.
	return d.enqueueMqttV1Message(iface, interfacePath, value, timestamp)
}

// SendIndividualMessage adds to the publishing channel a new message towards an individual aggregation interface.
// This call can be blocking if the channel is full (see `MaxInflightMessages`).
func (d *Device) SendIndividualMessage(interfaceName, path string, value interface{}) error {
	return d.SendIndividualMessageWithTimestamp(interfaceName, path, value, time.Time{})
}

// SendAggregateMessageWithTimestamp adds to the publishing channel a new message towards an Object Aggregated interface,
// with explicit timestamp. values must be a map containing the last tip of the endpoint, with no
// slash, as the key, and the corresponding value as value. interfacePath should match the path
// of the base endpoint, without the last tip.
// This call can be blocking if the channel is full (see `MaxInflightMessages`).
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

	// We are good to go. Let's add the message to the channel.
	return d.enqueueMqttV1Message(iface, interfacePath, values, timestamp)
}

// SendAggregateMessage adds to the publishing channel a new message towards an Object Aggregated interface.
// values must be a map containing the last tip of the endpoint, with no
// slash, as the key, and the corresponding value as value. interfacePath should match the path
// of the base endpoint, without the last tip.
// This call can be blocking if the channel is full (see `MaxInflightMessages`).
// Example: if dealing with an aggregate interface with endpoints [/my/aggregate/firstValue, /my/aggregate/secondValue],
// interfacePath would be "/my/aggregate", and values would be map["firstValue": <value>, "secondValue": <value>]
func (d *Device) SendAggregateMessage(interfaceName, interfacePath string, values map[string]interface{}) error {
	return d.SendAggregateMessageWithTimestamp(interfaceName, interfacePath, values, time.Time{})
}

// SetProperty sets a  property in Astarte. It adds to the publishing channel a property set
// message that is equivalent to `SendIndividualMessage` in every regard, except this call will fail
// if the interface isn't a property interface. It is advised to use this call, as APIs might change in
// the future. Once the property is set, it is also added to the local cache and can be retrieved at any time
// using `GetProperty` or `GetAllProperties`.
// This call can be blocking if the channel is full (see `MaxInflightMessages`).
func (d *Device) SetProperty(interfaceName, path string, value interface{}) error {
	// Get the interface from the introspection
	iface, ok := d.interfaces[interfaceName]
	if ok {
		if iface.Type != interfaces.PropertiesType {
			return fmt.Errorf("SetProperty can be used only on Property Interfaces", interfaceName)
		}
		// Validate the message
		if err := interfaces.ValidateIndividualMessage(iface, path, value); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("Interface %s not registered", interfaceName)
	}
	// We are good to go. Let's add the message to the channel.
	return d.enqueueMqttV1Message(iface, path, value, time.Time{})
}

// UnsetProperty deletes an existing property from Astarte, if the property can be unset. It adds to the publishing
// channel a property deletion message that contains the interface name and the path to be deleted. Once the property
// is deleted, it is also removed from the local cache.
// This call can be blocking if the channel is full (see `MaxInflightMessages`).
func (d *Device) UnsetProperty(interfaceName, path string) error {
	// Get the interface from the introspection
	iface, ok := d.interfaces[interfaceName]
	if ok {
		if iface.Type != interfaces.PropertiesType {
			return fmt.Errorf("UnsetProperty can be used only on Property Interfaces", interfaceName)
		}
		// Validate the path and whether it can allow unset
		mapping, err := interfaces.InterfaceMappingFromPath(iface, path)
		if err != nil {
			return err
		}
		if !mapping.AllowUnset {
			return errors.New("unset can be called only on properties with allowUnset")
		}
	} else {
		return fmt.Errorf("Interface %s not registered", interfaceName)
	}
	// We are good to go. Let's add the message to the channel.
	// This is a delete property, so a special message: empty payload
	return d.enqueueRawMqttV1Message(iface, path, []byte{})
}

// The main publishing loop: retrieves messages from the publishing channel and sends them one at a time, in order
func (d *Device) sendLoop() {
	for next := range d.messageQueue {
		d.publishMessage(next)
	}
}

// Prepare a MqttV1 message and add it to the publishing channel.  Can be blocking if the channel is full.
func (d *Device) enqueueMqttV1Message(astarteInterface interfaces.AstarteInterface, interfacePath string, values interface{}, timestamp time.Time) error {
	payload := map[string]interface{}{"v": interfaces.NormalizePayload(values, false)}
	if !timestamp.IsZero() {
		payload["t"] = timestamp.UTC()
	}
	doc, err := bson.Marshal(payload)
	if err != nil {
		return err
	}

	return d.enqueueRawMqttV1Message(astarteInterface, interfacePath, doc)
}

// Prepare a MqttV1 message and add it to the publishing channel.  Can be blocking if the channel is full.
func (d *Device) enqueueRawMqttV1Message(astarteInterface interfaces.AstarteInterface, interfacePath string, bsonPayload []byte) error {
	var qos uint8 = 2
	var mapping interfaces.AstarteInterfaceMapping
	if astarteInterface.Ownership != interfaces.DeviceOwnership {
		return errors.New("can't send message to a non-Device owned interface")
	}
	if astarteInterface.Type == interfaces.DatastreamType {
		// Guaranteed we won't get an error here
		mapping, _ = interfaces.InterfaceMappingFromPath(astarteInterface, interfacePath)
		switch mapping.Reliability {
		case interfaces.GuaranteedReliability:
			qos = 1
		case interfaces.UnreliableReliability:
			qos = 0
		}
	} else {
		// always store property messages
		mapping.Retention = interfaces.StoredRetention
	}

	if d.isSendingStoredMessages {
		fmt.Println("Sending previously stored messages with non-discard retention, the current message may be scheduled later")
	}
	message := makeAstarteMessageInfo(mapping.Expiry, mapping.Retention, astarteInterface.Name, interfacePath, astarteInterface.MajorVersion, qos, bsonPayload)
	d.messageQueue <- message

	return nil
}

func (d *Device) publishMessage(message astarteMessageInfo) error {
	topic := fmt.Sprintf("%s/%s%s", d.getBaseTopic(), message.InterfaceName, message.Path)

	// MQTT client returns `true` to IsConnected()
	// even if it is actually reconnecting
	if !d.m.IsConnectionOpen() {
		fmt.Printf("Message on %s not delivered: MQTT client disconnected\n", topic)
		d.storeMessage(message)
	} else {
		t := d.m.Publish(topic, message.Qos, false, message.Payload)
		// We wait for either successful delivery or error
		_ = t.Wait()
		if t.Error() != nil {
			fmt.Printf("Message on %s not delivered: %s\n", topic, t.Error().Error())
			d.storeMessage(message)
		} else {
			// message delivered, check if we need to remove it from the db
			// as messages with volatile retention have already been removed form the volatile queue
			if message.Retention == string(interfaces.StoredRetention) && message.StorageId != 0 {
				d.removeFailedMessageFromStorage(message.StorageId)
				// if message.storageId == 0, then the message never was in our db
			}
			// if it's a property, we need to set it or delete it
			if d.interfaces[message.InterfaceName].Type == interfaces.PropertiesType {
				if len(message.Payload) > 0 {
					go d.storeProperty(message.InterfaceName, message.Path, d.interfaces[message.InterfaceName].MajorVersion, message.Payload)
				} else {
					go d.removePropertyFromStorage(message.InterfaceName, message.Path, message.InterfaceMajor)
				}
			}
		}
	}

	return nil
}

func (d *Device) storeMessage(message astarteMessageInfo) {
	switch message.Retention {
	case string(interfaces.DiscardRetention):
		fmt.Printf("Message on %s%s failed, discarded because it has discard retention\n", message.InterfaceName, message.Path)
		return
	case string(interfaces.VolatileRetention):
		d.volatileMessages = append(d.volatileMessages, message)
	case string(interfaces.StoredRetention):
		d.storeFailedMessage(message)
	}
}

func (d *Device) resendFailedMessages() {
	d.isSendingStoredMessages = true
	fmt.Println("Sending old messages with non-discard retention")
	d.resendStoredMessages()
	d.resendVolatileMessages()
	// messages are again accepted
	d.isSendingStoredMessages = false
}

func (d *Device) setupSubscriptions() error {
	// Initialize with control subscriptions
	subscriptions := map[string]byte{
		fmt.Sprintf("%s/control/consumer/properties", d.getBaseTopic()): 2,
	}
	for _, i := range d.interfaces {
		if i.Ownership == interfaces.ServerOwnership {
			subscriptions[fmt.Sprintf("%s/%s/#", d.getBaseTopic(), i.Name)] = 2
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
	fmt.Printf("Sending introspection for %s\n", d.getBaseTopic())
	if !t.WaitTimeout(5 * time.Second) {
		return errors.New("Timed out while sending introspection")
	}
	return t.Error()
}

func (d *Device) sendEmptyCache() error {
	// Set up empty cache
	emptyCacheTopic := fmt.Sprintf("%s/control/emptyCache", d.getBaseTopic())

	// Send it
	t := d.m.Publish(emptyCacheTopic, 2, false, "1")
	fmt.Printf("Sending empty cache for %s\n", d.getBaseTopic())
	if !t.WaitTimeout(5 * time.Second) {
		return errors.New("Timed out while sending empty cache")
	}
	return t.Error()
}

func (d *Device) sendDeviceProperties() error {
	properties := d.retrieveDevicePropertiesFromStorage()
	var purgePropertiesMessage string

	// Check if property is device-owned
	for _, property := range properties {
		if d.interfaces[property.InterfaceName].Ownership == interfaces.DeviceOwnership {
			// if so, set up publish
			topic := fmt.Sprintf("%s/%s%s", d.getBaseTopic(), property.InterfaceName, property.Path)
			// And just DO IT
			t := d.m.Publish(topic, 2, false, property.RawValue)
			fmt.Printf("Sending device property %s\n", topic)
			if !t.WaitTimeout(5 * time.Second) {
				return errors.New("Timed out while sending device property")
			}
			// If an error occurred, let's return and see what's in store
			if t.Error() != nil {
				return t.Error()
			}

			// Add to the purge properties message
			purgePropertiesMessage += property.InterfaceName + property.Path + ";"
		}
	}

	if len(purgePropertiesMessage) > 0 {
		// If we built a purge properties message, remove the trailing ";"
		purgePropertiesMessage = purgePropertiesMessage[:len(purgePropertiesMessage)-1]
	}
	// We're sending the purge properties message even if it's empty, as it indeed has a meaning
	return d.sendPurgeProperties(purgePropertiesMessage)
}

func (d *Device) sendPurgeProperties(purgePropertiesMessage string) error {
	var (
		payloadHeader [4]byte
		out           []byte
		err           error
	)

	bytesWriter := new(bytes.Buffer)
	flateWriter := zlib.NewWriter(bytesWriter)

	_, err = io.Copy(flateWriter, bytes.NewBufferString(purgePropertiesMessage))
	if err != nil {
		return err
	}
	if e := flateWriter.Close(); e != nil {
		return e
	}

	// Create the output by adding the padding and the deflated message
	binary.BigEndian.PutUint32(payloadHeader[0:4], uint32(len(purgePropertiesMessage)))
	out = append(out, payloadHeader[0:4]...)
	out = append(out, bytesWriter.Bytes()...)

	// This is a special control message, as such we gotta circumvent the queue and just go for it (QoS == 2)
	t := d.m.Publish(fmt.Sprintf("%s/control/producer/properties", d.getBaseTopic()), 2, false, out)
	// We wait for either successful delivery or error
	_ = t.Wait()
	return t.Error()
}

func parseBSONPayload(payload []byte) (map[string]interface{}, error) {
	// Parse the payload
	parsed := map[string]interface{}{}
	err := bson.Unmarshal(payload, parsed)
	return parsed, err
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
