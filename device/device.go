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
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/astarte-platform/astarte-go/client"
	"github.com/astarte-platform/astarte-go/interfaces"
	"github.com/astarte-platform/astarte-go/misc"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// Device is the base struct for Astarte Devices
type Device struct {
	deviceID         string
	realm            string
	persistencyDir   string
	m                mqtt.Client
	interfaces       map[string]interfaces.AstarteInterface
	astarteAPIClient *client.Client
	// AutoReconnect sets whether the device should reconnect automatically
	AutoReconnect bool
	// RootCAs, when not nil, sets a custom set of Root CAs to trust against the broker
	RootCAs                     *x509.CertPool
	OnIndividualMessageReceived func(*Device, IndividualMessage)
	OnAggregateMessageReceived  func(*Device, AggregateMessage)
	OnErrors                    func(*Device, error)
	OnConnectionStateChanged    func(*Device, bool)
}

// NewDevice creates a new Device
func NewDevice(deviceID, realm, credentialsSecret string, pairingBaseURL string) (*Device, error) {
	// Create temporary directory for the persistent data
	// TODO: How to clean this up?
	persistencyDir, err := ioutil.TempDir("", deviceID)
	if err != nil {
		return nil, err
	}

	return newDevice(deviceID, realm, credentialsSecret, pairingBaseURL, persistencyDir)
}

// NewDeviceWithPersistency creates a new Device with a known persistency directory
func NewDeviceWithPersistency(deviceID, realm, credentialsSecret string, pairingBaseURL string, persistencyDir string) (*Device, error) {
	return newDevice(deviceID, realm, credentialsSecret, pairingBaseURL, persistencyDir)
}

func newDevice(deviceID, realm, credentialsSecret string, pairingBaseURL string, persistencyDir string) (*Device, error) {
	if !misc.IsValidAstarteDeviceID(deviceID) {
		return nil, fmt.Errorf("%s is not a valid Device ID", deviceID)
	}

	d := new(Device)
	d.deviceID = deviceID
	d.realm = realm
	d.persistencyDir = persistencyDir
	d.interfaces = map[string]interfaces.AstarteInterface{}

	var err error
	d.astarteAPIClient, err = client.NewClientWithIndividualURLs(map[misc.AstarteService]string{misc.Pairing: pairingBaseURL}, nil)
	if err != nil {
		return nil, err
	}
	d.astarteAPIClient.SetToken(credentialsSecret)

	return d, nil
}

// Connect connects the device through a goroutine
func (d *Device) Connect(result chan<- error) {
	go func(result chan<- error) {
		// Are we connected already?
		if d.IsConnected() {
			if result != nil {
				result <- nil
			}
			return
		}

		// At least one interface available?
		if len(d.interfaces) == 0 {
			if result != nil {
				result <- errors.New("Add at least an interface before attempting to connect")
			}
			return
		}

		// First of all, ensure we have a certificate
		if err := d.ensureCertificate(); err != nil {
			if result != nil {
				result <- err
			}
			return
		}

		brokerURL, err := d.getBrokerURL()
		if err != nil {
			if result != nil {
				result <- err
			}
			return
		}

		if err := d.initializeMQTTClient(brokerURL); err != nil {
			if result != nil {
				result <- err
			}
			return
		}

		// Wait for the token - we're in a coroutine anyway
		connectToken := d.m.Connect()
		if ok := connectToken.WaitTimeout(30 * time.Second); !ok {
			if result != nil {
				result <- errors.New("Timed out while connecting to the Broker")
			}
			return
		}
		if connectToken.Error() != nil {
			if result != nil {
				result <- connectToken.Error()
			}
			return
		}

		// If connected successfully, setup subscriptions and send the introspection before notifying
		if err := d.setupSubscriptions(); err != nil {
			d.m.Disconnect(0)
			if result != nil {
				result <- err
			}
			return
		}
		if err := d.sendIntrospection(); err != nil {
			d.m.Disconnect(0)
			if result != nil {
				result <- err
			}
			return
		}

		// All good: notify, and our routine is over.
		if result != nil {
			result <- nil
		}
	}(result)
}

// Disconnect disconnects the device
func (d *Device) Disconnect(result chan<- error) {
	// Wait 2 seconds and die
	d.m.Disconnect(2000)
}

// IsConnected returns whether the device is connected or not
func (d *Device) IsConnected() bool {
	if d.m != nil {
		return d.m.IsConnected()
	}
	return false
}

// AddInterface adds an interface to the device
func (d *Device) AddInterface(astarteInterface interfaces.AstarteInterface) {
	d.interfaces[astarteInterface.Name] = astarteInterface
}

// RemoveInterface removes an interface from the device
func (d *Device) RemoveInterface(astarteInterface interfaces.AstarteInterface) {
	delete(d.interfaces, astarteInterface.Name)
}
