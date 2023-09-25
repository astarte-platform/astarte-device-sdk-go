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
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/astarte-platform/astarte-go/client"
	"github.com/astarte-platform/astarte-go/interfaces"
	"github.com/astarte-platform/astarte-go/misc"
	backoff "github.com/cenkalti/backoff/v4"
	mqtt "github.com/ispirata/paho.mqtt.golang"
	"gorm.io/gorm"
)

const (
	DefaultInitialConnectionAttempts = 10
)

type messageQueue struct {
	sync.Mutex
	queue chan astarteMessageInfo
}

// Device is the base struct for Astarte Devices
type Device struct {
	deviceID                    string
	realm                       string
	m                           mqtt.Client
	interfaces                  map[string]interfaces.AstarteInterface
	astarteAPIClient            *client.Client
	brokerURL                   string
	db                          *gorm.DB
	inflightMessages            *messageQueue
	isSendingStoredMessages     bool
	volatileMessages            []astarteMessageInfo
	lastSentIntrospection       string
	opts                        DeviceOptions
	OnIndividualMessageReceived func(*Device, IndividualMessage)
	OnAggregateMessageReceived  func(*Device, AggregateMessage)
	OnErrors                    func(*Device, error)
	OnConnectionStateChanged    func(*Device, bool)
}

// NewDevice creates a new Device without persistency.
// NOTE: This constructor is NOT safe to use in production, as all persistency is turned off. When
// doing so, a number of features won't be available. Please use NewDeviceWithPersistency or
// NewDeviceWithOptions instead.
//
// Deprecated: due to its ambiguousness, this constructor will be removed or changed in the future.
func NewDevice(deviceID, realm, credentialsSecret, pairingBaseURL string) (*Device, error) {
	opts := NewDeviceOptions()
	opts.UseMqttStore = false
	opts.UseDatabase = false
	return newDevice(deviceID, realm, credentialsSecret, pairingBaseURL, opts)
}

// NewDeviceWithPersistency creates a new Device with a known persistency directory and an SQLite database
// which will be created and stored within that directory. It sticks to sane defaults and it is the right
// choice in most cases. Please note that when creating a device like this,
// you must compile your executable with CGO enabled due to SQLite requirements.
// More advanced configuration can be provided when creating a device with NewDeviceWithOptions.
func NewDeviceWithPersistency(deviceID, realm, credentialsSecret, pairingBaseURL, persistencyDir string) (*Device, error) {
	opts := NewDeviceOptions()
	opts.PersistencyDir = persistencyDir
	return newDevice(deviceID, realm, credentialsSecret, pairingBaseURL, opts)
}

// NewDeviceWithOptions creates a new Device with a set of options. It is meant to be used in
// those cases where NewDeviceWithPersistency doesn't provide you with a satisfactory configuration.
// Please refer to DeviceOptions documentation for all details.
// NewDeviceWithOptions will also return an error and will fail if the provided options are invalid or
// incompatible (e.g.: when requiring a default database without a persistency directory).
// If you are unsure about what some of these options mean, it is advised to stick to NewDeviceWithPersistency.
func NewDeviceWithOptions(deviceID, realm, credentialsSecret, pairingBaseURL string, opts DeviceOptions) (*Device, error) {
	return newDevice(deviceID, realm, credentialsSecret, pairingBaseURL, opts)
}

func newDevice(deviceID, realm, credentialsSecret, pairingBaseURL string, opts DeviceOptions) (*Device, error) {
	if !misc.IsValidAstarteDeviceID(deviceID) {
		return nil, fmt.Errorf("%s is not a valid Device ID", deviceID)
	}

	if err := opts.validate(); err != nil {
		return nil, err
	}

	d := new(Device)
	d.deviceID = deviceID
	d.realm = realm
	d.interfaces = map[string]interfaces.AstarteInterface{}
	d.opts = opts

	if len(opts.CryptoDir) == 0 {
		if len(opts.PersistencyDir) > 0 {
			d.opts.CryptoDir = filepath.Join(d.opts.PersistencyDir, "crypto")
		} else {
			// Use a temporary directory then
			tempDir, err := ioutil.TempDir("", deviceID)
			if err != nil {
				return nil, err
			}
			d.opts.CryptoDir = filepath.Join(tempDir, "crypto")

			// Add a finalizer for the temporary directory
			runtime.SetFinalizer(d, func(d *Device) {
				os.RemoveAll(tempDir)
			})
		}

		// Always create a subdir with the right set of permissions.
		if err := os.MkdirAll(d.opts.CryptoDir, 0700); err != nil {
			return nil, err
		}
	}

	var err error
	d.astarteAPIClient, err = client.NewClientWithIndividualURLs(map[misc.AstarteService]string{misc.Pairing: pairingBaseURL}, nil)
	if err != nil {
		return nil, err
	}
	d.astarteAPIClient.SetToken(credentialsSecret)

	// If a default database was requested, manage the default SQLite DB
	if opts.UseDatabase {
		d.db, err = d.getDefaultDB()
		if err != nil {
			return nil, err
		}
	} else {
		d.db = opts.Database
	}

	// migrateDb, like all DB functions, will just blink if no DB is available
	if err := d.migrateDb(); err != nil {
		return nil, err
	}

	return d, nil
}

// Connect connects the device through a goroutine
//nolint
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
				result <- errors.New("add at least an interface before attempting to connect")
			}
			return
		}

		// Define a retry policy and the operation to be executed, i.e. we want to
		// retrieve the brokerURL with an HTTP request
		policy := d.makeRetryPolicy()
		ensureBrokerURLOperation := func() error {
			return d.ensureBrokerURL()
		}

		err := backoff.Retry(ensureBrokerURLOperation, policy)
		if err != nil {
			if result != nil {
				if d.opts.ConnectRetry {
					errorMsg := fmt.Sprintf("Cannot establish a connection after %d attempts.", d.connectionRetryAttempts())
					result <- errors.New(errorMsg)
				} else {
					result <- err
				}
			}
			return
		}

		// Ensure we have a certificate
		policy.Reset()
		ensureCertificateOperation := func() error {
			return d.ensureCertificate()
		}

		err = backoff.Retry(ensureCertificateOperation, policy)
		if err != nil {
			if result != nil {
				errorMsg := fmt.Sprintf("Cannot ensure certificate: %v", err)
				result <- errors.New(errorMsg)
			}
			return
		}

		// Now that the client is up and running, we can start sending messages
		value := messageQueue{queue: make(chan astarteMessageInfo, d.opts.MaxInflightMessages)}
		d.inflightMessages = &value

		// initialize the client
		if err = d.initializeMQTTClient(); err != nil {
			if result != nil {
				result <- err
			}
			return
		}

		// Wait for the token - we're in a coroutine anyway
		policy.Reset()
		connectOperation := func() error {
			connectToken := d.m.Connect().(*mqtt.ConnectToken)
			if connectToken.Wait() && connectToken.Error() != nil {
				return connectToken.Error()
			}
			return nil
		}
		err = backoff.Retry(connectOperation, policy)
		if err != nil {
			if result != nil {
				result <- err
			}
			return
		}

		go d.sendLoop()

		// All good: notify, and our routine is over.
		if result != nil {
			result <- nil
		}
	}(result)
}

func (d *Device) connectionRetryAttempts() int {
	switch {
	case d.opts.ConnectRetry && d.opts.MaxRetries > 0:
		return d.opts.MaxRetries
	case d.opts.ConnectRetry:
		return DefaultInitialConnectionAttempts
	default:
		return 0
	}
}

func (d *Device) makeRetryPolicy() backoff.BackOff {
	policy := backoff.NewExponentialBackOff()
	retries := d.connectionRetryAttempts()

	return backoff.WithMaxRetries(policy, uint64(retries))
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

// AddInterface adds an interface to the device. The interface must be loaded with ParseInterface
// from the astarte-go/interfaces package, which also ensures that the interface is valid.
// The return value should be ignored as the error is always `nil` (i.e. AddInterface cannot fail).
// TODO since the function always returns nil, do not return an error (target release 1.0)
func (d *Device) AddInterface(astarteInterface interfaces.AstarteInterface) error {
	d.interfaces[astarteInterface.Name] = astarteInterface
	return nil
}

// RemoveInterface removes an interface from the device
func (d *Device) RemoveInterface(astarteInterface interfaces.AstarteInterface) {
	delete(d.interfaces, astarteInterface.Name)
}
