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
	"path/filepath"

	"github.com/astarte-platform/astarte-go/client"
	"github.com/astarte-platform/astarte-go/interfaces"
	"github.com/astarte-platform/astarte-go/misc"
	backoff "github.com/cenkalti/backoff/v4"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

const (
	DefaultInitialConnectionAttempts = 10
)

// Device is the base struct for Astarte Devices
type Device struct {
	deviceID                string
	realm                   string
	persistencyDir          string
	m                       mqtt.Client
	interfaces              map[string]interfaces.AstarteInterface
	astarteAPIClient        *client.Client
	brokerURL               string
	db                      *gorm.DB
	messageQueue            chan astarteMessageInfo
	isSendingStoredMessages bool
	volatileMessages        []astarteMessageInfo
	sessionPresent          bool
	// MaxInflightMessages is the maximum number of messages that can be in publishing channel at any given time
	// before adding messages becomes blocking. Defaults to 100.
	MaxInflightMessages int
	// IgnoreSSLErrors allows the device to ignore client SSL errors during connection.
	// Useful if you're using the device to connect to a test instance of Astarte with self signed certificates,
	// it is not recommended to leave this to `true` in production. Defaults to `false`.
	IgnoreSSLErrors bool
	// AutoReconnect sets whether the device should reconnect automatically if it loses the connection
	// after establishing it. Defaults to false.
	AutoReconnect bool
	// ConnectRetry sets whether the device should retry to connect if the first connection
	// fails. Defaults to false.
	ConnectRetry bool
	// MaxRetries sets the number of attempts for the device to establish the first connection.
	// If ConnectRetry is false, MaxRetries will be ignored. Defaults to 10.
	MaxRetries int
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

	return newDevice(deviceID, realm, credentialsSecret, pairingBaseURL, persistencyDir, false, nil)
}

// NewDeviceWithPersistency creates a new Device with a known persistency directory and an SQLite database
// which will be created and stored within that directory. Please note that when creating a device like this,
// you must have had CGO enabled when compiling your executable due to SQLite requirements.
// Not using a Database is not recommended, but you can do so by creating your device using NewDeviceWithPersistencyAndDatabase.
func NewDeviceWithPersistency(deviceID, realm, credentialsSecret string, pairingBaseURL string, persistencyDir string) (*Device, error) {
	return newDevice(deviceID, realm, credentialsSecret, pairingBaseURL, persistencyDir, true, nil)
}

// NewDeviceWithPersistencyAndDatabase creates a new Device with a known persistency directory and a Database
// of the user's choice. If db is nil, no database will be used. Otherwise, any valid and open gorm Database
// will be used. If you are unsure about this option and want to use a Database, stick to NewDeviceWithPersistency.
// Otherwise, ensure you're passing a dedicated DB to the SDK which can be used exclusively by the Astarte SDK
// to prevent naming conflicts.
// Not using a database when you can rely on persistency is strongly discouraged - however, there might be cases in
// which such a thing is needed, for example if you could not use CGO when compiling your executable.
func NewDeviceWithPersistencyAndDatabase(deviceID, realm, credentialsSecret string, pairingBaseURL string, persistencyDir string, db *gorm.DB) (*Device, error) {
	return newDevice(deviceID, realm, credentialsSecret, pairingBaseURL, persistencyDir, false, db)
}

func newDevice(deviceID, realm, credentialsSecret string, pairingBaseURL string, persistencyDir string, defaultDB bool, db *gorm.DB) (*Device, error) {
	if !misc.IsValidAstarteDeviceID(deviceID) {
		return nil, fmt.Errorf("%s is not a valid Device ID", deviceID)
	}

	d := new(Device)
	d.deviceID = deviceID
	d.realm = realm
	d.persistencyDir = persistencyDir
	d.interfaces = map[string]interfaces.AstarteInterface{}
	d.MaxInflightMessages = 100

	var err error
	d.astarteAPIClient, err = client.NewClientWithIndividualURLs(map[misc.AstarteService]string{misc.Pairing: pairingBaseURL}, nil)
	if err != nil {
		return nil, err
	}
	d.astarteAPIClient.SetToken(credentialsSecret)

	// If a default database was requested, manage the default SQLite DB
	if defaultDB {
		dbpath := filepath.Join(d.getDefaultDbDir(), "persistency.db")
		d.db, err = gorm.Open(sqlite.Open(dbpath), &gorm.Config{})
		if err != nil {
			fmt.Println("failed to load default database")
		}
	} else {
		d.db = db
	}

	// migrateDb, like all DB functions, will just blink if no DB is available
	if err := d.migrateDb(); err != nil {
		errors.New("Database migration failed")
		return nil, err
	}

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

		// Define a retry policy and the operation to be executed, i.e. we want to
		// retrieve the brokerURL with an HTTP request
		policy := d.makeRetryPolicy()
		ensureBrokerURLOperation := func() error {
			return d.ensureBrokerURL()
		}

		err := backoff.Retry(ensureBrokerURLOperation, policy)
		if err != nil {
			if result != nil {
				if d.ConnectRetry {
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

		// initialize the client
		if err := d.initializeMQTTClient(); err != nil {
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
			if !connectToken.SessionPresent() {
				fmt.Println("No MQTT session already present, starting one")
			} else {
				// remember that a session is present for future reconnections
				d.sessionPresent = connectToken.SessionPresent()
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

		// Now that the client is up and running, we can start sending messages
		d.messageQueue = make(chan astarteMessageInfo, d.MaxInflightMessages)
		go d.sendLoop()

		// All good: notify, and our routine is over.
		if result != nil {
			result <- nil
		}
	}(result)
}

func (d *Device) connectionRetryAttempts() int {
	switch {
	case d.ConnectRetry && d.MaxRetries > 0:
		return d.MaxRetries
	case d.ConnectRetry:
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
// from the astarte-go/interfaces package.
// AddInterface returns `nil` if the interface was loaded successfully, or a corresponding error
// otherwise (e.g. interface validation failed).
func (d *Device) AddInterface(astarteInterface interfaces.AstarteInterface) error {
	if err := astarteInterface.Aggregation.IsValid(); err != nil {
		return err
	}
	if err := astarteInterface.Type.IsValid(); err != nil {
		return err
	}
	if err := astarteInterface.Ownership.IsValid(); err != nil {
		return err
	}

	for _, mapping := range astarteInterface.Mappings {
		if err := mapping.Reliability.IsValid(); err != nil {
			return err
		}
		if err := mapping.Retention.IsValid(); err != nil {
			return err
		}
		if err := mapping.DatabaseRetentionPolicy.IsValid(); err != nil {
			return err
		}
	}

	d.interfaces[astarteInterface.Name] = astarteInterface
	return nil
}

// RemoveInterface removes an interface from the device
func (d *Device) RemoveInterface(astarteInterface interfaces.AstarteInterface) {
	delete(d.interfaces, astarteInterface.Name)
}
