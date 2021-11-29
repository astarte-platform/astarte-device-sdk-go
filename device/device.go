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
	"os"
	"path/filepath"
	"runtime"

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

// Device is the base struct for Astarte Devices
type Device struct {
	deviceID                string
	realm                   string
	persistencyDir          string
	cryptoDir               string
	useMqttStore            bool
	m                       mqtt.Client
	interfaces              map[string]interfaces.AstarteInterface
	astarteAPIClient        *client.Client
	brokerURL               string
	db                      *gorm.DB
	messageQueue            chan astarteMessageInfo
	isSendingStoredMessages bool
	volatileMessages        []astarteMessageInfo
	lastSentIntrospection   string
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

// DeviceOptions define a set of advanced options when creating the Device
type DeviceOptions struct {
	// UseMqttStore defines whether the SDK will use a store or not for the underlying
	// MQTT client. It will use the PersistencyDir when it is activated.
	UseMqttStore bool
	// PersistencyDir is a known path that will be used by the SDK for MQTT
	// persistency, for the default database if used, and for crypto storage unless
	// CryptoDir is specified.
	// If none of them are specified, the persistency dir can be empty. Otherwise,
	// an error will be returned upon creation.
	//
	// Creating a Device without persistency is not recommended and should be done only
	// when aware of the implications. Please read all documentation for details.
	PersistencyDir string
	// CryptoDir is a known, available path that will be used by the SDK for storing
	// certificate and private key for connecting to Astarte. When not specified, it
	// will use the PersistencyDir if specified, or a temporary directory if no persistency
	// was enabled. Please keep in mind that if a temporary directory is used for crypto
	// storage, certificates and private keys will always be regenerated at each startup,
	// which will take a toll on performances and startup times.
	CryptoDir string

	// UseDefaultDatabase defines whether the SDK should use its default database (SQLite)
	// for properties and datastream persistency. It will be stored in the specified
	// PersistencyDir. When set to true, it takes precedence over Database.
	UseDefaultDatabase bool
	// Database is a pointer to a valid, open gorm DB to be used for persistency, provided
	// UseDefaultDatabase is false. Ensure you're passing a dedicated DB which
	// can be used exclusively by the Astarte SDK to prevent naming conflicts.
	// If both this variable is nil and UseDefaultDatabase is false, no database will be used.
	// Not using a database when you can rely on persistency is strongly discouraged - however,
	// there might be cases in which such a thing is needed, for example if you cannot use CGO
	// when compiling your executable.
	Database *gorm.DB
}

// NewDevice creates a new Device without persistency.
// NOTE: This constructor is NOT safe to use in production, as all persistency is turned off. When
// doing so, a number of features won't be available. Please use NewDeviceWithPersistency or
// NewDeviceWithOptions instead.
//
// Deprecated: due to its ambiguousness, this constructor will be removed or changed in the future.
func NewDevice(deviceID, realm, credentialsSecret, pairingBaseURL string) (*Device, error) {
	opts := DeviceOptions{
		UseMqttStore:       false,
		UseDefaultDatabase: false,
	}
	return newDevice(deviceID, realm, credentialsSecret, pairingBaseURL, opts)
}

// NewDeviceWithPersistency creates a new Device with a known persistency directory and an SQLite database
// which will be created and stored within that directory. It sticks to sane defaults and it is the right
// choice in most cases. Please note that when creating a device like this,
// you must compile your executable with CGO enabled due to SQLite requirements.
// More advanced configuration can be provided when creating a device with NewDeviceWithOptions.
func NewDeviceWithPersistency(deviceID, realm, credentialsSecret, pairingBaseURL, persistencyDir string) (*Device, error) {
	opts := DeviceOptions{
		UseMqttStore:       true,
		PersistencyDir:     persistencyDir,
		UseDefaultDatabase: true,
	}
	return newDevice(deviceID, realm, credentialsSecret, pairingBaseURL, opts)
}

// NewDeviceWithOptions creates a new Device with an advanced set of options. It is meant to be used in
// those cases where NewDeviceWithPersistency doesn't provide you with a satisfactory configuration.
// Please refer to DeviceOptions documentation for all details.
// NewDeviceWithOptions will also return an error and will fail if the provided options are invalid or
// incompatible (e.g.: when requiring a default database without a persistency directory).
// If you are unsure about what some of these options mean, it is advised to stick to NewDeviceWithPersistency.
func NewDeviceWithOptions(deviceID, realm, credentialsSecret, pairingBaseURL string, opts DeviceOptions) (*Device, error) {
	return newDevice(deviceID, realm, credentialsSecret, pairingBaseURL, opts)
}

func validateDeviceOptions(opts DeviceOptions) error {
	switch {
	case len(opts.PersistencyDir) == 0 && opts.UseMqttStore:
		return errors.New("cannot enable mqtt store without a persistencyDir")
	case len(opts.PersistencyDir) == 0 && opts.UseDefaultDatabase:
		return errors.New("cannot use default database without a persistencyDir")
	}

	return nil
}

func newDevice(deviceID, realm, credentialsSecret, pairingBaseURL string, opts DeviceOptions) (*Device, error) {
	if !misc.IsValidAstarteDeviceID(deviceID) {
		return nil, fmt.Errorf("%s is not a valid Device ID", deviceID)
	}

	if err := validateDeviceOptions(opts); err != nil {
		return nil, err
	}

	d := new(Device)
	d.deviceID = deviceID
	d.realm = realm
	d.useMqttStore = opts.UseMqttStore
	d.persistencyDir = opts.PersistencyDir
	d.interfaces = map[string]interfaces.AstarteInterface{}
	d.MaxInflightMessages = 100

	if len(opts.CryptoDir) > 0 {
		d.cryptoDir = opts.CryptoDir
	} else {
		if len(d.persistencyDir) > 0 {
			d.cryptoDir = filepath.Join(d.persistencyDir, "crypto")
		} else {
			// Use a temporary directory then
			tempDir, err := os.MkdirTemp("", deviceID)
			if err != nil {
				return nil, err
			}
			d.cryptoDir = filepath.Join(tempDir, "crypto")

			// Add a finalizer for the temporary directory
			runtime.SetFinalizer(d, func(d *Device) {
				os.RemoveAll(tempDir)
			})
		}

		// Always create a subdir with the right set of permissions.
		if err := os.MkdirAll(d.cryptoDir, 0700); err != nil {
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
	if opts.UseDefaultDatabase {
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
