// Copyright © 2021 Ispirata Srl
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

	"gorm.io/gorm"
)

// DeviceOptions define a set of configurable options for a Device.
type DeviceOptions struct {
	// MaxInflightMessages is the maximum number of messages that can be in publishing channel at any given time
	// before adding messages becomes blocking.
	MaxInflightMessages int

	// IgnoreSSLErrors allows the device to ignore client SSL errors during connection.
	// Useful if you're using the device to connect to a test instance of Astarte with self signed certificates,
	// it is not recommended to leave this to true in production.
	IgnoreSSLErrors bool

	// AutoReconnect sets whether the device should reconnect automatically if it loses the connection
	// after establishing it.
	AutoReconnect bool

	// ConnectRetry sets whether the device should retry to connect if the first connection
	// fails. Defaults to false.
	ConnectRetry bool

	// MaxRetries sets the number of attempts for the device to establish the first connection.
	// If ConnectRetry is false, MaxRetries will be ignored.
	MaxRetries int

	// RootCAs, when not nil, sets a custom set of Root CAs to trust against the broker
	RootCAs *x509.CertPool

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

	// UseDatabase defines whether the SDK should use a database for properties
	// and datastream persistency. It will be stored in the specified PersistencyDir.
	// When set to true, and no database is provided, the default database (SQLite) will
	// be used.
	UseDatabase bool

	// Database is a pointer to a valid, open gorm DB to be used for persistency, provided
	// UseDatabase is true. Ensure you're passing a dedicated DB which
	// can be used exclusively by the Astarte SDK to prevent naming conflicts.
	// If both this variable is nil and UseDatabase is false, no database will be used.
	// Not using a database when you can rely on persistency is strongly discouraged - however,
	// there might be cases in which such a thing is needed, for example if you cannot use CGO
	// when compiling your executable.
	Database *gorm.DB
}

// NewDeviceOptions will create a new DeviceOptions with some
// sensible default values.
func NewDeviceOptions() DeviceOptions {
	return DeviceOptions{
		UseMqttStore:        true,
		UseDatabase:         true,
		IgnoreSSLErrors:     false,
		MaxInflightMessages: 100,
		AutoReconnect:       true,
		ConnectRetry:        true,
		MaxRetries:          10,
	}
}

func (opts *DeviceOptions) validate() error {
	switch {
	case len(opts.PersistencyDir) == 0 && opts.UseMqttStore:
		return errors.New("cannot enable mqtt store without a persistencyDir")
	case len(opts.PersistencyDir) == 0 && opts.UseDatabase:
		return errors.New("cannot use default database without a persistencyDir")
	case opts.Database != nil && !opts.UseDatabase:
		return errors.New("requested not to use database, but provided one")
	}

	return nil
}
