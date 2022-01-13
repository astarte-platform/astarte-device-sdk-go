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
	"bytes"
	"compress/zlib"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/astarte-platform/astarte-go/interfaces"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type astarteDeviceStatus struct {
	DeviceId          string `gorm:"primaryKey;not null"`
	LastIntrospection string `gorm:"not null"`
}

type astarteMessageInfo struct {
	StorageId      int    `gorm:"primaryKey;autoIncrement;not null"`
	Retention      string `gorm:"not null"`
	AbsoluteExpiry int64  `gorm:"not null"`
	InterfaceName  string `gorm:"not null"`
	Path           string `gorm:"not null"`
	InterfaceMajor int    `gorm:"not null"`
	Qos            uint8  `gorm:"not null"`
	Payload        []byte `gorm:"not null"`
}

type property struct {
	InterfaceName  string `gorm:"primaryKey;not null"`
	Path           string `gorm:"primaryKey;not null"`
	InterfaceMajor int    `gorm:"primaryKey;not null"`
	RawValue       []byte `gorm:"not null"`
}

func (d *Device) getDefaultDB() (*gorm.DB, error) {
	if len(d.opts.PersistencyDir) == 0 {
		return nil, errors.New("persistency not enabled")
	}

	dbDir := filepath.Join(d.opts.PersistencyDir, "db")
	if err := os.MkdirAll(dbDir, 0700); err != nil {
		return nil, err
	}

	dbpath := filepath.Join(dbDir, "persistency.db")
	return gorm.Open(sqlite.Open(dbpath), &gorm.Config{})
}

func (d *Device) migrateDb() error {
	if d.db == nil {
		// Nothing to do
		return nil
	}
	if err := d.db.AutoMigrate(&astarteDeviceStatus{}, &astarteMessageInfo{}, &property{}); err != nil {
		return fmt.Errorf("error in database migration: %s", err.Error())
	}
	return nil
}

func makeAstarteMessageInfo(expiry int, retention interfaces.AstarteMappingRetention, interfaceName string, path string, major int, qos uint8, payload []byte) astarteMessageInfo {
	var absoluteExpiry int64 = 0
	if expiry != 0 {
		absoluteExpiry = time.Now().Unix() + int64(expiry)
	}
	return astarteMessageInfo{AbsoluteExpiry: absoluteExpiry, Retention: string(retention), InterfaceName: interfaceName, Path: path, InterfaceMajor: major, Qos: qos, Payload: payload}
}

func (d *Device) storeFailedMessage(message astarteMessageInfo) {
	if d.db == nil {
		// Nothing to do
		return
	}
	// If the StorageId is != 0, then the message was already stored, no point in failing a transaction
	if message.StorageId == 0 {
		// Gorm creates a (autoincrementing) StorageId for us if it is 0: thank you gorm!
		d.db.Create(&message)
	}
}

func (d *Device) removeFailedMessageFromStorage(storageId int) {
	if d.db == nil {
		// Nothing to do
		return
	}
	d.db.Delete(&astarteMessageInfo{}, storageId)
}

func (d *Device) resendStoredMessages() {
	if d.db == nil {
		// Nothing to do
		return
	}
	var messages []astarteMessageInfo
	d.db.Find(&messages)
	for _, message := range messages {
		if !isStoredMessageExpired(message) && !d.isInterfaceOutdatedInIntrospection(message.InterfaceName, message.InterfaceMajor) {
			// if the message is not expired, try resending it
			d.messageQueue <- message
		} else {
			// else, it can be removed
			d.removeFailedMessageFromStorage(message.StorageId)
		}
	}
}

func (d *Device) resendVolatileMessages() {
	if d.db == nil {
		// Nothing to do
		return
	}
	for len(d.volatileMessages) > 0 {
		message := d.volatileMessages[0]
		d.volatileMessages = d.volatileMessages[1:]
		// try resending the message only if it is not expired
		if !isStoredMessageExpired(message) && !d.isInterfaceOutdatedInIntrospection(message.InterfaceName, message.InterfaceMajor) {
			d.messageQueue <- message
		}
	}
}

func isStoredMessageExpired(message astarteMessageInfo) bool {
	return message.AbsoluteExpiry <= time.Now().Unix() && message.AbsoluteExpiry != 0
}

func (d *Device) isInterfaceOutdatedInIntrospection(interfaceName string, interfaceMajor int) bool {
	for _, astarteInterface := range d.interfaces {
		if astarteInterface.Name == interfaceName {
			if astarteInterface.MajorVersion != interfaceMajor {
				return true
			} else {
				return false
			}
		}
	}
	// if the interface is not present in the current introspection, it must be outdated
	return true
}

func (d *Device) storeProperty(interfaceName string, path string, interfaceMajor int, value []byte) {
	if d.db == nil {
		// Nothing to do
		return
	}
	d.db.Clauses(clause.OnConflict{
		UpdateAll: true,
	}).Create(&property{InterfaceName: interfaceName, Path: path, InterfaceMajor: interfaceMajor, RawValue: value})
}

func (d *Device) removePropertyFromStorage(interfaceName, path string, interfaceMajor int) {
	if d.db == nil {
		// Nothing to do
		return
	}
	d.db.Where(&property{InterfaceName: interfaceName, Path: path, InterfaceMajor: interfaceMajor}).Delete(&property{})
}

func (d *Device) removePropertyFromStorageForAllMajors(interfaceName, path string) {
	if d.db == nil {
		// Nothing to do
		return
	}
	d.db.Where(&property{InterfaceName: interfaceName, Path: path}).Delete(&property{})
}

func (d *Device) handlePurgeProperties(payload []byte) error {
	if d.db == nil {
		// If no DB is up, there's no action we have to take
		return nil
	}

	var (
		flateReader      io.ReadCloser
		err              error
		storedProperties map[string]map[string]interface{}
	)
	// Create a new bytearray with 4 padding bytes
	deflated := payload[4:]

	bytesReader := bytes.NewReader(deflated)
	flateReader, err = zlib.NewReader(bytesReader)
	if err != nil {
		return err
	}

	buf := new(bytes.Buffer)
	// G110: Copy in chunks
	var totalRead, n int64
	for {
		n, err = io.CopyN(buf, flateReader, 1024)
		totalRead += n
		if err != nil {
			if err == io.EOF {
				// We're done
				break
			}
			// Actual error
			return err
		}
	}

	if e := flateReader.Close(); e != nil {
		return e
	}

	// Get existing properties
	storedProperties, err = d.GetAllProperties()
	if err != nil {
		return err
	}

	purgePropertiesMessage := buf.String()
	if len(purgePropertiesMessage) > 0 {
		consumerProperties := strings.Split(purgePropertiesMessage, ";")
		for _, entry := range consumerProperties {
			// Get the content
			splt := strings.SplitN(entry, "/", 2)
			if len(splt) != 2 {
				// Message was corrupt somehow
				continue
			}
			interfaceName := splt[0]
			interfacePath := "/" + splt[1]

			if paths, ok := storedProperties[interfaceName]; ok {
				// Delete is a no-op if the path doesn't exist
				delete(paths, interfacePath)
				// Rewrite the map. To spare some resources, simply erase the key from the main map if we
				// came to an empty map
				if len(paths) > 0 {
					storedProperties[interfaceName] = paths
				} else {
					delete(storedProperties, interfaceName)
				}
			}
		}
	}

	// Ok - we need to erase what's left from the storage
	for k, v := range storedProperties {
		for path := range v {
			d.removePropertyFromStorageForAllMajors(k, path)
		}
	}

	return nil
}

func (d *Device) retrieveDevicePropertiesFromStorage() []property {
	if d.db == nil {
		return []property{}
	}

	var properties []property
	d.db.Find(&properties)
	upToDate := []property{}
	for _, property := range properties {
		if !d.isInterfaceOutdatedInIntrospection(property.InterfaceName, property.InterfaceMajor) {
			// do not send an outdated property
			// we can safely assume that properties is not a big collection
			upToDate = append(upToDate, property)

		}
		// TODO: cleanup outdated properties
	}
	return upToDate
}

func valueFromBSONPayload(payload []byte) (interface{}, error) {
	parsed, err := parseBSONPayload(payload)
	if err != nil {
		return nil, err
	}

	v, ok := parsed["v"]
	if !ok {
		return nil, errors.New("malformed property")
	}

	return v, nil
}

func (d *Device) saveLastSentDeviceIntrospection(introspection string) {
	d.lastSentIntrospection = introspection

	if d.db == nil {
		// Nothing to do
		return
	}
	d.db.Clauses(clause.OnConflict{
		UpdateAll: true,
	}).Create(&astarteDeviceStatus{DeviceId: d.deviceID, LastIntrospection: introspection})
}

func (d *Device) getLastSentDeviceIntrospection() string {
	if d.lastSentIntrospection == "" {
		if d.db == nil {
			// Nothing to do
			return ""
		}

		var status astarteDeviceStatus
		d.db.Where(&astarteDeviceStatus{DeviceId: d.deviceID}).Find(&status)

		d.lastSentIntrospection = status.LastIntrospection
	}

	return d.lastSentIntrospection
}

// GetProperty retrieves a property from the local storage, if any. It returns nil and an error in case
// no storage is available or if the property wasn't found in the local storage or if any other error
// occurred while handling it, otherwise it returns the property's value.
func (d *Device) GetProperty(interfaceName, interfacePath string) (interface{}, error) {
	if d.db == nil {
		return nil, errors.New("no db available")
	}

	var p property
	interfaceMajor := d.interfaces[interfaceName].MajorVersion
	// since we use all fields of the primary key, we're sure there will be at most one property
	result := d.db.Where(&property{InterfaceName: interfaceName, Path: interfacePath, InterfaceMajor: interfaceMajor}).Find(&p)
	if result.Error != nil {
		return nil, result.Error
	}

	return valueFromBSONPayload(p.RawValue)
}

// GetAllPropertiesForInterface retrieves all available and stored properties from the local storage, if any,
// for a given interface. It returns an empty map and an error in case no storage is available, if no properties were found
// or if any error happened while handling them, otherwise it returns a map containing all stored paths and their
// respective values.
func (d *Device) GetAllPropertiesForInterface(interfaceName string) (map[string]interface{}, error) {
	if d.db == nil {
		return map[string]interface{}{}, errors.New("no db available")
	}

	var properties []property
	interfaceMajor := d.interfaces[interfaceName].MajorVersion
	result := d.db.Where(&property{InterfaceName: interfaceName, InterfaceMajor: interfaceMajor}).Find(&properties)
	if result.Error != nil {
		return nil, result.Error
	}

	props := map[string]interface{}{}
	for _, p := range properties {
		if v, err := valueFromBSONPayload(p.RawValue); err != nil {
			return props, err
		} else {
			props[p.Path] = v
		}
	}

	return props, nil
}

// GetAllProperties retrieves all available and stored properties from the local storage, if any.
// It returns an empty map and an error in case no storage is available or if any error happened while retrieving
// the properties, otherwise it returns a multi-dimensional map containing all the available interfaces, and for
// each interface all its available paths with their respective values.
func (d *Device) GetAllProperties() (map[string]map[string]interface{}, error) {
	if d.db == nil {
		return map[string]map[string]interface{}{}, errors.New("no db available")
	}

	props := map[string]map[string]interface{}{}
	for _, p := range d.retrieveDevicePropertiesFromStorage() {
		if v, err := valueFromBSONPayload(p.RawValue); err != nil {
			return props, err
		} else {
			if _, ok := props[p.InterfaceName]; !ok {
				// Create the inner map
				props[p.InterfaceName] = map[string]interface{}{}
			}
			props[p.InterfaceName][p.Path] = v
		}
	}

	return props, nil
}
