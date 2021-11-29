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
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/astarte-platform/astarte-go/interfaces"
	"gorm.io/gorm/clause"
)

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

func (d *Device) getDbDir() string {
	dbDir := filepath.Join(d.persistencyDir, "db")
	os.MkdirAll(dbDir, 0700)
	return dbDir
}

func (d *Device) migrateDb() error {
	if err := d.db.AutoMigrate(&astarteMessageInfo{}, &property{}); err != nil {
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
	// Gorm creates a (autoincrementing) StorageId for us if it is 0: thank you gorm!
	d.db.Create(&message)
}

func (d *Device) removeFailedMessage(storageId int) {
	d.db.Delete(&astarteMessageInfo{}, storageId)
}

func (d *Device) resendStoredMessages() {
	var messages []astarteMessageInfo
	d.db.Find(&messages)
	for _, message := range messages {
		if !isExpired(message) && !d.isOutdated(message.InterfaceName, message.InterfaceMajor) {
			// if the message is not expired, try resending it
			d.messageQueue <- message
		} else {
			// else, it can be removed
			d.removeFailedMessage(message.StorageId)
		}
	}
}

func (d *Device) resendVolatileMessages() {
	for len(d.volatileMessages) > 0 {
		message := d.volatileMessages[0]
		d.volatileMessages = d.volatileMessages[1:]
		// try resending the message only if it is not expired
		if !isExpired(message) && !d.isOutdated(message.InterfaceName, message.InterfaceMajor) {
			d.messageQueue <- message
		}
	}
}

func isExpired(message astarteMessageInfo) bool {
	return message.AbsoluteExpiry <= time.Now().Unix() && message.AbsoluteExpiry != 0
}

func (d *Device) isOutdated(interfaceName string, interfaceMajor int) bool {
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
	d.db.Clauses(clause.OnConflict{
		UpdateAll: true,
	}).Create(&property{InterfaceName: interfaceName, Path: path, InterfaceMajor: interfaceMajor, RawValue: value})
}

func (d *Device) deleteProperty(interfaceName string, path string, interfaceMajor int) {
	d.db.Delete(&interfaceName, &path, &interfaceMajor)
}

func (d *Device) retrieveDeviceProperties() []property {
	var properties []property
	d.db.Find(&properties)
	upToDate := []property{}
	for _, property := range properties {
		if !d.isOutdated(property.InterfaceName, property.InterfaceMajor) {
			// do not send an outdated property
			// we can safely assume that properties is not a big collection
			upToDate = append(upToDate, property)

		}
		// TODO: cleanup outdated properties
	}
	return upToDate
}
