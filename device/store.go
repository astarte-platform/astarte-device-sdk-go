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
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type astarteMessageInfo struct {
	StorageId      int `gorm:"primaryKey;autoIncrement;not null"`
	Retention      interfaces.AstarteMappingRetention
	AbsoluteExpiry int64  `gorm:"not null"`
	Topic          string `gorm:"not null"`
	Qos            uint8  `gorm:"not null"`
	Payload        []byte `gorm:"not null"`
}

type property struct {
	Path            string `gorm:"primaryKey;not null"`
	Interface_major int    `gorm:"primaryKey;not null"`
	Value           []byte `gorm:"not null"`
}

func (d *Device) getDbDir() string {
	dbDir := filepath.Join(d.persistencyDir, "db")
	os.MkdirAll(dbDir, 0700)
	return dbDir
}

func OpenDB(filepath string) (*gorm.DB, error) {
	os.MkdirAll(filepath, 0700)
	db, err := gorm.Open(sqlite.Open("test.db"), &gorm.Config{})
	if err != nil {
		fmt.Println("failed to connect database")
		return nil, err
	}
	return db, nil
}

func initializeDb(d *Device) error {
	d.db.AutoMigrate(&astarteMessageInfo{}, &property{})
	// initPropertiesStmt := `
	// CREATE TABLE IF NOT EXISTS persistent_properties (path TEXT NOT NULL, value BLOB NOT NULL, interface_major INTEGER NOT NULL, PRIMARY KEY (path, interface_major));
	// `
	// if _, err := d.db.Exec(initPropertiesStmt); err != nil {
	// 	log.Printf("%q: %s\n", err, initPropertiesStmt)
	// 	return err
	// }

	// initFailedMessagestmt := `
	// CREATE TABLE IF NOT EXISTS failed_messages (storageId INTEGER NOT NULL PRIMARY KEY, absoluteExpiry INTEGER NOT NULL, topic TEXT NOT NULL, payload BLOB NOT NULL, qos INTEGER NOT NULL);
	// `
	// if _, err := d.db.Exec(initFailedMessagestmt); err != nil {
	// 	log.Printf("%q: %s\n", err, initFailedMessagestmt)
	// 	return err
	// }
	return nil
}

func makeAstarteMessageInfo(expiry int, retention interfaces.AstarteMappingRetention, topic string, qos uint8, payload []byte) astarteMessageInfo {
	var absoluteExpiry int64 = 0
	if expiry != 0 {
		absoluteExpiry = time.Now().Unix() + int64(expiry)
	}
	return astarteMessageInfo{AbsoluteExpiry: absoluteExpiry, Retention: retention, Topic: topic, Qos: qos, Payload: payload}
}

func (d *Device) storeFailedMessage(storageId int, absoluteExpiry int64, topic string, payload []byte, qos uint8) error {
	// insertStmt, err := d.db.Prepare("INSERT INTO failed_messages(absoluteExpiry, topic, payload, qos) values(?,?,?,?);")
	// if err != nil {
	// 	return err
	// }
	// defer insertStmt.Close()
	// _, err = insertStmt.Exec(absoluteExpiry, topic, payload, qos)
	// if err != nil {
	// 	return err
	// }
	// return nil
	d.db.Create(&astarteMessageInfo{StorageId: storageId, AbsoluteExpiry: absoluteExpiry, Topic: topic, Payload: payload, Qos: qos})
	return nil
}

func (d *Device) removeFailedMessage(storageId int) error {
	// deleteStmt, err := d.db.Prepare("DELETE FROM failed_messages WHERE storageId=?;")
	// if err != nil {
	// 	log.Printf("%q: %s\n", err, deleteStmt)

	// }
	// defer deleteStmt.Close()
	// result, err := deleteStmt.Exec(storageId)
	// if rowsaffected, err := result.RowsAffected(); err == nil {
	// 	fmt.Printf("Rows affected: %d\n", rowsaffected)
	// }
	// if err != nil {
	// 	log.Printf("%q: %s\n", err, deleteStmt)
	// }
	d.db.Delete(&storageId)
	return nil
}

func (d *Device) retrieveStoredMessages() error {
	// rows, err := d.db.Query("SELECT * FROM failed_messages;")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// defer rows.Close()
	// for rows.Next() {
	// 	var storageId int
	// 	var absoluteExpiry int64
	// 	var topic string
	// 	var qos uint8
	// 	var payload []byte
	// 	err = rows.Scan(&storageId, &absoluteExpiry, &topic, &payload, &qos)
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}
	// 	next := AstarteMessageInfo{storageId: storageId, absoluteExpiry: absoluteExpiry, retention: interfaces.StoredRetention, topic: topic, qos: qos, payload: payload}
	// 	if !isExpired(next) {
	// 		d.messageQueue <- next
	// 	} else {
	// 		// TODO put expiry control in a goroutine
	// 		d.removeFailedMessage(storageId)
	// 	}
	// }
	// if err := rows.Err(); err != nil {
	// 	log.Fatal(err)
	// }
	var messages []astarteMessageInfo
	d.db.Find(&messages)
	for _, message := range messages {
		if !isExpired(message) {
			d.messageQueue <- message
			d.db.Delete(&message.StorageId)
		}
	}
	return nil
}

func (d *Device) retrieveVolatileMessages() error {
	for len(d.volatileMessages) > 0 {
		next := d.volatileMessages[0]
		d.volatileMessages = d.volatileMessages[1:]
		if !isExpired(next) {
			d.messageQueue <- next
		}
	}
	return nil
}

func isExpired(message astarteMessageInfo) bool {
	return message.AbsoluteExpiry > time.Now().Unix() || message.AbsoluteExpiry == 0
}

func (d *Device) storeProperty(path string, value []byte, interface_major int) error {
	// insertStmt, err := d.db.Prepare("INSERT OR REPLACE INTO persistent_properties(path, value, interface_major) values(?,?,?);")
	// if err != nil {
	// 	log.Printf("%q: %s\n", err, insertStmt)
	// }
	// defer insertStmt.Close()
	// _, err = insertStmt.Exec(path, value, interface_major)
	// if err != nil {
	// 	log.Printf("%q: %s\n", err, insertStmt)
	// }
	d.db.Create(&property{Path: path, Interface_major: interface_major, Value: value})
	return nil
}

func (d *Device) deleteProperty(path string, value []byte, interface_major int) error {
	// insertStmt, err := d.db.Prepare("DELETE FROM persistent_properties 	WHERE path=? AND value=? AND interface_major=?;")
	// if err != nil {
	// 	log.Printf("%q: %s\n", err, insertStmt)
	// }
	// defer insertStmt.Close()
	// _, err = insertStmt.Exec(path, value, interface_major)
	// if err != nil {
	// 	log.Printf("%q: %s\n", err, insertStmt)
	// }
	d.db.Delete(&path, &interface_major)
	return nil
}
