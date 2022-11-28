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

package example

import (
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/astarte-platform/astarte-device-sdk-go/device"
	"github.com/astarte-platform/astarte-go/interfaces"
	"github.com/astarte-platform/astarte-go/misc"
)

// ExecuteBasicDevice is the most minimal example of Astarte Go SDK usage. It initializes a device,
// loads an interface, and sends a message
func ExecuteBasicDevice() {
	// These are random values - ensure you put the right ones
	uuidNamespace := "e9b561df-a5b0-4e71-9cf1-402a37463bea"
	objectID := "00112233445566"
	credentialsSecret := "something"
	deviceRealm := "demo"
	apiEndpoint := "https://api.some.astarte.com/pairing"
	persistencyDir := "persistency/"

	// Compute device ID
	deviceID, err := misc.GetNamespacedAstarteDeviceID(uuidNamespace, []byte(objectID))
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	// Create device
	d, err := device.NewDeviceWithPersistency(deviceID, deviceRealm, credentialsSecret, apiEndpoint, persistencyDir)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	// Load interface - fix this path(s) to load the right interface
	byteValue, err := ioutil.ReadFile("org.astarte-platform.genericsensors.Values.json")
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	iface := interfaces.AstarteInterface{}
	if iface, err = interfaces.ParseInterface(byteValue); err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	if err = d.SafeAddInterface(iface); err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	// Set up device callbacks
	d.OnConnectionStateChanged = func(d *device.Device, state bool) {
		fmt.Printf("Device connection state: %t\n", state)
	}
	d.OnIndividualMessageReceived = func(d *device.Device, message device.IndividualMessage) {
		fmt.Printf("Individual message received: %v\n", message)
	}
	d.OnAggregateMessageReceived = func(d *device.Device, message device.AggregateMessage) {
		fmt.Printf("Aggregate message received: %v\n", message)
	}
	d.OnErrors = func(d *device.Device, err error) {
		fmt.Printf("Received error: %s\n", err.Error())
	}

	// Connect the device and listen to the connection status channel
	c := make(chan error)
	d.Connect(c)
	if err := <-c; err == nil {
		fmt.Println("Connected successfully")
	} else {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	// Send a message and finish
	d.SendIndividualMessageWithTimestamp("org.astarte-platform.genericsensors.Values", "/test/value", 45.3, time.Now())
	time.Sleep(2 * time.Second)
}
