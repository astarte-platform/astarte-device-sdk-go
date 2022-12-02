// Copyright © 2022 Ispirata Srl
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

package e2e

import (
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"math"
	"net/url"
	"os"
	"path"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/astarte-platform/astarte-go/client"
	"github.com/astarte-platform/astarte-go/interfaces"

	"github.com/astarte-platform/astarte-device-sdk-go/device"
)

//nolint:dupl
var (
	expectedDatastreamIndividual = map[string]interface{}{
		"/test/binaryblob":       []byte{1, 2, 3, 4, 5},
		"/test/binaryblobarray":  [][]byte{{12, 23, 34, 45, 56}, {1, 2, 3, 4, 5}},
		"/test/boolean":          true,
		"/test/booleanarray":     []bool{true, false, true},
		"/test/datetime":         time.Date(1996, time.April, 30, 0, 0, 0, 0, time.UTC),
		"/test/datetimearray":    []time.Time{time.Date(1996, time.April, 30, 0, 0, 0, 0, time.UTC), time.Date(1996, time.April, 30, 0, 0, 0, 0, time.UTC)},
		"/test/double":           1.1,
		"/test/doublearray":      []float64{1.1, 2.0, 3.3, 4.5},
		"/test/integer":          2,
		"/test/integerarray":     []int{1, 2, 3},
		"/test/longinteger":      int64(math.MaxInt32 + 1),
		"/test/longintegerarray": []int64{math.MaxInt32 + 1, math.MaxInt32 + 2},
		"/test/string":           "hello",
		"/test/stringarray":      []string{"hello", "world"},
	}
	expectedDatastreamObject = map[string]interface{}{
		"binaryblob":       []byte{1, 2, 3, 4, 5},
		"binaryblobarray":  [][]byte{{12, 23, 34, 45, 56}, {1, 2, 3, 4, 5}},
		"boolean":          true,
		"booleanarray":     []bool{true, false, true},
		"datetime":         time.Date(1996, time.April, 30, 0, 0, 0, 0, time.UTC),
		"datetimearray":    []time.Time{time.Date(1996, time.April, 30, 0, 0, 0, 0, time.UTC), time.Date(1996, time.April, 30, 0, 0, 0, 0, time.UTC)},
		"double":           1.1,
		"doublearray":      []float64{1.1, 2.0, 3.3, 4.5},
		"integer":          2,
		"integerarray":     []int{1, 2, 3},
		"longinteger":      int64(math.MaxInt32 + 1),
		"longintegerarray": []int64{math.MaxInt32 + 1, math.MaxInt32 + 2},
		"string":           "hello",
		"stringarray":      []string{"hello", "world"},
	}
	expectedProperties = map[string]interface{}{
		"/test/binaryblob":       []byte{1, 2, 3, 4, 5},
		"/test/binaryblobarray":  [][]byte{{12, 23, 34, 45, 56}, {1, 2, 3, 4, 5}},
		"/test/boolean":          true,
		"/test/booleanarray":     []bool{true, false, true},
		"/test/datetime":         time.Date(1996, time.April, 30, 0, 0, 0, 0, time.UTC),
		"/test/datetimearray":    []time.Time{time.Date(1996, time.April, 30, 0, 0, 0, 0, time.UTC), time.Date(1996, time.April, 30, 0, 0, 0, 0, time.UTC)},
		"/test/double":           1.1,
		"/test/doublearray":      []float64{1.1, 2.0, 3.3, 4.5},
		"/test/integer":          2,
		"/test/integerarray":     []int{1, 2, 3},
		"/test/longinteger":      int64(math.MaxInt32 + 1),
		"/test/longintegerarray": []int64{math.MaxInt32 + 1, math.MaxInt32 + 2},
		"/test/string":           "hello",
		"/test/stringarray":      []string{"hello", "world"},
	}
)

type EndToEndSuite struct {
	suite.Suite
	astarteAPIEndpoint   string
	interfaceDirectory   string
	jwt                  string
	realm                string
	deviceID             string
	credentialsSecret    string
	devicePersistencyDir string
	astarteAPIClient     *client.Client
	d                    *device.Device
}

func (suite *EndToEndSuite) SetupSuite() {
	suite.astarteAPIEndpoint = os.Getenv("E2E_ASTARTE_URL")
	suite.jwt = os.Getenv("E2E_JWT")
	suite.realm = os.Getenv("E2E_REALM")
	suite.deviceID = os.Getenv("E2E_DEVICE_ID")
	suite.credentialsSecret = os.Getenv("E2E_CREDENTIALS_SECRET")
	suite.devicePersistencyDir, _ = ioutil.TempDir("", "astarte2e")
	suite.interfaceDirectory = os.Getenv("E2E_INTERFACES_DIR")

	suite.setupAPIClient()
	suite.setupDevice()
}

func (suite *EndToEndSuite) TearDownSuite() {
	suite.d.Disconnect(make(chan<- error))
	time.Sleep(3 * time.Second)
	os.RemoveAll(suite.devicePersistencyDir)
}

func (suite *EndToEndSuite) TestDatastreamIndividualDevice() {
	// send everything
	for k, v := range expectedDatastreamIndividual {
		if err := suite.d.SendIndividualMessageWithTimestamp("org.astarte-platform.device.individual.datastream.Everything", k, v, time.Now()); err != nil {
			suite.Fail("Error sending individual message", err)
		}
		fmt.Printf("Sent %v on %s\n", v, k)
		time.Sleep(1 * time.Second)
	}

	res, err := suite.astarteAPIClient.AppEngine.GetDatastreamSnapshot(suite.realm, suite.deviceID, client.AstarteDeviceID, "org.astarte-platform.device.individual.datastream.Everything")
	if err != nil {
		suite.Fail("Error querying Astarte", err)
	}

	received := map[string]interface{}{}
	for k, v := range res {
		astarteType := strings.Split(k, "/")[2]
		received[k] = individualValueToAstarteType(v.Value, astarteType)
	}
	for path, expectedValue := range expectedDatastreamIndividual {
		if !suite.Equal(expectedValue, received[path]) {
			fmt.Printf("Expected: %v : %v  ---- received %v : %v\n",
				expectedValue, reflect.TypeOf(expectedValue),
				received[path], reflect.TypeOf(received[path]))
			suite.Fail("Sent value different from received value", expectedValue, received[path])
		}
	}
}

func (suite *EndToEndSuite) TestDatastreamObjectDevice() {
	data := expectedDatastreamObject
	// send everything
	if err := suite.d.SendAggregateMessageWithTimestamp("org.astarte-platform.device.object.datastream.Everything", "/test", data, time.Now()); err != nil {
		suite.Fail("Error sending aggregate message", err)
	}
	fmt.Printf("Sent %v on %s\n", data, "/test")
	time.Sleep(1 * time.Second)

	res, err := suite.astarteAPIClient.AppEngine.GetAggregateParametricDatastreamSnapshot(suite.realm, suite.deviceID, client.AstarteDeviceID, "org.astarte-platform.device.object.datastream.Everything")
	if err != nil {
		suite.Fail("Error querying Astarte", err)
	}
	result := res["/test"].Values

	received := map[string]interface{}{}
	for _, k := range result.Keys() {
		value, _ := result.Get(k)
		received[k] = individualValueToAstarteType(value, k)
	}
	for path, expectedValue := range expectedDatastreamObject {
		if !suite.Equal(expectedValue, received[path]) {
			fmt.Printf("Expected: %v : %v  ---- received %v : %v\n",
				expectedValue, reflect.TypeOf(expectedValue),
				received[path], reflect.TypeOf(received[path]))
			suite.Fail("Sent value different from received value", expectedValue, received[path])
		}
	}
}

func (suite *EndToEndSuite) TestPropertiesIndividualDevice() {
	// set everything
	for k, v := range expectedProperties {
		if err := suite.d.SetProperty("org.astarte-platform.device.individual.properties.Everything", k, v); err != nil {
			suite.Fail("Error setting property", err)
		}
		fmt.Printf("Set %v on %s\n", v, k)
		time.Sleep(1 * time.Second)
	}

	res, err := suite.astarteAPIClient.AppEngine.GetProperties(suite.realm, suite.deviceID, client.AstarteDeviceID, "org.astarte-platform.device.individual.properties.Everything")
	if err != nil {
		suite.Fail("Error querying Astarte", err)
	}

	received := map[string]interface{}{}
	for k, v := range res {
		astarteType := strings.Split(k, "/")[2]
		received[k] = individualValueToAstarteType(v, astarteType)
	}
	for path, expectedValue := range expectedProperties {
		if !suite.Equal(expectedValue, received[path]) {
			fmt.Printf("Expected: %v : %v  ---- received %v : %v\n",
				expectedValue, reflect.TypeOf(expectedValue),
				received[path], reflect.TypeOf(received[path]))
			suite.Fail("Sent value different from received value", expectedValue, received[path])
		}
	}

	// unset everything
	for k := range expectedProperties {
		if err := suite.d.UnsetProperty("org.astarte-platform.device.individual.properties.Everything", k); err != nil {
			suite.Fail("Error unsetting property", err)
		}
		fmt.Printf("Unset %s\n", k)
		time.Sleep(1 * time.Second)
	}

	res2, err2 := suite.astarteAPIClient.AppEngine.GetProperties(suite.realm, suite.deviceID, client.AstarteDeviceID, "org.astarte-platform.device.individual.properties.Everything")
	if err2 != nil {
		suite.Fail("Error querying Astarte", err2)
	}
	suite.Empty(res2, "Properties not unset")
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestEndToEndSuite(t *testing.T) {
	suite.Run(t, new(EndToEndSuite))
}

func (suite *EndToEndSuite) setupAPIClient() {
	// Create astarte API client
	astarteAPIClient, err := client.NewClient(suite.astarteAPIEndpoint, nil)

	if err != nil {
		suite.FailNow("Cannot setup API client", err)
	}
	// Use a given token for auth
	astarteAPIClient.SetToken(suite.jwt)
	suite.astarteAPIClient = astarteAPIClient
}

func (suite *EndToEndSuite) setupDevice() {
	// Some Go spin on URLs
	// We're ignoring errors here as the cross-parsing cannot fail.
	astarteURL, err := url.Parse(suite.astarteAPIEndpoint)
	if err != nil {
		suite.FailNow("Cannot parse Astarte API endpoint URL", err)
	}
	pairingURL, _ := url.Parse(astarteURL.String())
	pairingURL.Path = path.Join(pairingURL.Path, "pairing")

	// Create device
	d, err := device.NewDeviceWithPersistency(suite.deviceID, suite.realm, suite.credentialsSecret, pairingURL.String(), suite.devicePersistencyDir)
	if err != nil {
		suite.FailNow("Cannot setup device", err)
	}

	if err = loadInterfaces(d, suite.interfaceDirectory); err != nil {
		suite.FailNow("Error loading interfaces", err)
	}

	// Set up device options and callbacks
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
		fmt.Printf("Received error: %s by device %s \n", err.Error(), suite.deviceID)
	}

	// Connect the device and listen to the connection status channel
	c := make(chan error)
	d.Connect(c)
	if err := <-c; err == nil {
		fmt.Println("Connected successfully")
	} else {
		suite.FailNow("Error connecting device to Astarte", err)
	}

	// wait for routine initialization
	time.Sleep(2 * time.Second)
	suite.d = d
}

//nolint
func individualValueToAstarteType(value interface{}, astarteType string) interface{} {
	// cast like there's no tomorrow yolo
	switch astarteType {
	case "datetime":
		return toDate(value)
	case "datetimearray":
		n := []time.Time{}
		for _, v := range value.([]interface{}) {
			n = append(n, toDate(v))
		}
		return n
	case "integer":
		return int(value.(float64))
	case "integerarray":
		n := []int{}
		for _, v := range value.([]interface{}) {
			n = append(n, int(v.(float64)))
		}
		return n
	case "double":
		return value.(float64)
	case "doublearray":
		n := []float64{}
		for _, v := range value.([]interface{}) {
			n = append(n, v.(float64))
		}
		return n
	case "longinteger":
		return toLongInteger(value)
	case "longintegerarray":
		n := []int64{}
		for _, v := range value.([]interface{}) {
			n = append(n, toLongInteger(v))
		}
		return n
	case "boolean":
		return value.(bool)
	case "booleanarray":
		n := []bool{}
		for _, v := range value.([]interface{}) {
			n = append(n, v.(bool))
		}
		return n
	case "string":
		return value.(string)
	case "stringarray":
		n := []string{}
		for _, v := range value.([]interface{}) {
			n = append(n, v.(string))
		}
		return n
	case "binaryblob":
		n, _ := base64.StdEncoding.DecodeString(value.(string))
		return n
	case "binaryblobarray":
		n := [][]byte{}
		for _, v := range value.([]interface{}) {
			decoded, _ := base64.StdEncoding.DecodeString(v.(string))
			n = append(n, decoded)
		}
		return n
	default:
		// can't happen because we checked all astarte types
		return nil
	}
}

func toDate(value interface{}) time.Time {
	date, _ := time.ParseInLocation(time.RFC3339, fmt.Sprintf("%s", value), time.UTC)
	return date
}

func toLongInteger(value interface{}) int64 {
	switch v := value.(type) {
	case string:
		n, _ := strconv.ParseInt(v, 10, 64)
		return n
	// Assuming that, if it is not a string, it can be casted to a float64
	default:
		return int64(v.(float64))
	}
}

func loadInterfaces(d *device.Device, interfaceDirectory string) error {
	files, _ := ioutil.ReadDir(interfaceDirectory)
	var err error
	for _, f := range files {
		fmt.Println(f.Name())
		var iface interfaces.AstarteInterface
		if iface, err = interfaces.ParseInterfaceFromFile(interfaceDirectory + "/" + f.Name()); err != nil {
			return err
		}
		if err = d.SafeAddInterface(iface); err != nil {
			return err
		}
	}
	return nil
}
