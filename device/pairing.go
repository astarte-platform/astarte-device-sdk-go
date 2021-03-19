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
	"strings"
)

func (d *Device) ensureCertificate() error {
	// Ensure all crypto routines.
	if err := d.ensureKeyPair(); err != nil {
		return err
	}
	if err := d.ensureCSR(); err != nil {
		return err
	}
	// Do we have a certificate already?
	if d.hasValidCertificate() {
		// We are good to go!
		return nil
	}

	// Obtain the certificate
	return d.obtainNewCertificate()
}

func (d *Device) obtainNewCertificate() error {
	csrString, err := d.getCSRString()
	if err != nil {
		return err
	}

	cert, err := d.astarteAPIClient.Pairing.ObtainNewMQTTv1CertificateForDevice(d.realm, d.deviceID, csrString)
	if err != nil {
		return err
	}

	return d.saveCertificateFromString(cert)
}

func (d *Device) ensureBrokerURL() error {
	info, err := d.astarteAPIClient.Pairing.GetMQTTv1ProtocolInformationForDevice(d.realm, d.deviceID)
	if err != nil {
		return err
	}

	d.brokerURL = strings.Replace(info.BrokerURL, "mqtts", "ssl", 1)
	return nil
}
