package device

import (
	"context"
	"fmt"
	"time"

	"github.com/hashicorp/nomad/plugins/device"
)

// doFingerprint is the long-running goroutine that detects device changes
func (d *GenericDevicePlugin) doFingerprint(ctx context.Context, devices chan *device.FingerprintResponse) {
	defer close(devices)

	// Create a timer that will fire immediately for the first detection
	ticker := time.NewTimer(0)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ticker.Reset(d.fingerprintPeriod)
		}

		d.writeFingerprintToChannel(devices)
	}
}

// fingerprintedDevice is what we "discover" and transform into device.Device objects.
//
// plugin implementations will likely have a native struct provided by the corresonding SDK
type fingerprintedDevice struct {
	ID     string
	device GenericDevice
}

// writeFingerprintToChannel collects fingerprint info, partitions devices into
// device groups, and sends the data over the provided channel.
func (d *GenericDevicePlugin) writeFingerprintToChannel(devices chan<- *device.FingerprintResponse) {
	d.deviceLock.Lock()
	defer d.deviceLock.Unlock()

	if len(d.identifiedDevices) == 0 {
		// "discover" the devices we have configured
		discoveredDevices := make([]*fingerprintedDevice, 0)

		for _, configuredDevice := range d.configuredDevices {
			count := configuredDevice.Count
			if count == 0 {
				count = 1
			}
			for deviceIndex := 0; deviceIndex < count; deviceIndex++ {
				discoveredDevices = append(discoveredDevices, &fingerprintedDevice{
					ID: fmt.Sprintf("%s/%s/%s/%d", configuredDevice.Type, configuredDevice.Vendor, configuredDevice.Model, deviceIndex),
					device: GenericDevice{
						Type:   configuredDevice.Type,
						Vendor: configuredDevice.Vendor,
						Model:  configuredDevice.Model,
					},
				})
			}
		}

		d.logger.Info("Found devices", "count", len(discoveredDevices))

		// during fingerprinting, devices are grouped by "device group" in
		// order to facilitate scheduling
		// devices in the same device group should have the same
		// Vendor, Type, and Name ("Model")
		// Build Fingerprint response with computed groups and send it over the channel
		deviceListByDeviceName := make(map[string][]*fingerprintedDevice)
		for _, device := range discoveredDevices {
			deviceName := device.device.Model
			deviceListByDeviceName[deviceName] = append(deviceListByDeviceName[deviceName], device)
			d.identifiedDevices[device.ID] = device.device
		}

		// Build Fingerprint response with computed groups and send it over the channel
		deviceGroups := make([]*device.DeviceGroup, 0, len(deviceListByDeviceName))
		for groupName, devices := range deviceListByDeviceName {
			deviceGroups = append(deviceGroups, deviceGroupFromFingerprintData(groupName, devices))
		}

		devices <- device.NewFingerprint(deviceGroups...)
	}
}

// deviceGroupFromFingerprintData composes deviceGroup from a slice of detected devices
func deviceGroupFromFingerprintData(groupName string, deviceList []*fingerprintedDevice) *device.DeviceGroup {
	// deviceGroup without devices makes no sense -> return nil when no devices are provided
	if len(deviceList) == 0 {
		return nil
	}

	devices := make([]*device.Device, 0, len(deviceList))
	for _, dev := range deviceList {
		devices = append(devices, &device.Device{
			ID:         dev.ID,
			Healthy:    true,
			HwLocality: nil,
		})
	}

	deviceGroup := &device.DeviceGroup{
		// TODO: is this a valid assumption?
		Vendor: deviceList[0].device.Vendor,
		// TODO: is this a valid assumption?
		Type: deviceList[0].device.Type,

		Name:    groupName,
		Devices: devices,
		// The device API assumes that devices with the same DeviceName have the same
		// attributes like amount of memory, power, bar1memory, etc.
		// If not, then they'll need to be split into different device groups
		// with different names.
		/*
			Attributes: map[string]*structs.Attribute{
				"attrA": {
					Int:  helper.Int64ToPtr(1024),
					Unit: "MB",
				},
				"attrB": {
					Float: helper.Float64ToPtr(10.5),
					Unit:  "MW",
				},
			},
		*/
	}
	return deviceGroup
}
