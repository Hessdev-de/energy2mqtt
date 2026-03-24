//! Discovered Devices Store
//!
//! Persists auto-discovered devices (e.g., from Zenner Datahub) to a YAML file
//! and allows users to control which devices are exported to Home Assistant.

use std::{collections::HashMap, fs, path::PathBuf, sync::RwLock};
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
#[cfg(feature = "api")]
use utoipa::ToSchema;

/// Represents a single discovered device
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "api", derive(ToSchema))]
pub struct DiscoveredDevice {
    /// User-editable friendly name
    pub name: String,
    /// Unix timestamp when device was first seen
    pub first_seen: u64,
    /// Unix timestamp when device was last seen
    pub last_seen: u64,
    /// Auto-detected manufacturer
    #[serde(skip_serializing_if = "Option::is_none")]
    pub manufacturer: Option<String>,
    /// Auto-detected model
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Whether to export this device to Home Assistant
    #[serde(default = "default_export_to_ha")]
    pub export_to_ha: bool,
    /// Optional Home Assistant area assignment
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ha_area: Option<String>,
    /// User notes (e.g., why device is ignored)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub notes: Option<String>,
}

fn default_export_to_ha() -> bool {
    true
}

impl DiscoveredDevice {
    pub fn new(device_id: &str) -> Self {
        let now = crate::get_unix_ts();
        DiscoveredDevice {
            name: device_id.to_string(),
            first_seen: now,
            last_seen: now,
            manufacturer: None,
            model: None,
            export_to_ha: true,
            ha_area: None,
            notes: None,
        }
    }

    pub fn with_name(mut self, name: String) -> Self {
        self.name = name;
        self
    }

    pub fn with_manufacturer(mut self, manufacturer: String) -> Self {
        self.manufacturer = Some(manufacturer);
        self
    }

    pub fn with_model(mut self, model: String) -> Self {
        self.model = Some(model);
        self
    }
}

/// Update request for a discovered device (partial update)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "api", derive(ToSchema))]
pub struct DiscoveredDeviceUpdate {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub export_to_ha: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ha_area: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub notes: Option<String>,
}

/// Devices for a single instance (e.g., one Zenner Datahub connection)
pub type InstanceDevices = HashMap<String, DiscoveredDevice>;

/// All instances for a protocol
pub type ProtocolInstances = HashMap<String, InstanceDevices>;

/// The complete discovered devices store
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "api", derive(ToSchema))]
pub struct DiscoveredDevicesData {
    #[serde(default)]
    pub version: u32,
    #[serde(default)]
    pub zenner_datahub: ProtocolInstances,
    // Future protocols can be added here:
    // #[serde(default)]
    // pub lorawan_ttn: ProtocolInstances,
}

/// Thread-safe wrapper for the discovered devices store
pub struct DiscoveredDevicesStore {
    data: RwLock<DiscoveredDevicesData>,
    path: PathBuf,
    dirty: RwLock<bool>,
}

impl DiscoveredDevicesStore {
    /// Create a new store, loading from file if it exists
    pub fn new(path: PathBuf) -> Self {
        let data = Self::load_from_file(&path);
        DiscoveredDevicesStore {
            data: RwLock::new(data),
            path,
            dirty: RwLock::new(false),
        }
    }

    /// Load data from YAML file
    fn load_from_file(path: &PathBuf) -> DiscoveredDevicesData {
        if !path.exists() {
            info!("Discovered devices file not found, starting with empty store: {:?}", path);
            return DiscoveredDevicesData {
                version: 1,
                ..Default::default()
            };
        }

        match fs::read_to_string(path) {
            Ok(content) => {
                match serde_yml::from_str(&content) {
                    Ok(data) => {
                        info!("Loaded discovered devices from {:?}", path);
                        data
                    }
                    Err(e) => {
                        error!("Failed to parse discovered devices file: {}", e);
                        warn!("Starting with empty store due to parse error");
                        DiscoveredDevicesData {
                            version: 1,
                            ..Default::default()
                        }
                    }
                }
            }
            Err(e) => {
                error!("Failed to read discovered devices file: {}", e);
                DiscoveredDevicesData {
                    version: 1,
                    ..Default::default()
                }
            }
        }
    }

    /// Save data to YAML file
    pub fn save(&self) -> Result<(), String> {
        let data = self.data.read().map_err(|e| format!("Lock error: {}", e))?;

        let yaml = serde_yml::to_string(&*data)
            .map_err(|e| format!("Serialization error: {}", e))?;

        // Add header comment
        let content = format!(
            "# Discovered Devices - Auto-generated by energy2mqtt\n\
             # You can edit this file to change device names or disable HA export\n\
             # Changes will be picked up on next restart or API call\n\n{}",
            yaml
        );

        fs::write(&self.path, content)
            .map_err(|e| format!("Write error: {}", e))?;

        // Clear dirty flag
        if let Ok(mut dirty) = self.dirty.write() {
            *dirty = false;
        }

        debug!("Saved discovered devices to {:?}", self.path);
        Ok(())
    }

    /// Save if dirty
    pub fn save_if_dirty(&self) -> Result<(), String> {
        let is_dirty = self.dirty.read().map(|d| *d).unwrap_or(false);
        if is_dirty {
            self.save()
        } else {
            Ok(())
        }
    }

    /// Mark the store as dirty (needs saving)
    fn mark_dirty(&self) {
        if let Ok(mut dirty) = self.dirty.write() {
            *dirty = true;
        }
    }

    /// Check if a device should be exported to Home Assistant
    pub fn should_export(&self, protocol: &str, instance: &str, device_id: &str) -> bool {
        let data = match self.data.read() {
            Ok(d) => d,
            Err(_) => return true, // Default to export on lock error
        };

        let instances = match protocol {
            "zenner_datahub" => &data.zenner_datahub,
            _ => return true, // Unknown protocol, default to export
        };

        instances
            .get(instance)
            .and_then(|devices| devices.get(device_id))
            .map(|device| device.export_to_ha)
            .unwrap_or(true) // Not found = new device = export by default
    }

    /// Get a device if it exists
    pub fn get_device(&self, protocol: &str, instance: &str, device_id: &str) -> Option<DiscoveredDevice> {
        let data = self.data.read().ok()?;

        let instances = match protocol {
            "zenner_datahub" => &data.zenner_datahub,
            _ => return None,
        };

        instances
            .get(instance)
            .and_then(|devices| devices.get(device_id))
            .cloned()
    }

    /// Get the friendly name for a device (or device_id if not set)
    pub fn get_display_name(&self, protocol: &str, instance: &str, device_id: &str) -> String {
        self.get_device(protocol, instance, device_id)
            .map(|d| d.name)
            .unwrap_or_else(|| device_id.to_string())
    }

    /// Add or update a device
    pub fn upsert_device(
        &self,
        protocol: &str,
        instance: &str,
        device_id: &str,
        device: DiscoveredDevice,
    ) -> Result<(), String> {
        let mut data = self.data.write().map_err(|e| format!("Lock error: {}", e))?;

        let instances = match protocol {
            "zenner_datahub" => &mut data.zenner_datahub,
            _ => return Err(format!("Unknown protocol: {}", protocol)),
        };

        let devices = instances.entry(instance.to_string()).or_insert_with(HashMap::new);
        devices.insert(device_id.to_string(), device);

        drop(data);
        self.mark_dirty();

        Ok(())
    }

    /// Update last_seen timestamp for a device, creating it if it doesn't exist
    pub fn touch_device(
        &self,
        protocol: &str,
        instance: &str,
        device_id: &str,
        manufacturer: Option<String>,
        model: Option<String>,
    ) -> Result<DiscoveredDevice, String> {
        let mut data = self.data.write().map_err(|e| format!("Lock error: {}", e))?;
        let now = crate::get_unix_ts();

        let instances = match protocol {
            "zenner_datahub" => &mut data.zenner_datahub,
            _ => return Err(format!("Unknown protocol: {}", protocol)),
        };

        let devices = instances.entry(instance.to_string()).or_insert_with(HashMap::new);

        let device = devices.entry(device_id.to_string()).or_insert_with(|| {
            info!("New device discovered: {} / {} / {}", protocol, instance, device_id);
            DiscoveredDevice::new(device_id)
        });

        // Update last_seen
        device.last_seen = now;

        // Update manufacturer/model if provided and not already set
        if device.manufacturer.is_none() {
            device.manufacturer = manufacturer;
        }
        if device.model.is_none() {
            device.model = model;
        }

        let result = device.clone();
        drop(data);
        self.mark_dirty();

        Ok(result)
    }

    /// Update specific fields of a device
    pub fn update_device(
        &self,
        protocol: &str,
        instance: &str,
        device_id: &str,
        update: DiscoveredDeviceUpdate,
    ) -> Result<DiscoveredDevice, String> {
        let mut data = self.data.write().map_err(|e| format!("Lock error: {}", e))?;

        let instances = match protocol {
            "zenner_datahub" => &mut data.zenner_datahub,
            _ => return Err(format!("Unknown protocol: {}", protocol)),
        };

        let devices = instances
            .get_mut(instance)
            .ok_or_else(|| format!("Instance not found: {}", instance))?;

        let device = devices
            .get_mut(device_id)
            .ok_or_else(|| format!("Device not found: {}", device_id))?;

        // Apply updates
        if let Some(name) = update.name {
            device.name = name;
        }
        if let Some(export) = update.export_to_ha {
            device.export_to_ha = export;
        }
        if update.ha_area.is_some() {
            device.ha_area = update.ha_area;
        }
        if update.notes.is_some() {
            device.notes = update.notes;
        }

        let result = device.clone();
        drop(data);
        self.mark_dirty();

        Ok(result)
    }

    /// Delete/forget a device
    pub fn delete_device(
        &self,
        protocol: &str,
        instance: &str,
        device_id: &str,
    ) -> Result<(), String> {
        let mut data = self.data.write().map_err(|e| format!("Lock error: {}", e))?;

        let instances = match protocol {
            "zenner_datahub" => &mut data.zenner_datahub,
            _ => return Err(format!("Unknown protocol: {}", protocol)),
        };

        if let Some(devices) = instances.get_mut(instance) {
            devices.remove(device_id);

            // Clean up empty instance
            if devices.is_empty() {
                instances.remove(instance);
            }
        }

        drop(data);
        self.mark_dirty();

        Ok(())
    }

    /// Count devices for a protocol (optionally filtered by instance)
    pub fn count_devices(&self, protocol: &str, instance: Option<&str>) -> usize {
        let data = match self.data.read() {
            Ok(d) => d,
            Err(_) => return 0,
        };

        let instances = match protocol {
            "zenner_datahub" => &data.zenner_datahub,
            _ => return 0,
        };

        match instance {
            Some(inst) => instances.get(inst).map(|d| d.len()).unwrap_or(0),
            None => instances.values().map(|d| d.len()).sum(),
        }
    }

    /// Get all devices for a protocol
    pub fn get_all_devices(&self, protocol: &str) -> HashMap<String, HashMap<String, DiscoveredDevice>> {
        let data = match self.data.read() {
            Ok(d) => d,
            Err(_) => return HashMap::new(),
        };

        match protocol {
            "zenner_datahub" => data.zenner_datahub.clone(),
            _ => HashMap::new(),
        }
    }

    /// Get all devices for a specific instance
    pub fn get_instance_devices(&self, protocol: &str, instance: &str) -> HashMap<String, DiscoveredDevice> {
        let data = match self.data.read() {
            Ok(d) => d,
            Err(_) => return HashMap::new(),
        };

        let instances = match protocol {
            "zenner_datahub" => &data.zenner_datahub,
            _ => return HashMap::new(),
        };

        instances.get(instance).cloned().unwrap_or_default()
    }

    /// Get summary of all protocols and their device counts
    pub fn get_summary(&self) -> HashMap<String, usize> {
        let data = match self.data.read() {
            Ok(d) => d,
            Err(_) => return HashMap::new(),
        };

        let mut summary = HashMap::new();

        let zenner_count: usize = data.zenner_datahub.values().map(|d| d.len()).sum();
        if zenner_count > 0 {
            summary.insert("zenner_datahub".to_string(), zenner_count);
        }

        summary
    }
}

// Global instance
use std::sync::OnceLock;
static DISCOVERED_DEVICES: OnceLock<DiscoveredDevicesStore> = OnceLock::new();

/// Initialize the global discovered devices store
pub fn init_discovered_devices(path: PathBuf) {
    let store = DiscoveredDevicesStore::new(path);
    if DISCOVERED_DEVICES.set(store).is_err() {
        warn!("Discovered devices store already initialized");
    }
}

/// Get the global discovered devices store
pub fn get_discovered_devices() -> Option<&'static DiscoveredDevicesStore> {
    DISCOVERED_DEVICES.get()
}

/// Convenience macro to get the store or return early
#[macro_export]
macro_rules! get_discovered_devices_store {
    () => {
        match $crate::discovered_devices::get_discovered_devices() {
            Some(store) => store,
            None => {
                log::warn!("Discovered devices store not initialized");
                return;
            }
        }
    };
}
