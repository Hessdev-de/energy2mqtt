/**
 * Energy2MQTT UI Components
 * Reusable components for rendering adapters and devices
 */

// =========================================================================
// Utility Functions
// =========================================================================

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

function formatUptime(seconds) {
    if (!seconds || seconds < 0) return '--';

    const days = Math.floor(seconds / 86400);
    const hours = Math.floor((seconds % 86400) / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    const secs = seconds % 60;

    const parts = [];
    if (days > 0) parts.push(`${days}d`);
    if (hours > 0) parts.push(`${hours}h`);
    if (minutes > 0) parts.push(`${minutes}m`);
    if (parts.length === 0) parts.push(`${secs}s`);

    return parts.join(' ');
}

// =========================================================================
// Toast Notifications
// =========================================================================

class Toast {
    static show(message, type = 'info', duration = 3000) {
        const toast = document.getElementById('toast');
        const icon = toast.querySelector('.toast-icon');
        const text = toast.querySelector('.toast-message');

        // Set icon based on type
        const icons = {
            success: 'check_circle',
            error: 'error',
            warning: 'warning',
            info: 'info'
        };

        icon.textContent = icons[type] || icons.info;
        text.textContent = message;

        // Remove old classes and add new one
        toast.className = 'toast active ' + type;

        // Auto-hide
        clearTimeout(toast._timeout);
        toast._timeout = setTimeout(() => {
            toast.classList.remove('active');
        }, duration);
    }

    static success(message) {
        this.show(message, 'success');
    }

    static error(message) {
        this.show(message, 'error', 5000);
    }

    static warning(message) {
        this.show(message, 'warning', 4000);
    }

    static info(message) {
        this.show(message, 'info');
    }
}

// =========================================================================
// Dialog Manager
// =========================================================================

class DialogManager {
    static show(dialogId) {
        const overlay = document.getElementById('dialogOverlay');
        const dialog = document.getElementById(dialogId);

        // Hide all dialogs first
        document.querySelectorAll('.dialog').forEach(d => d.classList.remove('active'));

        // Show overlay and specific dialog
        overlay.classList.add('active');
        dialog.classList.add('active');

        // Focus first input
        const firstInput = dialog.querySelector('input:not([type="hidden"]), select');
        if (firstInput) {
            setTimeout(() => firstInput.focus(), 100);
        }
    }

    static hide() {
        const overlay = document.getElementById('dialogOverlay');
        overlay.classList.remove('active');
        document.querySelectorAll('.dialog').forEach(d => d.classList.remove('active'));
    }

    static confirm(title, message) {
        return new Promise((resolve) => {
            const dialog = document.getElementById('confirmDialog');
            document.getElementById('confirmDialogTitle').textContent = title;
            document.getElementById('confirmDialogMessage').textContent = message;

            const confirmBtn = document.getElementById('confirmDialogConfirm');

            // Clear old listeners
            const newConfirmBtn = confirmBtn.cloneNode(true);
            confirmBtn.parentNode.replaceChild(newConfirmBtn, confirmBtn);

            newConfirmBtn.addEventListener('click', () => {
                DialogManager.hide();
                resolve(true);
            });

            this.show('confirmDialog');

            // Handle cancel via overlay click or cancel button
            const cancelHandler = () => {
                resolve(false);
            };

            dialog.querySelector('.dialog-cancel')?.addEventListener('click', cancelHandler, { once: true });
        });
    }
}

// =========================================================================
// Modbus Components
// =========================================================================

class ModbusComponents {
    static renderHubsList(hubs, container) {
        if (!hubs || hubs.length === 0) {
            container.innerHTML = `
                <div class="empty-state">
                    <span class="material-icons">settings_input_component</span>
                    <h3>No Modbus Hubs Configured</h3>
                    <p>Add a Modbus hub to start reading energy data from your devices.</p>
                </div>
            `;
            return;
        }

        // Store hubs for edit handlers
        this._currentHubs = hubs;

        container.innerHTML = hubs.map(hub => this.renderHub(hub)).join('');

        // Attach event listeners
        container.querySelectorAll('[data-action]').forEach(btn => {
            btn.addEventListener('click', (e) => {
                const action = btn.dataset.action;
                const hubName = btn.dataset.hub;
                const deviceName = btn.dataset.device;

                switch (action) {
                    case 'edit-hub':
                        this.handleEditHub(hubName);
                        break;
                    case 'delete-hub':
                        this.handleDeleteHub(hubName);
                        break;
                    case 'add-device':
                        this.handleAddDevice(hubName);
                        break;
                    case 'edit-device':
                        this.handleEditDevice(hubName, deviceName);
                        break;
                    case 'delete-device':
                        this.handleDeleteDevice(hubName, deviceName);
                        break;
                }
            });
        });
    }

    static renderHub(hub) {
        const protocolLabels = {
            'TCP': 'Modbus TCP',
            'RTU': 'Modbus RTU',
            'RTUoverTCP': 'RTU over TCP'
        };

        return `
            <div class="card adapter-card">
                <div class="adapter-header">
                    <span class="material-icons">settings_input_component</span>
                    <div class="adapter-info">
                        <div class="adapter-name">${escapeHtml(hub.name)}</div>
                        <div class="adapter-details">
                            ${escapeHtml(hub.host)}:${hub.port} &bull; ${protocolLabels[hub.proto] || hub.proto}
                        </div>
                    </div>
                    <div class="adapter-actions">
                        <button class="icon-button small" title="Edit Hub"
                                data-action="edit-hub" data-hub="${escapeHtml(hub.name)}">
                            <span class="material-icons">edit</span>
                        </button>
                        <button class="icon-button small" title="Delete Hub"
                                data-action="delete-hub" data-hub="${escapeHtml(hub.name)}">
                            <span class="material-icons">delete</span>
                        </button>
                    </div>
                </div>
                <div class="adapter-content">
                    <div class="devices-section">
                        <div class="devices-header">
                            <h4>Devices (${hub.devices?.length || 0})</h4>
                            <button class="text-button" data-action="add-device" data-hub="${escapeHtml(hub.name)}">
                                <span class="material-icons">add</span>
                                Add Device
                            </button>
                        </div>
                        <div class="devices-list">
                            ${this.renderDevicesList(hub)}
                        </div>
                    </div>
                </div>
            </div>
        `;
    }

    static renderDevicesList(hub) {
        if (!hub.devices || hub.devices.length === 0) {
            return `
                <div class="empty-state" style="padding: 16px;">
                    <p>No devices configured for this hub.</p>
                </div>
            `;
        }

        return hub.devices.map(device => `
            <div class="device-item">
                <div class="device-icon">
                    <span class="material-icons">electric_meter</span>
                </div>
                <div class="device-info">
                    <div class="device-name">${escapeHtml(device.name)}</div>
                    <div class="device-meta">
                        Meter: ${escapeHtml(device.meter)} &bull;
                        Slave ID: ${device.slave_id} &bull;
                        Interval: ${device.read_interval}s
                    </div>
                </div>
                <div class="device-actions">
                    <button class="icon-button small" title="Edit Device"
                            data-action="edit-device"
                            data-hub="${escapeHtml(hub.name)}"
                            data-device="${escapeHtml(device.name)}">
                        <span class="material-icons">edit</span>
                    </button>
                    <button class="icon-button small" title="Delete Device"
                            data-action="delete-device"
                            data-hub="${escapeHtml(hub.name)}"
                            data-device="${escapeHtml(device.name)}">
                        <span class="material-icons">delete</span>
                    </button>
                </div>
            </div>
        `).join('');
    }

    static async handleDeleteHub(hubName) {
        const confirmed = await DialogManager.confirm(
            'Delete Modbus Hub',
            `Are you sure you want to delete "${hubName}" and all its devices?`
        );

        if (confirmed) {
            try {
                await window.api.deleteModbusHub(hubName);
                Toast.success(`Hub "${hubName}" deleted`);
                window.dispatchEvent(new CustomEvent('modbus:refresh'));
            } catch (error) {
                Toast.error(`Failed to delete hub: ${error.message}`);
            }
        }
    }

    static handleAddDevice(hubName) {
        document.getElementById('modbusDeviceForm').reset();
        document.getElementById('modbusDeviceHubName').value = hubName;
        document.getElementById('modbusDeviceEditName').value = '';
        document.getElementById('modbusDeviceDialogTitle').textContent = 'Add Device to ' + hubName;
        document.getElementById('modbusDeviceSubmit').textContent = 'Add Device';
        DialogManager.show('modbusDeviceDialog');
    }

    static async handleDeleteDevice(hubName, deviceName) {
        const confirmed = await DialogManager.confirm(
            'Delete Device',
            `Are you sure you want to delete "${deviceName}"?`
        );

        if (confirmed) {
            try {
                await window.api.deleteModbusDevice(hubName, deviceName);
                Toast.success(`Device "${deviceName}" deleted`);
                window.dispatchEvent(new CustomEvent('modbus:refresh'));
            } catch (error) {
                Toast.error(`Failed to delete device: ${error.message}`);
            }
        }
    }

    static handleEditHub(hubName) {
        const hub = this._currentHubs?.find(h => h.name === hubName);
        if (!hub) return;

        // Fill form with existing data
        document.getElementById('modbusHubEditName').value = hub.name;
        document.getElementById('modbusHubName').value = hub.name;
        document.getElementById('modbusHubHost').value = hub.host || '';
        document.getElementById('modbusHubPort').value = hub.port || 502;
        document.getElementById('modbusHubProto').value = hub.proto || 'TCP';
        document.getElementById('modbusHubConnTimeout').value = hub.conn_timeout || 10;
        document.getElementById('modbusHubReadTimeout').value = hub.read_timeout || 5;

        document.getElementById('modbusHubDialogTitle').textContent = 'Edit Modbus Hub';
        document.getElementById('modbusHubSubmit').textContent = 'Save Changes';
        DialogManager.show('modbusHubDialog');
    }

    static handleEditDevice(hubName, deviceName) {
        const hub = this._currentHubs?.find(h => h.name === hubName);
        if (!hub) return;

        const device = hub.devices?.find(d => d.name === deviceName);
        if (!device) return;

        // Fill form with existing data
        document.getElementById('modbusDeviceHubName').value = hubName;
        document.getElementById('modbusDeviceEditName').value = device.name;
        document.getElementById('modbusDeviceName').value = device.name;
        document.getElementById('modbusDeviceMeter').value = device.meter || '';
        document.getElementById('modbusDeviceSlaveId').value = device.slave_id || 1;
        document.getElementById('modbusDeviceReadInterval').value = device.read_interval || 30;

        document.getElementById('modbusDeviceDialogTitle').textContent = 'Edit Modbus Device';
        document.getElementById('modbusDeviceSubmit').textContent = 'Save Changes';
        DialogManager.show('modbusDeviceDialog');
    }
}

// =========================================================================
// KNX Components
// =========================================================================

class KnxComponents {
    static renderAdaptersList(adapters, container) {
        if (!adapters || adapters.length === 0) {
            container.innerHTML = `
                <div class="empty-state">
                    <span class="material-icons">device_hub</span>
                    <h3>No KNX Adapters Configured</h3>
                    <p>Add a KNX IP gateway to start reading energy data from your KNX devices.</p>
                </div>
            `;
            return;
        }

        // Store adapters for edit handlers
        this._currentAdapters = adapters;

        container.innerHTML = adapters.map(adapter => this.renderAdapter(adapter)).join('');

        // Attach event listeners
        container.querySelectorAll('[data-action]').forEach(btn => {
            btn.addEventListener('click', (e) => {
                const action = btn.dataset.action;
                const adapterName = btn.dataset.adapter;
                const meterName = btn.dataset.meter;
                const switchName = btn.dataset.switch;

                switch (action) {
                    case 'edit-adapter':
                        this.handleEditAdapter(adapterName);
                        break;
                    case 'delete-adapter':
                        this.handleDeleteAdapter(adapterName);
                        break;
                    case 'add-meter':
                        this.handleAddMeter(adapterName);
                        break;
                    case 'edit-meter':
                        this.handleEditMeter(adapterName, meterName);
                        break;
                    case 'add-switch':
                        this.handleAddSwitch(adapterName);
                        break;
                    case 'edit-switch':
                        this.handleEditSwitch(adapterName, switchName);
                        break;
                    case 'delete-meter':
                        this.handleDeleteMeter(adapterName, meterName);
                        break;
                    case 'delete-switch':
                        this.handleDeleteSwitch(adapterName, switchName);
                        break;
                }
            });
        });
    }

    static renderAdapter(adapter) {
        const statusClass = adapter.enabled !== false ? '' : 'disabled';
        const statusBadge = adapter.enabled !== false
            ? '<span class="badge success">Enabled</span>'
            : '<span class="badge">Disabled</span>';
        const readDelay = adapter.read_delay_ms || 100;

        return `
            <div class="card adapter-card ${statusClass}">
                <div class="adapter-header">
                    <span class="material-icons">device_hub</span>
                    <div class="adapter-info">
                        <div class="adapter-name">${escapeHtml(adapter.name)} ${statusBadge}</div>
                        <div class="adapter-details">
                            ${escapeHtml(adapter.host)}:${adapter.port || 3671} &bull; Read delay: ${readDelay}ms
                        </div>
                    </div>
                    <div class="adapter-actions">
                        <button class="icon-button small" title="Edit Adapter"
                                data-action="edit-adapter" data-adapter="${escapeHtml(adapter.name)}">
                            <span class="material-icons">edit</span>
                        </button>
                        <button class="icon-button small" title="Delete Adapter"
                                data-action="delete-adapter" data-adapter="${escapeHtml(adapter.name)}">
                            <span class="material-icons">delete</span>
                        </button>
                    </div>
                </div>
                <div class="adapter-content">
                    <!-- Meters Section -->
                    <div class="devices-section">
                        <div class="devices-header">
                            <h4>Meters (${adapter.meters?.length || 0})</h4>
                            <button class="text-button" data-action="add-meter" data-adapter="${escapeHtml(adapter.name)}">
                                <span class="material-icons">add</span>
                                Add Meter
                            </button>
                        </div>
                        <div class="devices-list">
                            ${this.renderMetersList(adapter)}
                        </div>
                    </div>

                    <!-- Switches Section -->
                    <div class="devices-section">
                        <div class="devices-header">
                            <h4>Switches (${adapter.switches?.length || 0})</h4>
                            <button class="text-button" data-action="add-switch" data-adapter="${escapeHtml(adapter.name)}">
                                <span class="material-icons">add</span>
                                Add Switch
                            </button>
                        </div>
                        <div class="devices-list">
                            ${this.renderSwitchesList(adapter)}
                        </div>
                    </div>
                </div>
            </div>
        `;
    }

    static renderMetersList(adapter) {
        if (!adapter.meters || adapter.meters.length === 0) {
            return `
                <div class="empty-state" style="padding: 16px;">
                    <p>No meters configured for this adapter.</p>
                </div>
            `;
        }

        return adapter.meters.map(meter => {
            let configInfo = '';
            let switchInfo = '';

            // Check if multi-phase or single-phase
            if (meter.phases && meter.phases.length > 0) {
                const phaseNames = meter.phases.map(p => p.name || 'Phase').join(', ');
                configInfo = `<span class="badge">Multi-Phase</span> ${phaseNames}`;
                // Check for switches in phases or global
                const hasPhaseSwitch = meter.phases.some(p => p.switch_ga || p.switch_state_ga);
                if (meter.switch_ga || hasPhaseSwitch) {
                    switchInfo = ' <span class="badge">Switch</span>';
                }
            } else {
                const gaInfo = [];
                if (meter.voltage_ga) gaInfo.push(`V: ${meter.voltage_ga}`);
                if (meter.power_ga) gaInfo.push(`P: ${meter.power_ga}`);
                if (meter.energy_ga) gaInfo.push(`E: ${meter.energy_ga}`);
                configInfo = gaInfo.length > 0 ? gaInfo.join(' &bull; ') : 'No group addresses configured';
                // Check for switch
                if (meter.switch_ga || meter.switch_state_ga) {
                    switchInfo = ' <span class="badge">Switch</span>';
                }
            }

            return `
                <div class="device-item">
                    <div class="device-icon">
                        <span class="material-icons">electric_meter</span>
                    </div>
                    <div class="device-info">
                        <div class="device-name">${escapeHtml(meter.name)}${switchInfo}</div>
                        <div class="device-meta">
                            ${configInfo}
                            ${meter.manufacturer ? ` &bull; ${escapeHtml(meter.manufacturer)}` : ''}
                        </div>
                    </div>
                    <div class="device-actions">
                        <button class="icon-button small" title="Edit Meter"
                                data-action="edit-meter"
                                data-adapter="${escapeHtml(adapter.name)}"
                                data-meter="${escapeHtml(meter.name)}">
                            <span class="material-icons">edit</span>
                        </button>
                        <button class="icon-button small" title="Delete Meter"
                                data-action="delete-meter"
                                data-adapter="${escapeHtml(adapter.name)}"
                                data-meter="${escapeHtml(meter.name)}">
                            <span class="material-icons">delete</span>
                        </button>
                    </div>
                </div>
            `;
        }).join('');
    }

    static renderSwitchesList(adapter) {
        if (!adapter.switches || adapter.switches.length === 0) {
            return `
                <div class="empty-state" style="padding: 16px;">
                    <p>No switches configured for this adapter.</p>
                </div>
            `;
        }

        return adapter.switches.map(sw => `
            <div class="device-item">
                <div class="device-icon">
                    <span class="material-icons">toggle_on</span>
                </div>
                <div class="device-info">
                    <div class="device-name">${escapeHtml(sw.name)}</div>
                    <div class="device-meta">
                        GA: ${escapeHtml(sw.group_address)}
                        ${sw.state_address ? ` &bull; State: ${escapeHtml(sw.state_address)}` : ''}
                        ${sw.expose_to_ha !== false ? ' &bull; HA Exposed' : ''}
                    </div>
                </div>
                <div class="device-actions">
                    <button class="icon-button small" title="Edit Switch"
                            data-action="edit-switch"
                            data-adapter="${escapeHtml(adapter.name)}"
                            data-switch="${escapeHtml(sw.name)}">
                        <span class="material-icons">edit</span>
                    </button>
                    <button class="icon-button small" title="Delete Switch"
                            data-action="delete-switch"
                            data-adapter="${escapeHtml(adapter.name)}"
                            data-switch="${escapeHtml(sw.name)}">
                        <span class="material-icons">delete</span>
                    </button>
                </div>
            </div>
        `).join('');
    }

    static async handleDeleteAdapter(adapterName) {
        const confirmed = await DialogManager.confirm(
            'Delete KNX Adapter',
            `Are you sure you want to delete "${adapterName}" and all its meters and switches?`
        );

        if (confirmed) {
            try {
                await window.api.deleteKnxAdapter(adapterName);
                Toast.success(`Adapter "${adapterName}" deleted`);
                window.dispatchEvent(new CustomEvent('knx:refresh'));
            } catch (error) {
                Toast.error(`Failed to delete adapter: ${error.message}`);
            }
        }
    }

    static handleAddMeter(adapterName) {
        document.getElementById('knxMeterForm').reset();
        document.getElementById('knxMeterAdapterName').value = adapterName;
        document.getElementById('knxMeterEditName').value = '';
        document.getElementById('knxMeterDialogTitle').textContent = 'Add Meter to ' + adapterName;
        document.getElementById('knxMeterSubmit').textContent = 'Add Meter';
        document.getElementById('knxMeterEnabled').checked = true;
        // Reset phase mode
        const phaseModeSelect = document.getElementById('knxMeterPhaseMode');
        if (phaseModeSelect) phaseModeSelect.value = 'single';
        const phaseConfig = document.getElementById('phaseConfig');
        if (phaseConfig) phaseConfig.classList.add('hidden');
        const singlePhaseConfig = document.getElementById('singlePhaseConfig');
        if (singlePhaseConfig) singlePhaseConfig.classList.remove('hidden');
        DialogManager.show('knxMeterDialog');
    }

    static handleAddSwitch(adapterName) {
        document.getElementById('knxSwitchForm').reset();
        document.getElementById('knxSwitchAdapterName').value = adapterName;
        document.getElementById('knxSwitchEditName').value = '';
        document.getElementById('knxSwitchDialogTitle').textContent = 'Add Switch to ' + adapterName;
        document.getElementById('knxSwitchSubmit').textContent = 'Add Switch';
        document.getElementById('knxSwitchEnabled').checked = true;
        document.getElementById('knxSwitchExposeToHa').checked = true;
        DialogManager.show('knxSwitchDialog');
    }

    static handleEditAdapter(adapterName) {
        const adapter = this._currentAdapters?.find(a => a.name === adapterName);
        if (!adapter) return;

        // Fill form with existing data
        document.getElementById('knxAdapterEditName').value = adapter.name;
        document.getElementById('knxAdapterName').value = adapter.name;
        document.getElementById('knxAdapterHost').value = adapter.host || '';
        document.getElementById('knxAdapterPort').value = adapter.port || 3671;
        document.getElementById('knxAdapterEnabled').checked = adapter.enabled !== false;
        document.getElementById('knxAdapterConnTimeout').value = adapter.connection_timeout || 10;
        document.getElementById('knxAdapterReadTimeout').value = adapter.read_timeout || 5;
        document.getElementById('knxAdapterReadDelayMs').value = adapter.read_delay_ms || 100;

        document.getElementById('knxAdapterDialogTitle').textContent = 'Edit KNX Adapter';
        document.getElementById('knxAdapterSubmit').textContent = 'Save Changes';
        DialogManager.show('knxAdapterDialog');
    }

    static handleEditMeter(adapterName, meterName) {
        const adapter = this._currentAdapters?.find(a => a.name === adapterName);
        if (!adapter) return;

        const meter = adapter.meters?.find(m => m.name === meterName);
        if (!meter) return;

        // Fill form with existing data
        document.getElementById('knxMeterAdapterName').value = adapterName;
        document.getElementById('knxMeterEditName').value = meter.name;
        document.getElementById('knxMeterName').value = meter.name;
        document.getElementById('knxMeterManufacturer').value = meter.manufacturer || '';
        document.getElementById('knxMeterModel').value = meter.model || '';
        document.getElementById('knxMeterEnabled').checked = meter.enabled !== false;

        // Check if multi-phase or single-phase
        const isMultiPhase = meter.phases && meter.phases.length > 0;
        const phaseModeSelect = document.getElementById('knxMeterPhaseMode');
        const phaseConfig = document.getElementById('phaseConfig');
        const singlePhaseConfig = document.getElementById('singlePhaseConfig');

        if (isMultiPhase && phaseModeSelect) {
            phaseModeSelect.value = 'multi';
            if (phaseConfig) phaseConfig.classList.remove('hidden');
            if (singlePhaseConfig) singlePhaseConfig.classList.add('hidden');

            // Fill phase data
            for (let i = 0; i < 3; i++) {
                const phase = meter.phases[i];
                const phaseNum = i + 1;
                if (phase) {
                    const nameEl = document.getElementById(`knxMeterPhase${phaseNum}Name`);
                    if (nameEl) nameEl.value = phase.name || `L${phaseNum}`;
                    const voltageEl = document.getElementById(`knxMeterPhase${phaseNum}VoltageGa`);
                    if (voltageEl) voltageEl.value = phase.voltage_ga || '';
                    const currentEl = document.getElementById(`knxMeterPhase${phaseNum}CurrentGa`);
                    if (currentEl) currentEl.value = phase.current_ga || '';
                    const powerEl = document.getElementById(`knxMeterPhase${phaseNum}PowerGa`);
                    if (powerEl) powerEl.value = phase.power_ga || '';
                    const energyEl = document.getElementById(`knxMeterPhase${phaseNum}EnergyGa`);
                    if (energyEl) energyEl.value = phase.energy_ga || '';
                    const switchEl = document.getElementById(`knxMeterPhase${phaseNum}SwitchGa`);
                    if (switchEl) switchEl.value = phase.switch_ga || '';
                    const switchStateEl = document.getElementById(`knxMeterPhase${phaseNum}SwitchStateGa`);
                    if (switchStateEl) switchStateEl.value = phase.switch_state_ga || '';
                }
            }

            // Fill totals
            const totalPowerEl = document.getElementById('knxMeterTotalPowerGa');
            if (totalPowerEl) totalPowerEl.value = meter.total_power_ga || '';
            const totalEnergyEl = document.getElementById('knxMeterTotalEnergyGa');
            if (totalEnergyEl) totalEnergyEl.value = meter.total_energy_ga || '';
            const totalCurrentEl = document.getElementById('knxMeterTotalCurrentGa');
            if (totalCurrentEl) totalCurrentEl.value = meter.total_current_ga || '';
            const calcTotalsEl = document.getElementById('knxMeterCalculateTotals');
            if (calcTotalsEl) calcTotalsEl.checked = meter.calculate_totals !== false;

            // Global switch for multi-phase
            const multiSwitchEl = document.getElementById('knxMeterMultiSwitchGa');
            if (multiSwitchEl) multiSwitchEl.value = meter.switch_ga || '';
            const multiSwitchStateEl = document.getElementById('knxMeterMultiSwitchStateGa');
            if (multiSwitchStateEl) multiSwitchStateEl.value = meter.switch_state_ga || '';
        } else {
            if (phaseModeSelect) phaseModeSelect.value = 'single';
            if (phaseConfig) phaseConfig.classList.add('hidden');
            if (singlePhaseConfig) singlePhaseConfig.classList.remove('hidden');
            // Fill single-phase group addresses
            if (document.getElementById('knxMeterVoltageGa')) {
                document.getElementById('knxMeterVoltageGa').value = meter.voltage_ga || '';
            }
            if (document.getElementById('knxMeterCurrentGa')) {
                document.getElementById('knxMeterCurrentGa').value = meter.current_ga || '';
            }
            if (document.getElementById('knxMeterPowerGa')) {
                document.getElementById('knxMeterPowerGa').value = meter.power_ga || '';
            }
            if (document.getElementById('knxMeterEnergyGa')) {
                document.getElementById('knxMeterEnergyGa').value = meter.energy_ga || '';
            }
            // Switch fields for single-phase
            if (document.getElementById('knxMeterSwitchGa')) {
                document.getElementById('knxMeterSwitchGa').value = meter.switch_ga || '';
            }
            if (document.getElementById('knxMeterSwitchStateGa')) {
                document.getElementById('knxMeterSwitchStateGa').value = meter.switch_state_ga || '';
            }
        }

        document.getElementById('knxMeterDialogTitle').textContent = 'Edit KNX Meter';
        document.getElementById('knxMeterSubmit').textContent = 'Save Changes';
        DialogManager.show('knxMeterDialog');
    }

    static handleEditSwitch(adapterName, switchName) {
        const adapter = this._currentAdapters?.find(a => a.name === adapterName);
        if (!adapter) return;

        const sw = adapter.switches?.find(s => s.name === switchName);
        if (!sw) return;

        // Fill form with existing data
        document.getElementById('knxSwitchAdapterName').value = adapterName;
        document.getElementById('knxSwitchEditName').value = sw.name;
        document.getElementById('knxSwitchName').value = sw.name;
        document.getElementById('knxSwitchGroupAddress').value = sw.group_address || '';
        document.getElementById('knxSwitchStateAddress').value = sw.state_address || '';
        document.getElementById('knxSwitchEnabled').checked = sw.enabled !== false;
        document.getElementById('knxSwitchExposeToHa').checked = sw.expose_to_ha !== false;

        document.getElementById('knxSwitchDialogTitle').textContent = 'Edit KNX Switch';
        document.getElementById('knxSwitchSubmit').textContent = 'Save Changes';
        DialogManager.show('knxSwitchDialog');
    }

    static async handleDeleteMeter(adapterName, meterName) {
        const confirmed = await DialogManager.confirm(
            'Delete KNX Meter',
            `Are you sure you want to delete "${meterName}"?`
        );

        if (confirmed) {
            try {
                await window.api.deleteKnxMeter(adapterName, meterName);
                Toast.success(`Meter "${meterName}" deleted`);
                window.dispatchEvent(new CustomEvent('knx:refresh'));
            } catch (error) {
                Toast.error(`Failed to delete meter: ${error.message}`);
            }
        }
    }

    static async handleDeleteSwitch(adapterName, switchName) {
        const confirmed = await DialogManager.confirm(
            'Delete KNX Switch',
            `Are you sure you want to delete "${switchName}"?`
        );

        if (confirmed) {
            try {
                await window.api.deleteKnxSwitch(adapterName, switchName);
                Toast.success(`Switch "${switchName}" deleted`);
                window.dispatchEvent(new CustomEvent('knx:refresh'));
            } catch (error) {
                Toast.error(`Failed to delete switch: ${error.message}`);
            }
        }
    }
}

// =========================================================================
// Zenner Datahub Components
// =========================================================================

class ZennerComponents {
    static renderInstancesList(instances, container) {
        if (!instances || instances.length === 0) {
            container.innerHTML = `
                <div class="empty-state">
                    <span class="material-icons">water_drop</span>
                    <h3>No Zenner Datahub Instances</h3>
                    <p>Add a Zenner Datahub connection to start receiving meter data.</p>
                </div>
            `;
            return;
        }

        container.innerHTML = instances.map(instance => this.renderInstance(instance)).join('');

        // Attach event listeners
        container.querySelectorAll('[data-action]').forEach(btn => {
            btn.addEventListener('click', (e) => {
                const action = btn.dataset.action;
                const instanceName = btn.dataset.instance;

                switch (action) {
                    case 'edit-instance':
                        this.handleEditInstance(instanceName, instances);
                        break;
                    case 'delete-instance':
                        this.handleDeleteInstance(instanceName);
                        break;
                }
            });
        });
    }

    static renderInstance(instance) {
        const statusClass = instance.enabled !== false ? '' : 'disabled';
        const statusBadge = instance.enabled !== false
            ? '<span class="badge success">Enabled</span>'
            : '<span class="badge">Disabled</span>';

        return `
            <div class="card adapter-card ${statusClass}">
                <div class="adapter-header">
                    <span class="material-icons" style="font-size: 32px; color: var(--primary-color);">water_drop</span>
                    <div class="adapter-info">
                        <div class="adapter-name">${escapeHtml(instance.name)}</div>
                        <div class="adapter-details">
                            ${escapeHtml(instance.broker_host)}:${instance.broker_port} &bull;
                            Topic: ${escapeHtml(instance.base_topic)} &bull;
                            Interval: ${instance.update_interval}s
                        </div>
                    </div>
                    ${statusBadge}
                    <div class="adapter-actions">
                        <button class="icon-button small" title="Edit Instance"
                                data-action="edit-instance"
                                data-instance="${escapeHtml(instance.name)}">
                            <span class="material-icons">edit</span>
                        </button>
                        <button class="icon-button small" title="Delete Instance"
                                data-action="delete-instance"
                                data-instance="${escapeHtml(instance.name)}">
                            <span class="material-icons">delete</span>
                        </button>
                    </div>
                </div>
                <div class="adapter-content">
                    <div class="instance-details">
                        <div class="detail-row">
                            <span class="detail-label">Client Name</span>
                            <span class="detail-value">${escapeHtml(instance.client_name || 'auto')}</span>
                        </div>
                        <div class="detail-row">
                            <span class="detail-label">Username</span>
                            <span class="detail-value">${escapeHtml(instance.broker_user || '-')}</span>
                        </div>
                    </div>
                </div>
            </div>
        `;
    }

    static handleEditInstance(instanceName, instances) {
        const instance = instances.find(i => i.name === instanceName);
        if (!instance) return;

        // Fill form with existing data
        document.getElementById('zennerInstanceEditName').value = instance.name;
        document.getElementById('zennerInstanceName').value = instance.name;
        document.getElementById('zennerBrokerHost').value = instance.broker_host || '';
        document.getElementById('zennerBrokerPort').value = instance.broker_port || 1883;
        document.getElementById('zennerBrokerUser').value = instance.broker_user || '';
        document.getElementById('zennerBrokerPass').value = instance.broker_pass || '';
        document.getElementById('zennerClientName').value = instance.client_name || '';
        document.getElementById('zennerBaseTopic').value = instance.base_topic || '';
        document.getElementById('zennerUpdateInterval').value = instance.update_interval || 60;
        document.getElementById('zennerEnabled').checked = instance.enabled !== false;

        document.getElementById('zennerInstanceDialogTitle').textContent = 'Edit Zenner Instance';
        document.getElementById('zennerInstanceSubmit').textContent = 'Save Changes';
        DialogManager.show('zennerInstanceDialog');
    }

    static async handleDeleteInstance(instanceName) {
        const confirmed = await DialogManager.confirm(
            'Delete Zenner Instance',
            `Are you sure you want to delete "${instanceName}"?`
        );

        if (confirmed) {
            try {
                await window.api.deleteZennerInstance(instanceName);
                Toast.success(`Instance "${instanceName}" deleted`);
                window.dispatchEvent(new CustomEvent('zenner:refresh'));
            } catch (error) {
                Toast.error(`Failed to delete instance: ${error.message}`);
            }
        }
    }
}

// =============================================================================
// OMS Components
// =============================================================================

class OmsComponents {
    static renderMetersList(meters, container) {
        if (!meters || meters.length === 0) {
            container.innerHTML = `
                <div class="empty-state">
                    <span class="material-icons">sensors</span>
                    <h3>No OMS Meters</h3>
                    <p>Add an OMS meter to start receiving wireless M-Bus data.</p>
                </div>
            `;
            return;
        }

        container.innerHTML = meters.map(meter => this.renderMeter(meter)).join('');

        // Attach event listeners
        container.querySelectorAll('[data-action]').forEach(btn => {
            btn.addEventListener('click', (e) => {
                const action = btn.dataset.action;
                const meterName = btn.dataset.meter;

                switch (action) {
                    case 'edit-meter':
                        this.handleEditMeter(meterName, meters);
                        break;
                    case 'delete-meter':
                        this.handleDeleteMeter(meterName);
                        break;
                }
            });
        });
    }

    static renderMeter(meter) {
        // Parse DIN address components if possible
        const dinInfo = this.parseDinAddress(meter.id);

        return `
            <div class="card adapter-card">
                <div class="adapter-header">
                    <span class="material-icons" style="font-size: 32px; color: var(--primary-color);">sensors</span>
                    <div class="adapter-info">
                        <div class="adapter-name">${escapeHtml(meter.name)}</div>
                        <div class="adapter-details">
                            Device ID: <code>${escapeHtml(meter.id)}</code>
                            ${dinInfo ? ` &bull; ${dinInfo}` : ''}
                        </div>
                    </div>
                    <div class="adapter-actions">
                        <button class="icon-button small" title="Edit Meter"
                                data-action="edit-meter"
                                data-meter="${escapeHtml(meter.name)}">
                            <span class="material-icons">edit</span>
                        </button>
                        <button class="icon-button small" title="Delete Meter"
                                data-action="delete-meter"
                                data-meter="${escapeHtml(meter.name)}">
                            <span class="material-icons">delete</span>
                        </button>
                    </div>
                </div>
                <div class="adapter-content">
                    <div class="instance-details">
                        <div class="detail-row">
                            <span class="detail-label">Encryption Key</span>
                            <span class="detail-value monospace-text">${this.maskKey(meter.key)}</span>
                        </div>
                    </div>
                </div>
            </div>
        `;
    }

    static parseDinAddress(id) {
        // DIN address format: device_type (1 hex) + manufacturer (3 alphanumeric) + version + serial (hex)
        if (!id || id.length < 8) return null;

        try {
            const deviceTypes = {
                '0': 'Other', '1': 'Oil', '2': 'Electricity', '3': 'Gas',
                '4': 'Heat', '5': 'Steam', '6': 'Hot Water', '7': 'Water',
                '8': 'HCA', '9': 'Compressed Air', 'A': 'Cooling (outlet)',
                'B': 'Cooling (inlet)', 'C': 'Heat (inlet)', 'D': 'Heat/Cooling',
                'E': 'Bus/System', 'F': 'Unknown'
            };

            const typeChar = id[0].toUpperCase();
            const deviceType = deviceTypes[typeChar] || 'Unknown';
            const manufacturer = id.substring(1, 4).toUpperCase();

            return `${deviceType} (${manufacturer})`;
        } catch (e) {
            return null;
        }
    }

    static maskKey(key) {
        if (!key || key.length < 8) return key;
        return key.substring(0, 4) + '••••••••••••••••••••••••' + key.substring(key.length - 4);
    }

    static handleEditMeter(meterName, meters) {
        const meter = meters.find(m => m.name === meterName);
        if (!meter) return;

        // Fill form with existing data
        document.getElementById('omsMeterEditName').value = meter.name;
        document.getElementById('omsMeterName').value = meter.name;
        document.getElementById('omsMeterDeviceId').value = meter.id || '';
        document.getElementById('omsMeterKey').value = meter.key || '';

        document.getElementById('omsMeterDialogTitle').textContent = 'Edit OMS Meter';
        document.getElementById('omsMeterSubmit').textContent = 'Save Changes';
        DialogManager.show('omsMeterDialog');
    }

    static async handleDeleteMeter(meterName) {
        const confirmed = await DialogManager.confirm(
            'Delete OMS Meter',
            `Are you sure you want to delete "${meterName}"?`
        );

        if (confirmed) {
            try {
                await window.api.deleteOmsMeter(meterName);
                Toast.success(`Meter "${meterName}" deleted`);
                window.dispatchEvent(new CustomEvent('oms:refresh'));
            } catch (error) {
                Toast.error(`Failed to delete meter: ${error.message}`);
            }
        }
    }
}

// Export for global use
window.Toast = Toast;
window.DialogManager = DialogManager;
window.ModbusComponents = ModbusComponents;
window.KnxComponents = KnxComponents;
window.ZennerComponents = ZennerComponents;
window.OmsComponents = OmsComponents;
window.formatUptime = formatUptime;
