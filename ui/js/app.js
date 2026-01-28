/**
 * Energy2MQTT Main Application
 * Handles page navigation, state management, and UI updates
 */

class Energy2MqttApp {
    constructor() {
        this.currentPage = 'overview';
        this.config = null;
        this.healthData = null;

        this.init();
    }

    async init() {
        // Check if setup wizard is needed
        const needsSetup = await window.setupWizard.checkIfNeeded();
        if (needsSetup) {
            // Setup wizard is shown, don't initialize the rest yet
            // The page will reload after setup is complete
            return;
        }

        // Set up navigation
        this.setupNavigation();

        // Set up dialogs
        this.setupDialogs();

        // Set up form handlers
        this.setupFormHandlers();

        // Set up action buttons
        this.setupActionButtons();

        // Set up WebSocket connection status
        this.setupConnectionStatus();

        // Set up refresh events
        this.setupRefreshEvents();

        // Load initial data
        await this.loadInitialData();

        // Connect WebSocket
        window.api.connectWebSocket();

        // Start health polling
        this.startHealthPolling();

        // Set up unsaved changes indicator
        this.setupUnsavedIndicator();

        // Start config status polling
        this.startConfigStatusPolling();
    }

    // =========================================================================
    // Navigation
    // =========================================================================

    setupNavigation() {
        // Sidebar navigation
        document.querySelectorAll('.nav-item[data-page]').forEach(item => {
            item.addEventListener('click', (e) => {
                e.preventDefault();
                this.navigateTo(item.dataset.page);
            });
        });

        // Card action buttons that navigate
        document.querySelectorAll('.card-actions [data-page]').forEach(btn => {
            btn.addEventListener('click', () => {
                this.navigateTo(btn.dataset.page);
            });
        });

        // Mobile menu toggle
        document.getElementById('menuToggle')?.addEventListener('click', () => {
            document.querySelector('.sidebar').classList.toggle('open');
        });

        // Close sidebar on page click (mobile)
        document.querySelector('.main-content')?.addEventListener('click', () => {
            document.querySelector('.sidebar').classList.remove('open');
        });
    }

    navigateTo(pageName) {
        // Update navigation
        document.querySelectorAll('.nav-item').forEach(item => {
            item.classList.toggle('active', item.dataset.page === pageName);
        });

        // Update page visibility
        document.querySelectorAll('.page').forEach(page => {
            page.classList.toggle('active', page.id === `page-${pageName}`);
        });

        // Update title
        const titles = {
            'overview': 'Overview',
            'modbus': 'Modbus',
            'knx': 'KNX',
            'zenner': 'Zenner Datahub',
            'oms': 'OMS Meters',
            'live': 'Live View',
            'settings': 'Settings'
        };
        document.getElementById('pageTitle').textContent = titles[pageName] || pageName;

        this.currentPage = pageName;

        // Load page-specific data
        this.loadPageData(pageName);

        // Close mobile sidebar
        document.querySelector('.sidebar').classList.remove('open');
    }

    async loadPageData(pageName) {
        // Disconnect live view when leaving the page
        if (this.currentPage === 'live' && pageName !== 'live') {
            window.liveView?.disconnect();
        }

        switch (pageName) {
            case 'modbus':
                await this.loadModbusData();
                break;
            case 'knx':
                await this.loadKnxData();
                break;
            case 'zenner':
                await this.loadZennerData();
                break;
            case 'oms':
                await this.loadOmsData();
                break;
            case 'settings':
                this.updateMqttPage();
                break;
            case 'live':
                this.initLiveView();
                break;
        }
    }

    initLiveView() {
        if (window.liveView) {
            window.liveView.init();
            window.liveView.connect();
        }
    }

    // =========================================================================
    // Data Loading
    // =========================================================================

    async loadInitialData() {
        try {
            // Load config
            this.config = await window.api.getConfig();
            this.updateOverviewStats();
            this.updateMqttPage();

            // Load health
            await this.loadHealthData();
        } catch (error) {
            console.error('Failed to load initial data:', error);
            Toast.error('Failed to connect to energy2mqtt');
        }
    }

    async loadHealthData() {
        try {
            this.healthData = await window.api.getHealth();
            this.updateHealthDisplay();
        } catch (error) {
            console.error('Failed to load health data:', error);
            this.healthData = null;
            this.updateHealthDisplay();
        }
    }

    async loadModbusData() {
        const container = document.getElementById('modbusHubsList');
        container.innerHTML = '<div class="loading"><div class="spinner"></div></div>';

        try {
            const modbusConfig = await window.api.getModbusConfig();
            ModbusComponents.renderHubsList(modbusConfig.hubs || [], container);
        } catch (error) {
            console.error('Failed to load Modbus data:', error);
            container.innerHTML = `
                <div class="empty-state">
                    <span class="material-icons">error</span>
                    <h3>Failed to Load</h3>
                    <p>${error.message}</p>
                </div>
            `;
        }
    }

    async loadKnxData() {
        const container = document.getElementById('knxAdaptersList');
        container.innerHTML = '<div class="loading"><div class="spinner"></div></div>';

        try {
            const knxAdapters = await window.api.getKnxConfig();
            KnxComponents.renderAdaptersList(knxAdapters || [], container);
        } catch (error) {
            console.error('Failed to load KNX data:', error);
            container.innerHTML = `
                <div class="empty-state">
                    <span class="material-icons">error</span>
                    <h3>Failed to Load</h3>
                    <p>${error.message}</p>
                </div>
            `;
        }
    }

    async loadZennerData() {
        const container = document.getElementById('zennerInstancesList');
        container.innerHTML = '<div class="loading"><div class="spinner"></div></div>';

        try {
            const zennerInstances = await window.api.getZennerConfig();
            ZennerComponents.renderInstancesList(zennerInstances || [], container);
        } catch (error) {
            console.error('Failed to load Zenner data:', error);
            container.innerHTML = `
                <div class="empty-state">
                    <span class="material-icons">error</span>
                    <h3>Failed to Load</h3>
                    <p>${error.message}</p>
                </div>
            `;
        }
    }

    async loadOmsData() {
        const container = document.getElementById('omsMetersList');
        container.innerHTML = '<div class="loading"><div class="spinner"></div></div>';

        try {
            const omsMeters = await window.api.getOmsConfig();
            OmsComponents.renderMetersList(omsMeters || [], container);
        } catch (error) {
            console.error('Failed to load OMS data:', error);
            container.innerHTML = `
                <div class="empty-state">
                    <span class="material-icons">error</span>
                    <h3>Failed to Load</h3>
                    <p>${error.message}</p>
                </div>
            `;
        }
    }

    // =========================================================================
    // UI Updates
    // =========================================================================

    updateOverviewStats() {
        if (!this.config) return;

        // Update hub/adapter counts
        document.getElementById('modbusHubCount').textContent =
            this.config.modbus?.hubs?.length || 0;
        document.getElementById('knxAdapterCount').textContent =
            this.config.knx?.length || 0;
        document.getElementById('zennerInstanceCount').textContent =
            this.config.zenner_datahub?.length || 0;
        document.getElementById('omsMeterCount').textContent =
            this.config.oms?.length || 0;
    }

    updateHealthDisplay() {
        const mqttStatus = document.getElementById('mqttStatus');
        const uptime = document.getElementById('uptime');
        const connectionStatus = document.getElementById('connectionStatus');

        if (!this.healthData) {
            mqttStatus.textContent = 'Unknown';
            mqttStatus.className = 'badge';
            uptime.textContent = '--';
            return;
        }

        // MQTT Status
        const status = this.healthData.mqtt?.status || 'unknown';
        const statusClasses = {
            'connected': 'badge success',
            'disconnected': 'badge error',
            'reconnecting': 'badge warning',
            'error': 'badge error'
        };
        mqttStatus.textContent = status.charAt(0).toUpperCase() + status.slice(1);
        mqttStatus.className = statusClasses[status] || 'badge';

        // Update sidebar connection status based on MQTT status
        if (connectionStatus) {
            const dot = connectionStatus.querySelector('.status-dot');
            const text = connectionStatus.querySelector('.status-text');
            if (status === 'connected') {
                dot.className = 'status-dot connected';
                text.textContent = 'MQTT Connected';
            } else if (status === 'reconnecting') {
                dot.className = 'status-dot';
                text.textContent = 'Reconnecting...';
            } else {
                dot.className = 'status-dot error';
                text.textContent = 'MQTT ' + (status.charAt(0).toUpperCase() + status.slice(1));
            }
        }

        // Uptime
        uptime.textContent = formatUptime(this.healthData.uptime_seconds);
    }

    updateMqttPage() {
        if (!this.config?.mqtt) return;

        document.getElementById('mqttHost').value = this.config.mqtt.host || '';
        document.getElementById('mqttPort').value = this.config.mqtt.port || 1883;
        document.getElementById('mqttUser').value = this.config.mqtt.user || '';
        document.getElementById('mqttHaEnabled').checked = this.config.mqtt.ha_enabled !== false;
    }

    // =========================================================================
    // Dialogs
    // =========================================================================

    setupDialogs() {
        const overlay = document.getElementById('dialogOverlay');

        // Close on overlay click
        overlay.addEventListener('click', (e) => {
            if (e.target === overlay) {
                DialogManager.hide();
            }
        });

        // Close buttons
        document.querySelectorAll('.dialog-close, .dialog-cancel').forEach(btn => {
            btn.addEventListener('click', () => {
                DialogManager.hide();
            });
        });

        // ESC key to close
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape') {
                DialogManager.hide();
            }
        });
    }

    // =========================================================================
    // Form Handlers
    // =========================================================================

    setupFormHandlers() {
        // KNX Meter Phase Mode Toggle
        document.getElementById('knxMeterPhaseMode')?.addEventListener('change', (e) => {
            const singlePhase = document.getElementById('knxMeterSinglePhase');
            const multiPhase = document.getElementById('knxMeterMultiPhase');
            if (e.target.value === 'single') {
                singlePhase.style.display = 'block';
                multiPhase.style.display = 'none';
            } else {
                singlePhase.style.display = 'none';
                multiPhase.style.display = 'block';
            }
        });

        // KNX Meter Phase Tabs
        document.querySelectorAll('.phase-tab').forEach(tab => {
            tab.addEventListener('click', (e) => {
                e.preventDefault();
                const phase = tab.dataset.phase;

                // Update active tab
                document.querySelectorAll('.phase-tab').forEach(t => t.classList.remove('active'));
                tab.classList.add('active');

                // Show corresponding content
                document.querySelectorAll('.phase-content').forEach(c => {
                    c.style.display = 'none';
                    c.classList.remove('active');
                });
                const content = document.getElementById(`knxMeterPhase${phase}`);
                if (content) {
                    content.style.display = 'block';
                    content.classList.add('active');
                }
            });
        });

        // Modbus Hub Form
        document.getElementById('modbusHubSubmit')?.addEventListener('click', async () => {
            const form = document.getElementById('modbusHubForm');
            if (!form.checkValidity()) {
                form.reportValidity();
                return;
            }

            const editingName = document.getElementById('modbusHubEditName').value;

            // Get current hub data to preserve devices if editing
            let existingDevices = [];
            if (editingName) {
                try {
                    const config = await window.api.getModbusConfig();
                    const existingHub = config.hubs?.find(h => h.name === editingName);
                    if (existingHub) {
                        existingDevices = existingHub.devices || [];
                    }
                } catch (e) {
                    console.error('Failed to get existing hub:', e);
                }
            }

            const hub = {
                name: document.getElementById('modbusHubName').value,
                host: document.getElementById('modbusHubHost').value,
                port: parseInt(document.getElementById('modbusHubPort').value),
                proto: document.getElementById('modbusHubProto').value,
                connection_timeout: parseInt(document.getElementById('modbusHubConnTimeout').value) || 10,
                read_timeout: parseInt(document.getElementById('modbusHubReadTimeout').value) || 5,
                devices: existingDevices
            };

            try {
                if (editingName) {
                    await window.api.updateModbusHub(editingName, hub);
                    Toast.success(`Hub "${hub.name}" updated`);
                } else {
                    await window.api.addModbusHub(hub);
                    Toast.success(`Hub "${hub.name}" created`);
                }
                DialogManager.hide();
                await this.loadModbusData();
                this.config = await window.api.getConfig();
                this.updateOverviewStats();
            } catch (error) {
                Toast.error(`${error.message}`);
            }
        });

        // Modbus Device Form
        document.getElementById('modbusDeviceSubmit')?.addEventListener('click', async () => {
            const form = document.getElementById('modbusDeviceForm');
            if (!form.checkValidity()) {
                form.reportValidity();
                return;
            }

            const hubName = document.getElementById('modbusDeviceHubName').value;
            const editingName = document.getElementById('modbusDeviceEditName').value;
            const device = {
                name: document.getElementById('modbusDeviceName').value,
                meter: document.getElementById('modbusDeviceMeter').value,
                slave_id: parseInt(document.getElementById('modbusDeviceSlaveId').value),
                read_interval: parseInt(document.getElementById('modbusDeviceReadInterval').value)
            };

            try {
                if (editingName) {
                    // Update: delete old and add new
                    await window.api.deleteModbusDevice(hubName, editingName);
                    await window.api.addModbusDevice(hubName, device);
                    Toast.success(`Device "${device.name}" updated`);
                } else {
                    await window.api.addModbusDevice(hubName, device);
                    Toast.success(`Device "${device.name}" added`);
                }
                DialogManager.hide();
                await this.loadModbusData();
            } catch (error) {
                Toast.error(`${error.message}`);
            }
        });

        // KNX Adapter Form
        document.getElementById('knxAdapterSubmit')?.addEventListener('click', async () => {
            const form = document.getElementById('knxAdapterForm');
            if (!form.checkValidity()) {
                form.reportValidity();
                return;
            }

            const editingName = document.getElementById('knxAdapterEditName').value;

            // Get current adapter data to preserve meters/switches if editing
            let existingMeters = [];
            let existingSwitches = [];
            if (editingName) {
                try {
                    const config = await window.api.getKnxConfig();
                    const existingAdapter = config?.find(a => a.name === editingName);
                    if (existingAdapter) {
                        existingMeters = existingAdapter.meters || [];
                        existingSwitches = existingAdapter.switches || [];
                    }
                } catch (e) {
                    console.error('Failed to get existing adapter:', e);
                }
            }

            const adapter = {
                name: document.getElementById('knxAdapterName').value,
                host: document.getElementById('knxAdapterHost').value,
                port: parseInt(document.getElementById('knxAdapterPort').value) || 3671,
                enabled: document.getElementById('knxAdapterEnabled').checked,
                connection_timeout: parseInt(document.getElementById('knxAdapterConnTimeout').value) || 10,
                read_timeout: parseInt(document.getElementById('knxAdapterReadTimeout').value) || 5,
                read_delay_ms: parseInt(document.getElementById('knxAdapterReadDelayMs').value) || 100,
                meters: existingMeters,
                switches: existingSwitches
            };

            try {
                if (editingName) {
                    await window.api.updateKnxAdapter(editingName, adapter);
                    Toast.success(`Adapter "${adapter.name}" updated`);
                } else {
                    await window.api.addKnxAdapter(adapter);
                    Toast.success(`Adapter "${adapter.name}" created`);
                }
                DialogManager.hide();
                await this.loadKnxData();
                this.config = await window.api.getConfig();
                this.updateOverviewStats();
            } catch (error) {
                Toast.error(`${error.message}`);
            }
        });

        // KNX Meter Form
        document.getElementById('knxMeterSubmit')?.addEventListener('click', async () => {
            const form = document.getElementById('knxMeterForm');
            if (!form.checkValidity()) {
                form.reportValidity();
                return;
            }

            const adapterName = document.getElementById('knxMeterAdapterName').value;
            const phaseMode = document.getElementById('knxMeterPhaseMode').value;

            const meter = {
                name: document.getElementById('knxMeterName').value,
                manufacturer: document.getElementById('knxMeterManufacturer').value || undefined,
                model: document.getElementById('knxMeterModel').value || undefined,
                read_interval: parseInt(document.getElementById('knxMeterReadInterval').value) || 60,
                enabled: document.getElementById('knxMeterEnabled').checked
            };

            if (phaseMode === 'single') {
                // Single phase configuration
                meter.voltage_ga = document.getElementById('knxMeterVoltageGa').value || undefined;
                meter.current_ga = document.getElementById('knxMeterCurrentGa').value || undefined;
                meter.power_ga = document.getElementById('knxMeterPowerGa').value || undefined;
                meter.energy_ga = document.getElementById('knxMeterEnergyGa').value || undefined;
                meter.voltage_type = document.getElementById('knxMeterVoltageType').value;
                meter.current_type = document.getElementById('knxMeterCurrentType').value;
                meter.power_type = document.getElementById('knxMeterPowerType').value;
                meter.energy_type = document.getElementById('knxMeterEnergyType').value;
                // Switch control
                meter.switch_ga = document.getElementById('knxMeterSwitchGa').value || undefined;
                meter.switch_state_ga = document.getElementById('knxMeterSwitchStateGa').value || undefined;
            } else {
                // Multi-phase configuration
                const phases = [];
                const voltageType = document.getElementById('knxMeterPhaseVoltageType').value;
                const currentType = document.getElementById('knxMeterPhaseCurrentType').value;
                const powerType = document.getElementById('knxMeterPhasePowerType').value;
                const energyType = document.getElementById('knxMeterPhaseEnergyType').value;

                for (let i = 1; i <= 3; i++) {
                    const phaseName = document.getElementById(`knxMeterPhase${i}Name`).value;
                    const voltageGa = document.getElementById(`knxMeterPhase${i}VoltageGa`).value;
                    const currentGa = document.getElementById(`knxMeterPhase${i}CurrentGa`).value;
                    const powerGa = document.getElementById(`knxMeterPhase${i}PowerGa`).value;
                    const energyGa = document.getElementById(`knxMeterPhase${i}EnergyGa`).value;
                    const switchGa = document.getElementById(`knxMeterPhase${i}SwitchGa`).value;
                    const switchStateGa = document.getElementById(`knxMeterPhase${i}SwitchStateGa`).value;

                    // Only add phase if it has at least one GA defined
                    if (voltageGa || currentGa || powerGa || energyGa || switchGa) {
                        const phase = {
                            name: phaseName || `L${i}`,
                            voltage_type: voltageType,
                            current_type: currentType,
                            power_type: powerType,
                            energy_type: energyType
                        };
                        if (voltageGa) phase.voltage_ga = voltageGa;
                        if (currentGa) phase.current_ga = currentGa;
                        if (powerGa) phase.power_ga = powerGa;
                        if (energyGa) phase.energy_ga = energyGa;
                        if (switchGa) phase.switch_ga = switchGa;
                        if (switchStateGa) phase.switch_state_ga = switchStateGa;
                        phases.push(phase);
                    }
                }

                if (phases.length > 0) {
                    meter.phases = phases;
                }

                // Total values
                meter.total_power_ga = document.getElementById('knxMeterTotalPowerGa').value || undefined;
                meter.total_energy_ga = document.getElementById('knxMeterTotalEnergyGa').value || undefined;
                meter.total_current_ga = document.getElementById('knxMeterTotalCurrentGa').value || undefined;
                meter.calculate_totals = document.getElementById('knxMeterCalculateTotals').checked;

                // Global switch control for multi-phase
                meter.switch_ga = document.getElementById('knxMeterMultiSwitchGa').value || undefined;
                meter.switch_state_ga = document.getElementById('knxMeterMultiSwitchStateGa').value || undefined;
            }

            // Remove undefined values
            Object.keys(meter).forEach(key => meter[key] === undefined && delete meter[key]);

            const editingName = document.getElementById('knxMeterEditName').value;

            try {
                if (editingName) {
                    // Update: delete old and add new
                    await window.api.deleteKnxMeter(adapterName, editingName);
                    await window.api.addKnxMeter(adapterName, meter);
                    Toast.success(`Meter "${meter.name}" updated`);
                } else {
                    await window.api.addKnxMeter(adapterName, meter);
                    Toast.success(`Meter "${meter.name}" added`);
                }
                DialogManager.hide();
                await this.loadKnxData();
            } catch (error) {
                Toast.error(`${error.message}`);
            }
        });

        // KNX Switch Form
        document.getElementById('knxSwitchSubmit')?.addEventListener('click', async () => {
            const form = document.getElementById('knxSwitchForm');
            if (!form.checkValidity()) {
                form.reportValidity();
                return;
            }

            const adapterName = document.getElementById('knxSwitchAdapterName').value;
            const switchConfig = {
                name: document.getElementById('knxSwitchName').value,
                group_address: document.getElementById('knxSwitchGroupAddress').value,
                state_address: document.getElementById('knxSwitchStateAddress').value || undefined,
                expose_to_ha: document.getElementById('knxSwitchExposeToHa').checked,
                enabled: document.getElementById('knxSwitchEnabled').checked
            };

            // Remove undefined values
            Object.keys(switchConfig).forEach(key => switchConfig[key] === undefined && delete switchConfig[key]);

            const editingName = document.getElementById('knxSwitchEditName').value;

            try {
                if (editingName) {
                    // Update: delete old and add new
                    await window.api.deleteKnxSwitch(adapterName, editingName);
                    await window.api.addKnxSwitch(adapterName, switchConfig);
                    Toast.success(`Switch "${switchConfig.name}" updated`);
                } else {
                    await window.api.addKnxSwitch(adapterName, switchConfig);
                    Toast.success(`Switch "${switchConfig.name}" added`);
                }
                DialogManager.hide();
                await this.loadKnxData();
            } catch (error) {
                Toast.error(`${error.message}`);
            }
        });

        // Zenner Datahub Instance Form
        document.getElementById('zennerInstanceSubmit')?.addEventListener('click', async () => {
            const form = document.getElementById('zennerInstanceForm');
            if (!form.checkValidity()) {
                form.reportValidity();
                return;
            }

            const instance = {
                name: document.getElementById('zennerInstanceName').value,
                broker_host: document.getElementById('zennerBrokerHost').value,
                broker_port: parseInt(document.getElementById('zennerBrokerPort').value) || 1883,
                broker_user: document.getElementById('zennerBrokerUser').value,
                broker_pass: document.getElementById('zennerBrokerPass').value,
                client_name: document.getElementById('zennerClientName').value || '',
                base_topic: document.getElementById('zennerBaseTopic').value,
                update_interval: parseInt(document.getElementById('zennerUpdateInterval').value) || 60,
                enabled: document.getElementById('zennerEnabled').checked
            };

            const editingName = document.getElementById('zennerInstanceEditName').value;

            try {
                if (editingName) {
                    await window.api.updateZennerInstance(editingName, instance);
                    Toast.success(`Instance "${instance.name}" updated`);
                } else {
                    await window.api.addZennerInstance(instance);
                    Toast.success(`Instance "${instance.name}" created`);
                }
                DialogManager.hide();
                await this.loadZennerData();
                this.config = await window.api.getConfig();
                this.updateOverviewStats();
            } catch (error) {
                Toast.error(`${error.message}`);
            }
        });

        // OMS Meter Form
        document.getElementById('omsMeterSubmit')?.addEventListener('click', async () => {
            const form = document.getElementById('omsMeterForm');
            if (!form.checkValidity()) {
                form.reportValidity();
                return;
            }

            const meter = {
                name: document.getElementById('omsMeterName').value,
                id: document.getElementById('omsMeterDeviceId').value.toUpperCase(),
                key: document.getElementById('omsMeterKey').value.toUpperCase()
            };

            // Additional validation for key format
            if (!/^[0-9A-F]{32}$/.test(meter.key)) {
                Toast.error('Encryption key must be exactly 32 hexadecimal characters');
                return;
            }

            const editingName = document.getElementById('omsMeterEditName').value;

            try {
                if (editingName) {
                    await window.api.updateOmsMeter(editingName, meter);
                    Toast.success(`Meter "${meter.name}" updated`);
                } else {
                    await window.api.addOmsMeter(meter);
                    Toast.success(`Meter "${meter.name}" created`);
                }
                DialogManager.hide();
                await this.loadOmsData();
                this.config = await window.api.getConfig();
                this.updateOverviewStats();
            } catch (error) {
                Toast.error(`${error.message}`);
            }
        });
    }

    // =========================================================================
    // Action Buttons
    // =========================================================================

    setupActionButtons() {
        // Add Modbus Hub
        document.getElementById('addModbusHub')?.addEventListener('click', () => {
            document.getElementById('modbusHubForm').reset();
            document.getElementById('modbusHubDialogTitle').textContent = 'Add Modbus Hub';
            document.getElementById('modbusHubEditName').value = '';
            document.getElementById('modbusHubPort').value = 502;
            document.getElementById('modbusHubConnTimeout').value = 10;
            document.getElementById('modbusHubReadTimeout').value = 5;
            document.getElementById('modbusHubSubmit').textContent = 'Add Hub';
            DialogManager.show('modbusHubDialog');
        });

        // Add KNX Adapter
        document.getElementById('addKnxAdapter')?.addEventListener('click', () => {
            document.getElementById('knxAdapterForm').reset();
            document.getElementById('knxAdapterDialogTitle').textContent = 'Add KNX Adapter';
            document.getElementById('knxAdapterEditName').value = '';
            document.getElementById('knxAdapterPort').value = 3671;
            document.getElementById('knxAdapterEnabled').checked = true;
            document.getElementById('knxAdapterConnTimeout').value = 10;
            document.getElementById('knxAdapterReadTimeout').value = 5;
            document.getElementById('knxAdapterReadDelayMs').value = 100;
            document.getElementById('knxAdapterSubmit').textContent = 'Add Adapter';
            DialogManager.show('knxAdapterDialog');
        });

        // Add Zenner Instance
        document.getElementById('addZennerInstance')?.addEventListener('click', () => {
            document.getElementById('zennerInstanceForm').reset();
            document.getElementById('zennerInstanceDialogTitle').textContent = 'Add Zenner Datahub Instance';
            document.getElementById('zennerBrokerPort').value = 1883;
            document.getElementById('zennerUpdateInterval').value = 60;
            document.getElementById('zennerEnabled').checked = true;
            document.getElementById('zennerInstanceEditName').value = '';
            document.getElementById('zennerInstanceSubmit').textContent = 'Add Instance';
            DialogManager.show('zennerInstanceDialog');
        });

        // Add OMS Meter
        document.getElementById('addOmsMeter')?.addEventListener('click', () => {
            document.getElementById('omsMeterForm').reset();
            document.getElementById('omsMeterDialogTitle').textContent = 'Add OMS Meter';
            document.getElementById('omsMeterEditName').value = '';
            document.getElementById('omsMeterSubmit').textContent = 'Add Meter';
            DialogManager.show('omsMeterDialog');
        });

        // Refresh button
        document.getElementById('refreshBtn')?.addEventListener('click', async () => {
            await this.loadInitialData();
            await this.loadPageData(this.currentPage);
            Toast.info('Data refreshed');
        });

        // Save config
        document.getElementById('saveConfig')?.addEventListener('click', async () => {
            try {
                await window.api.saveConfig();
                Toast.success('Configuration saved');
            } catch (error) {
                Toast.error(`Failed to save: ${error.message}`);
            }
        });

        // Reload config
        document.getElementById('reloadConfig')?.addEventListener('click', async () => {
            try {
                await window.api.reloadConfig();
                Toast.success('Configuration reloaded');
                await this.loadInitialData();
            } catch (error) {
                Toast.error(`Failed to reload: ${error.message}`);
            }
        });
    }

    // =========================================================================
    // Connection Status
    // =========================================================================

    setupConnectionStatus() {
        window.addEventListener('api:connection', (e) => {
            const statusEl = document.getElementById('connectionStatus');
            const dot = statusEl.querySelector('.status-dot');
            const text = statusEl.querySelector('.status-text');

            if (e.detail.connected) {
                dot.className = 'status-dot connected';
                text.textContent = 'Connected';
            } else {
                dot.className = 'status-dot';
                text.textContent = 'Reconnecting...';
            }
        });

        // Listen for config changes via WebSocket
        window.addEventListener('api:configChange', (e) => {
            const change = e.detail;
            console.log('Config changed:', change);

            // Refresh relevant page
            if (change.base === 'modbus' && this.currentPage === 'modbus') {
                this.loadModbusData();
            } else if (change.base === 'knx' && this.currentPage === 'knx') {
                this.loadKnxData();
            } else if (change.base === 'zenner' && this.currentPage === 'zenner') {
                this.loadZennerData();
            }

            // Update overview stats
            this.loadInitialData();
        });
    }

    // =========================================================================
    // Refresh Events
    // =========================================================================

    setupRefreshEvents() {
        window.addEventListener('modbus:refresh', () => {
            this.loadModbusData();
            this.loadInitialData();
        });

        window.addEventListener('knx:refresh', () => {
            this.loadKnxData();
            this.loadInitialData();
        });

        window.addEventListener('zenner:refresh', () => {
            this.loadZennerData();
            this.loadInitialData();
        });

        window.addEventListener('oms:refresh', () => {
            this.loadOmsData();
            this.loadInitialData();
        });
    }

    // =========================================================================
    // Health Polling
    // =========================================================================

    startHealthPolling() {
        // Poll health every 30 seconds
        setInterval(() => {
            this.loadHealthData();
        }, 30000);
    }

    // =========================================================================
    // Unsaved Changes Indicator
    // =========================================================================

    setupUnsavedIndicator() {
        const saveBtn = document.getElementById('saveNowBtn');
        if (saveBtn) {
            saveBtn.addEventListener('click', async () => {
                await this.saveConfigNow();
            });
        }
    }

    async saveConfigNow() {
        const indicator = document.getElementById('unsavedIndicator');
        const saveBtn = document.getElementById('saveNowBtn');

        try {
            // Disable button and show saving state
            if (saveBtn) {
                saveBtn.disabled = true;
                saveBtn.innerHTML = '<span class="material-icons rotating">sync</span>';
            }

            await window.api.saveConfig();
            Toast.success('Configuration saved successfully');

            // Hide the indicator
            if (indicator) {
                indicator.classList.add('hidden');
            }
        } catch (error) {
            console.error('Failed to save config:', error);
            Toast.error('Failed to save configuration: ' + error.message);
        } finally {
            // Restore button state
            if (saveBtn) {
                saveBtn.disabled = false;
                saveBtn.innerHTML = '<span class="material-icons">save</span>';
            }
        }
    }

    startConfigStatusPolling() {
        // Check config status immediately
        this.checkConfigStatus();

        // Poll config status every 5 seconds
        setInterval(() => {
            this.checkConfigStatus();
        }, 5000);
    }

    async checkConfigStatus() {
        try {
            const status = await window.api.getConfigStatus();
            const indicator = document.getElementById('unsavedIndicator');

            if (indicator) {
                if (status && status.dirty) {
                    indicator.classList.remove('hidden');
                } else {
                    indicator.classList.add('hidden');
                }
            }
        } catch (error) {
            // Silently fail - don't spam console with errors if API is temporarily unavailable
        }
    }
}

// Initialize app when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    window.app = new Energy2MqttApp();
});
