/**
 * Energy2MQTT API Client
 * Handles all communication with the backend API
 */

class Energy2MqttApi {
    constructor(baseUrl = '') {
        this.baseUrl = baseUrl || window.location.origin;
        this.wsConnection = null;
        this.wsReconnectAttempts = 0;
        this.maxReconnectAttempts = 10;
        this.reconnectDelay = 1000;
        this.configChangeCallbacks = [];
    }

    // =========================================================================
    // HTTP Methods
    // =========================================================================

    async request(method, endpoint, data = null) {
        const url = `${this.baseUrl}${endpoint}`;
        const options = {
            method,
            headers: {
                'Content-Type': 'application/json',
            },
        };

        if (data) {
            options.body = JSON.stringify(data);
        }

        try {
            const response = await fetch(url, options);

            if (!response.ok) {
                const errorText = await response.text();
                throw new Error(`HTTP ${response.status}: ${errorText || response.statusText}`);
            }

            // Handle empty responses
            const text = await response.text();
            if (!text) {
                return null;
            }

            // Only parse as JSON if Content-Type indicates JSON
            const contentType = response.headers.get('Content-Type') || '';
            if (contentType.includes('application/json')) {
                return JSON.parse(text);
            }

            // For non-JSON responses, return null (success acknowledged)
            return null;
        } catch (error) {
            console.error(`API Error [${method} ${endpoint}]:`, error);
            throw error;
        }
    }

    async get(endpoint) {
        return this.request('GET', endpoint);
    }

    async post(endpoint, data) {
        return this.request('POST', endpoint, data);
    }

    async put(endpoint, data) {
        return this.request('PUT', endpoint, data);
    }

    async delete(endpoint) {
        return this.request('DELETE', endpoint);
    }

    // =========================================================================
    // Health & System
    // =========================================================================

    async getHealth() {
        return this.get('/health');
    }

    async getConfig() {
        return this.get('/api/v1/config');
    }

    async getConfigStatus() {
        return this.get('/api/v1/config/status');
    }

    async saveConfig() {
        return this.post('/api/v1/ha/config/save');
    }

    async reloadConfig() {
        return this.post('/api/v1/ha/config/reload');
    }

    async restart() {
        return this.post('/api/v1/ha/restart');
    }

    // =========================================================================
    // Modbus API
    // =========================================================================

    async getModbusConfig() {
        return this.get('/api/v1/modbus');
    }

    async addModbusHub(hub) {
        return this.post('/api/v1/modbus', hub);
    }

    async updateModbusHub(name, hub) {
        return this.put(`/api/v1/modbus/${encodeURIComponent(name)}`, hub);
    }

    async deleteModbusHub(name) {
        return this.delete(`/api/v1/modbus/${encodeURIComponent(name)}`);
    }

    async addModbusDevice(hubName, device) {
        // Note: This endpoint may need to be implemented in the backend
        // For now, we'll update the entire hub config
        const config = await this.getModbusConfig();
        const hub = config.hubs.find(h => h.name === hubName);
        if (!hub) {
            throw new Error(`Hub "${hubName}" not found`);
        }
        hub.devices.push(device);
        return this.updateModbusHub(hubName, hub);
    }

    async deleteModbusDevice(hubName, deviceName) {
        const config = await this.getModbusConfig();
        const hub = config.hubs.find(h => h.name === hubName);
        if (!hub) {
            throw new Error(`Hub "${hubName}" not found`);
        }
        hub.devices = hub.devices.filter(d => d.name !== deviceName);
        return this.updateModbusHub(hubName, hub);
    }

    // =========================================================================
    // KNX API
    // =========================================================================

    async getKnxConfig() {
        return this.get('/api/v1/knx');
    }

    async addKnxAdapter(adapter) {
        return this.post('/api/v1/knx', adapter);
    }

    async updateKnxAdapter(name, adapter) {
        return this.put(`/api/v1/knx/${encodeURIComponent(name)}`, adapter);
    }

    async deleteKnxAdapter(name) {
        return this.delete(`/api/v1/knx/${encodeURIComponent(name)}`);
    }

    async addKnxMeter(adapterName, meter) {
        return this.post(`/api/v1/knx/${encodeURIComponent(adapterName)}/meters`, meter);
    }

    async deleteKnxMeter(adapterName, meterName) {
        return this.delete(`/api/v1/knx/${encodeURIComponent(adapterName)}/meters/${encodeURIComponent(meterName)}`);
    }

    async addKnxSwitch(adapterName, switchConfig) {
        return this.post(`/api/v1/knx/${encodeURIComponent(adapterName)}/switches`, switchConfig);
    }

    async deleteKnxSwitch(adapterName, switchName) {
        return this.delete(`/api/v1/knx/${encodeURIComponent(adapterName)}/switches/${encodeURIComponent(switchName)}`);
    }

    // =========================================================================
    // Zenner Datahub API
    // =========================================================================

    async getZennerConfig() {
        return this.get('/api/v1/zenner');
    }

    async addZennerInstance(instance) {
        return this.post('/api/v1/zenner', instance);
    }

    async updateZennerInstance(name, instance) {
        return this.put(`/api/v1/zenner/${encodeURIComponent(name)}`, instance);
    }

    async deleteZennerInstance(name) {
        return this.delete(`/api/v1/zenner/${encodeURIComponent(name)}`);
    }

    // =========================================================================
    // OMS API
    // =========================================================================

    async getOmsConfig() {
        return this.get('/api/v1/oms');
    }

    async addOmsMeter(meter) {
        return this.post('/api/v1/oms', meter);
    }

    async updateOmsMeter(name, meter) {
        return this.put(`/api/v1/oms/${encodeURIComponent(name)}`, meter);
    }

    async deleteOmsMeter(name) {
        return this.delete(`/api/v1/oms/${encodeURIComponent(name)}`);
    }

    // =========================================================================
    // WebSocket Connection
    // =========================================================================

    connectWebSocket() {
        if (this.wsConnection && this.wsConnection.readyState === WebSocket.OPEN) {
            return;
        }

        const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsUrl = `${wsProtocol}//${window.location.host}/api/v1/ws/configChanges`;

        try {
            this.wsConnection = new WebSocket(wsUrl);

            this.wsConnection.onopen = () => {
                console.log('WebSocket connected');
                this.wsReconnectAttempts = 0;
                this.onConnectionChange(true);
            };

            this.wsConnection.onmessage = (event) => {
                try {
                    const data = JSON.parse(event.data);
                    console.log('WebSocket message:', data);
                    this.notifyConfigChange(data);
                } catch (e) {
                    console.error('Failed to parse WebSocket message:', e);
                }
            };

            this.wsConnection.onclose = () => {
                console.log('WebSocket disconnected');
                this.onConnectionChange(false);
                this.scheduleReconnect();
            };

            this.wsConnection.onerror = (error) => {
                console.error('WebSocket error:', error);
                this.onConnectionChange(false);
            };
        } catch (error) {
            console.error('Failed to create WebSocket connection:', error);
            this.scheduleReconnect();
        }
    }

    scheduleReconnect() {
        if (this.wsReconnectAttempts >= this.maxReconnectAttempts) {
            console.error('Max WebSocket reconnect attempts reached');
            return;
        }

        this.wsReconnectAttempts++;
        const delay = this.reconnectDelay * Math.pow(2, this.wsReconnectAttempts - 1);
        console.log(`Reconnecting WebSocket in ${delay}ms (attempt ${this.wsReconnectAttempts})`);

        setTimeout(() => {
            this.connectWebSocket();
        }, delay);
    }

    disconnectWebSocket() {
        if (this.wsConnection) {
            this.wsConnection.close();
            this.wsConnection = null;
        }
    }

    // =========================================================================
    // Event Callbacks
    // =========================================================================

    onConnectionChange(connected) {
        // Override this method to handle connection status changes
        window.dispatchEvent(new CustomEvent('api:connection', { detail: { connected } }));
    }

    onConfigChange(callback) {
        this.configChangeCallbacks.push(callback);
        return () => {
            this.configChangeCallbacks = this.configChangeCallbacks.filter(cb => cb !== callback);
        };
    }

    notifyConfigChange(change) {
        this.configChangeCallbacks.forEach(callback => {
            try {
                callback(change);
            } catch (e) {
                console.error('Config change callback error:', e);
            }
        });
        window.dispatchEvent(new CustomEvent('api:configChange', { detail: change }));
    }
}

// Create global API instance
window.api = new Energy2MqttApi();
