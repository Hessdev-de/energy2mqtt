/**
 * Energy2MQTT Live View
 * Real-time MQTT traffic monitoring with JSON syntax highlighting
 */

class LiveView {
    constructor() {
        this.messages = [];
        this.maxMessages = 500;
        this.isPaused = false;
        this.wsConnection = null;
        this.filters = {
            outgoing: true,
            incoming: true,
            metering: true,
            discovery: true,
            commands: true
        };
        this.messageCount = 0;
    }

    init() {
        this.container = document.getElementById('liveMessages');
        this.messageCountEl = document.getElementById('liveMessageCount');
        this.liveIndicator = document.getElementById('liveIndicator');

        this.setupControls();
        this.setupFilters();
    }

    setupControls() {
        // Pause/Resume button
        document.getElementById('liveTogglePause')?.addEventListener('click', () => {
            this.togglePause();
        });

        // Clear button
        document.getElementById('liveClear')?.addEventListener('click', () => {
            this.clearMessages();
        });
    }

    setupFilters() {
        const filterIds = {
            'filterOutgoing': 'outgoing',
            'filterIncoming': 'incoming',
            'filterMetering': 'metering',
            'filterDiscovery': 'discovery',
            'filterCommands': 'commands'
        };

        Object.entries(filterIds).forEach(([elementId, filterKey]) => {
            document.getElementById(elementId)?.addEventListener('change', (e) => {
                this.filters[filterKey] = e.target.checked;
                this.rerenderMessages();
            });
        });
    }

    connect() {
        if (this.wsConnection && this.wsConnection.readyState === WebSocket.OPEN) {
            return;
        }

        const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsUrl = `${wsProtocol}//${window.location.host}/api/v1/ws/live`;

        try {
            this.wsConnection = new WebSocket(wsUrl);

            this.wsConnection.onopen = () => {
                console.log('Live view WebSocket connected');
                this.updateIndicator(true);
            };

            this.wsConnection.onmessage = (event) => {
                if (this.isPaused) return;

                try {
                    const data = JSON.parse(event.data);
                    this.addMessage(data);
                } catch (e) {
                    console.error('Failed to parse live event:', e);
                }
            };

            this.wsConnection.onclose = () => {
                console.log('Live view WebSocket disconnected');
                this.updateIndicator(false);
                // Reconnect after delay
                setTimeout(() => this.connect(), 3000);
            };

            this.wsConnection.onerror = (error) => {
                console.error('Live view WebSocket error:', error);
            };
        } catch (error) {
            console.error('Failed to create WebSocket connection:', error);
        }
    }

    disconnect() {
        if (this.wsConnection) {
            this.wsConnection.close();
            this.wsConnection = null;
        }
    }

    togglePause() {
        this.isPaused = !this.isPaused;
        const btn = document.getElementById('liveTogglePause');
        const icon = btn?.querySelector('.material-icons');
        const indicator = document.getElementById('liveIndicator');
        const liveText = indicator?.querySelector('.live-text');

        if (this.isPaused) {
            icon.textContent = 'play_arrow';
            indicator?.classList.remove('receiving');
            indicator?.classList.add('paused');
            if (liveText) liveText.textContent = 'Paused';
        } else {
            icon.textContent = 'pause';
            indicator?.classList.remove('paused');
            if (liveText) liveText.textContent = 'Waiting';
        }
    }

    clearMessages() {
        this.messages = [];
        this.messageCount = 0;
        this.updateMessageCount();
        this.container.innerHTML = `
            <div class="live-view-empty">
                <span class="material-icons">stream</span>
                <p>Waiting for MQTT traffic...</p>
                <p class="hint">Messages will appear here in real-time</p>
            </div>
        `;
    }

    addMessage(event) {
        // Flash the indicator to show we're receiving
        this.flashReceiving();

        // Remove empty state if present
        const emptyState = this.container.querySelector('.live-view-empty');
        if (emptyState) {
            emptyState.remove();
        }

        // Add to messages array
        this.messages.unshift(event);
        this.messageCount++;

        // Trim old messages
        if (this.messages.length > this.maxMessages) {
            this.messages.pop();
            const lastChild = this.container.lastElementChild;
            if (lastChild) {
                lastChild.remove();
            }
        }

        // Check if message passes filters
        if (this.shouldShowMessage(event)) {
            const messageEl = this.createMessageElement(event);
            this.container.insertBefore(messageEl, this.container.firstChild);
        }

        this.updateMessageCount();
    }

    shouldShowMessage(event) {
        // Direction filter
        if (event.direction === 'Outgoing' && !this.filters.outgoing) return false;
        if (event.direction === 'Incoming' && !this.filters.incoming) return false;

        // Type filter
        const eventType = event.event_type;
        if (eventType === 'Metering' && !this.filters.metering) return false;
        if (eventType === 'AutoDiscovery' && !this.filters.discovery) return false;
        if ((eventType === 'Command' || eventType === 'Publish') && !this.filters.commands) return false;

        return true;
    }

    createMessageElement(event) {
        const el = document.createElement('div');
        el.className = `live-message ${event.direction.toLowerCase()}`;

        // Add type-specific class
        const typeClass = this.getTypeClass(event.event_type);
        if (typeClass) {
            el.classList.add(typeClass);
        }

        const timestamp = new Date(event.timestamp).toLocaleTimeString('en-US', {
            hour12: false,
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit',
            fractionalSecondDigits: 3
        });

        const directionIcon = event.direction === 'Outgoing' ? 'arrow_upward' : 'arrow_downward';

        el.innerHTML = `
            <div class="live-message-header">
                <div class="live-message-direction ${event.direction.toLowerCase()}">
                    <span class="material-icons">${directionIcon}</span>
                </div>
                <span class="live-message-time">${timestamp}</span>
                <span class="live-message-type">${event.event_type}</span>
                <span class="live-message-topic" title="${this.escapeHtml(event.topic)}">${this.escapeHtml(event.topic)}</span>
                <div class="live-message-meta">
                    ${event.retain ? '<span class="badge">Retain</span>' : ''}
                    ${event.qos !== undefined ? `<span class="badge">QoS ${event.qos}</span>` : ''}
                </div>
                <span class="material-icons live-message-expand">expand_more</span>
            </div>
            <div class="live-message-body">
                <div class="json-view">${this.highlightJson(event.payload)}</div>
            </div>
        `;

        // Toggle expand on header click
        el.querySelector('.live-message-header').addEventListener('click', () => {
            el.classList.toggle('expanded');
        });

        return el;
    }

    getTypeClass(eventType) {
        const typeClasses = {
            'Metering': 'metering',
            'AutoDiscovery': 'discovery',
            'System': 'system'
        };
        return typeClasses[eventType] || '';
    }

    highlightJson(obj) {
        try {
            const json = typeof obj === 'string' ? obj : JSON.stringify(obj, null, 2);
            return this.syntaxHighlight(json);
        } catch (e) {
            return `<span class="json-raw">${this.escapeHtml(String(obj))}</span>`;
        }
    }

    syntaxHighlight(json) {
        // Escape HTML first
        json = this.escapeHtml(json);

        // Apply syntax highlighting
        return json.replace(
            /("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g,
            (match) => {
                let cls = 'json-number';
                if (/^"/.test(match)) {
                    if (/:$/.test(match)) {
                        cls = 'json-key';
                        // Remove the colon from the match and add it separately
                        match = match.slice(0, -1);
                        return `<span class="${cls}">${match}</span>:`;
                    } else {
                        cls = 'json-string';
                    }
                } else if (/true|false/.test(match)) {
                    cls = 'json-boolean';
                } else if (/null/.test(match)) {
                    cls = 'json-null';
                }
                return `<span class="${cls}">${match}</span>`;
            }
        );
    }

    escapeHtml(str) {
        const div = document.createElement('div');
        div.textContent = str;
        return div.innerHTML;
    }

    rerenderMessages() {
        this.container.innerHTML = '';

        const visibleMessages = this.messages.filter(m => this.shouldShowMessage(m));

        if (visibleMessages.length === 0) {
            this.container.innerHTML = `
                <div class="live-view-empty">
                    <span class="material-icons">filter_list</span>
                    <p>No messages match current filters</p>
                    <p class="hint">Try adjusting the filter settings above</p>
                </div>
            `;
            return;
        }

        visibleMessages.forEach(event => {
            const messageEl = this.createMessageElement(event);
            this.container.appendChild(messageEl);
        });
    }

    updateMessageCount() {
        if (this.messageCountEl) {
            this.messageCountEl.textContent = `${this.messageCount} message${this.messageCount !== 1 ? 's' : ''}`;
        }
    }

    updateIndicator(connected) {
        if (!this.liveIndicator) return;

        const liveText = this.liveIndicator.querySelector('.live-text');

        this.liveIndicator.classList.remove('receiving', 'paused', 'disconnected');

        if (!connected) {
            this.liveIndicator.classList.add('disconnected');
            if (liveText) liveText.textContent = 'Disconnected';
        } else if (this.isPaused) {
            this.liveIndicator.classList.add('paused');
            if (liveText) liveText.textContent = 'Paused';
        } else {
            if (liveText) liveText.textContent = 'Waiting';
        }
    }

    flashReceiving() {
        if (!this.liveIndicator || this.isPaused) return;

        const liveText = this.liveIndicator.querySelector('.live-text');
        this.liveIndicator.classList.add('receiving');
        if (liveText) liveText.textContent = 'Live';

        // Reset to waiting after a brief period of no messages
        clearTimeout(this.receiveTimeout);
        this.receiveTimeout = setTimeout(() => {
            if (!this.isPaused) {
                this.liveIndicator.classList.remove('receiving');
                if (liveText) liveText.textContent = 'Waiting';
            }
        }, 2000);
    }
}

// Create global instance
window.liveView = new LiveView();
