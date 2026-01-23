/**
 * Energy2MQTT Setup Wizard
 * First-time configuration wizard for MQTT setup
 */

class SetupWizard {
    constructor() {
        this.currentStep = 1;
        this.totalSteps = 4;
        this.testResult = null;
        this.overlay = document.getElementById('setupWizard');
    }

    async checkIfNeeded() {
        try {
            const response = await fetch('/api/v1/setup/status');
            const data = await response.json();

            if (data.needs_setup) {
                this.show();
                return true;
            } else {
                this.hide();
                return false;
            }
        } catch (error) {
            console.error('Failed to check setup status:', error);
            // On error, hide the wizard and let the app try to work
            this.hide();
            return false;
        }
    }

    show() {
        this.overlay.classList.remove('hidden');
        document.body.style.overflow = 'hidden';
    }

    hide() {
        this.overlay.classList.add('hidden');
        document.body.style.overflow = '';
    }

    goToStep(step) {
        if (step < 1 || step > this.totalSteps) return;

        // Hide all steps
        document.querySelectorAll('.setup-step').forEach(el => {
            el.classList.remove('active');
        });

        // Show target step
        document.getElementById(`setupStep${step}`).classList.add('active');

        // Update progress
        document.querySelectorAll('.progress-step').forEach(el => {
            const stepNum = parseInt(el.dataset.step);
            el.classList.remove('active', 'completed');
            if (stepNum < step) {
                el.classList.add('completed');
            } else if (stepNum === step) {
                el.classList.add('active');
            }
        });

        this.currentStep = step;

        // Update review if going to step 3
        if (step === 3) {
            this.updateReview();
        }
    }

    nextStep() {
        // Validate current step before proceeding
        if (this.currentStep === 2) {
            const form = document.getElementById('setupMqttForm');
            if (!form.checkValidity()) {
                form.reportValidity();
                return;
            }
        }

        this.goToStep(this.currentStep + 1);
    }

    prevStep() {
        this.goToStep(this.currentStep - 1);
    }

    getFormData() {
        return {
            host: document.getElementById('setupMqttHost').value.trim(),
            port: parseInt(document.getElementById('setupMqttPort').value) || 1883,
            user: document.getElementById('setupMqttUser').value.trim(),
            pass: document.getElementById('setupMqttPass').value,
            ha_enabled: document.getElementById('setupMqttHaEnabled').checked,
            client_name: document.getElementById('setupMqttClientName').value.trim() || 'energy2mqtt'
        };
    }

    updateReview() {
        const data = this.getFormData();
        document.getElementById('reviewHost').textContent = data.host || '-';
        document.getElementById('reviewPort').textContent = data.port;
        document.getElementById('reviewUser').textContent = data.user || '(none)';
        document.getElementById('reviewHa').textContent = data.ha_enabled ? 'Enabled' : 'Disabled';
    }

    async testConnection() {
        const resultEl = document.getElementById('setupTestResult');
        const iconEl = resultEl.querySelector('.test-icon');
        const messageEl = resultEl.querySelector('.test-message');

        // Show testing state
        resultEl.className = 'setup-test-result testing';
        iconEl.textContent = 'sync';
        messageEl.textContent = 'Testing connection...';

        try {
            const data = this.getFormData();
            const response = await fetch('/api/v1/setup/mqtt/test', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(data)
            });

            const result = await response.json();
            this.testResult = result;

            if (result.success) {
                resultEl.className = 'setup-test-result success';
                iconEl.textContent = 'check_circle';
                messageEl.textContent = result.message;
            } else {
                resultEl.className = 'setup-test-result error';
                iconEl.textContent = 'error';
                messageEl.textContent = result.message;
            }
        } catch (error) {
            resultEl.className = 'setup-test-result error';
            iconEl.textContent = 'error';
            messageEl.textContent = `Connection test failed: ${error.message}`;
            this.testResult = { success: false, message: error.message };
        }
    }

    async saveConfig() {
        const saveBtn = document.getElementById('setupSaveBtn');
        const originalContent = saveBtn.innerHTML;

        // Show saving state
        saveBtn.innerHTML = '<span class="material-icons" style="animation: spin 1s linear infinite;">sync</span><span>Saving...</span>';
        saveBtn.disabled = true;

        try {
            const data = this.getFormData();
            const response = await fetch('/api/v1/setup/mqtt/save', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(data)
            });

            const result = await response.json();

            if (result.status === 'success') {
                // Go to completion step with waiting state
                this.goToStep(4);

                // If service is restarting, wait for it to come back
                if (result.restarting) {
                    this.waitForRestart();
                }
            } else {
                Toast.error(result.message || 'Failed to save configuration');
                saveBtn.innerHTML = originalContent;
                saveBtn.disabled = false;
            }
        } catch (error) {
            Toast.error(`Failed to save: ${error.message}`);
            saveBtn.innerHTML = originalContent;
            saveBtn.disabled = false;
        }
    }

    async waitForRestart() {
        const contentEl = document.querySelector('#setupStep4 .setup-content');

        // Update UI to show waiting state
        contentEl.innerHTML = `
            <div class="setup-success-icon" style="background: linear-gradient(135deg, rgba(3, 169, 244, 0.1) 0%, rgba(3, 169, 244, 0.2) 100%);">
                <span class="material-icons" style="color: var(--primary-color); animation: spin 2s linear infinite;">sync</span>
            </div>
            <h2>Restarting Service...</h2>
            <p>Energy2MQTT is restarting with your new configuration. Please wait.</p>
            <div class="setup-restart-status" id="restartStatus">
                <span class="material-icons">hourglass_empty</span>
                <span>Waiting for service to restart...</span>
            </div>
        `;

        // Hide the finish button while waiting
        const finishBtn = document.querySelector('#setupStep4 .setup-actions .filled-button');
        if (finishBtn) {
            finishBtn.style.display = 'none';
        }

        // Poll for service to come back
        const maxAttempts = 30;
        const pollInterval = 2000; // 2 seconds
        let attempts = 0;

        const checkService = async () => {
            attempts++;
            const statusEl = document.getElementById('restartStatus');

            try {
                const response = await fetch('/health', {
                    method: 'GET',
                    cache: 'no-cache'
                });

                if (response.ok) {
                    // Service is back!
                    this.showRestartComplete();
                    return;
                }
            } catch (e) {
                // Service not ready yet, expected during restart
            }

            if (attempts < maxAttempts) {
                if (statusEl) {
                    statusEl.innerHTML = `
                        <span class="material-icons" style="animation: spin 1s linear infinite;">sync</span>
                        <span>Waiting for service... (${attempts}/${maxAttempts})</span>
                    `;
                }
                setTimeout(checkService, pollInterval);
            } else {
                // Timeout - show manual refresh option
                this.showRestartTimeout();
            }
        };

        // Start polling after a short delay to let the service shut down
        setTimeout(checkService, 3000);
    }

    showRestartComplete() {
        const contentEl = document.querySelector('#setupStep4 .setup-content');
        contentEl.innerHTML = `
            <div class="setup-success-icon">
                <span class="material-icons">check_circle</span>
            </div>
            <h2>Setup Complete!</h2>
            <p>Energy2MQTT has restarted and is now connected with your new configuration.</p>
            <div class="setup-info-box" style="background-color: rgba(76, 175, 80, 0.08); border-left-color: var(--success-color);">
                <span class="material-icons" style="color: var(--success-color);">check_circle</span>
                <div>
                    <strong>Ready to Go</strong>
                    <p>You can now start adding Modbus hubs, KNX adapters, and other devices.</p>
                </div>
            </div>
        `;

        // Show finish button
        const finishBtn = document.querySelector('#setupStep4 .setup-actions .filled-button');
        if (finishBtn) {
            finishBtn.style.display = '';
            finishBtn.innerHTML = '<span class="material-icons">check</span><span>Get Started</span>';
        }
    }

    showRestartTimeout() {
        const contentEl = document.querySelector('#setupStep4 .setup-content');
        contentEl.innerHTML = `
            <div class="setup-success-icon" style="background: linear-gradient(135deg, rgba(255, 152, 0, 0.1) 0%, rgba(255, 152, 0, 0.2) 100%);">
                <span class="material-icons" style="color: var(--warning-color);">schedule</span>
            </div>
            <h2>Taking Longer Than Expected</h2>
            <p>The service hasn't responded yet. This could be normal if it's still starting up.</p>
            <div class="setup-info-box warning">
                <span class="material-icons">info</span>
                <div>
                    <strong>What to do</strong>
                    <p>Wait a moment and click "Retry" below, or manually refresh the page. If problems persist, check the service logs.</p>
                </div>
            </div>
        `;

        // Update finish button to retry
        const actionsEl = document.querySelector('#setupStep4 .setup-actions');
        actionsEl.innerHTML = `
            <button class="outlined-button" onclick="window.location.reload()">
                <span class="material-icons">refresh</span>
                <span>Refresh Page</span>
            </button>
            <button class="filled-button" onclick="setupWizard.waitForRestart()">
                <span class="material-icons">sync</span>
                <span>Retry</span>
            </button>
        `;
    }

    finish() {
        this.hide();
        // Reload the page to reinitialize with new config
        window.location.reload();
    }
}

// Create global instance
window.setupWizard = new SetupWizard();
