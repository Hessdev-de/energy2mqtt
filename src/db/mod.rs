use crate::mqtt::Transmission;
use tokio::sync::mpsc::Sender;
use tokio::sync::broadcast;



/// DeviceManager handles the storage and configuration
pub struct DeviceManager {
    sender: Sender<Transmission>,
    broadcast: tokio::sync::broadcast::Sender<String>,
    #[allow(dead_code)]
    rx_first: tokio::sync::broadcast::Receiver<String>
}

impl DeviceManager {
    /// Create a new DeviceManager with the specified SQLite database path
    pub fn new(meter_data_sender: Sender<Transmission>) -> Self {
        let (broadcast_tx, rx) = broadcast::channel(16);
        return DeviceManager {
            sender: meter_data_sender.clone(),
            broadcast: broadcast_tx,
            rx_first: rx,
        };
    }

    pub fn get_sender_instance(&self) -> Sender<Transmission> {
        return self.sender.clone();
    }

    pub fn get_broadcast_receiver(&self) -> tokio::sync::broadcast::Receiver<String> {
        return self.broadcast.subscribe();
    }

    pub fn send_broadcast_text(&self, text: String) {
        self.broadcast.send(text).unwrap();
    }

    pub fn get_broadcast_sender(&self) -> tokio::sync::broadcast::Sender<String> {
        return self.broadcast.clone();
    }
}