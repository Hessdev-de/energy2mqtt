
use log::info;
use tokio::sync::mpsc::Sender;
use crate::mqtt::{PublishData, SubscribeData, Transmission};

pub struct CommandHandler {
   sender: Sender<Transmission>,
}

impl CommandHandler { 

  pub fn new(sender: Sender<Transmission>) -> Self {
    return CommandHandler { 
      sender: sender,
    }
  }

  pub async fn start_thread(&self) {
        info!("Starting CommandHandler thread");
        /* We need to subscribe to an MQTT topic and wait for data to fill our buffers */
        let (sender, mut receiver) = tokio::sync::mpsc::channel(10);

        let register = Transmission::Subscribe(SubscribeData{
            topic: "mgt/command".to_string(),
            sender
        });

        let _ = self.sender.send(register).await;

        /* We are not using the HADiscover and HAComponent stuff here because we know the json  */
        let json = r###"
        {
          "dev": {
            "ids":"e2m_management",
            "name":"energy2mqtt Bridge",
            "manufacturer":"energy2mqtt",
            "model":"Bridge"
          },
          "o": {
            "name":"energy2mqtt",
            "sw_version":"0.1.1",
            "support_url":"https://energy2mqtt.org"
          },
          "cmps":{
            "restart": {
              "p":"button",
              "device_class":"restart",
              "name":"restart",
              "object_id":"bridge_restart",
              "payload_press":"restart",
              "unique_id":"e2m_management_bridge_restart",
              "command_topic": "energy2mqtt/mgt/command"
            },
            "uptime": {
              "p":"sensor",
              "name":"uptime",
              "object_id":"uptime",
              "unique_id":"e2m_management_uptime",
              "state_topic": "energy2mqtt/mgt/uptime",
              "state_class": "measurement",
              "unit_of_measurement": "s"
            }
          }
        }"###;

        let p = Transmission::Publish(PublishData {
            topic: "homeassistant/device/e2m_bridge/config".to_string(),
            payload: json.to_string(),
            qos: 0,
            retain: true,
        });

        /* Send our data */
        let _ = self.sender.send(p).await;

        info!("Start waiting for command messages");
        while let Some(c) = receiver.recv().await {
            info!("Received command {c}");
            
            if c == "restart" {
                /* if we exit that thread the rest will exit, too */
                info!("Request to shutdown received");
                return;
            }
        }
  }
}


