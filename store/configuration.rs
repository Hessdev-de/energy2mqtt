use std::collections::HashSet;
use log::error;
use serde::Deserialize;
use tokio::sync::RwLock;

#[derive(Debug, PartialEq, Deserialize, Clone)]
pub enum ConfigIntervals {
    Instant, /* As soon as we receive the value */
    Trigger(String), /* We may watch other subjects and store the value afterwards */
    Seconds(u32),
    Minutes(u32),
    Hours(u32),
    Days(u32),
    Exact(u32 /* hour */, u32 /* minute */, u32 /* seconds */),
}

impl ConfigIntervals {
    pub fn to_secs(&self) -> u32 {
        match self {
            ConfigIntervals::Seconds(s) => { *s },
            ConfigIntervals::Minutes(m) => { m * 60 },
            ConfigIntervals::Hours(h) => { h * 60 * 60 },
            ConfigIntervals::Days(d) => { d * 24 * 60 * 60},
            _ => {
                /* Instant, Trigger and Exact returns max of u32 because it can't be converted */
                u32::MAX
            },
        }
    }
}

fn defaultconfig_retention() -> ConfigIntervals { ConfigIntervals::Days(356) }
fn defaultconfig_tz() -> String { "Europe/Berlin".to_string() }

#[derive(Deserialize)]
pub struct DefaultConfig {
    #[serde(default="defaultconfig_retention")]
    pub retention: ConfigIntervals,
    #[serde(default="defaultconfig_tz")]
    pub timezone: String,
}

impl Default for DefaultConfig {
    fn default() -> Self {
        Self { 
            retention: defaultconfig_retention(),
            timezone: defaultconfig_tz(),
        }
    }
}

fn default_database() -> String {
    "store.sqlite3".to_string()
}

#[derive(Deserialize, Clone)]
pub struct Topics {
    pub topic: String,
    pub name_template: String,
    pub store: Vec<ConfigIntervals>,
    pub publish: Option<bool>,
    pub max_variant: Option<ConfigIntervals>, /* only positive values allowed */
    pub retention: Option<ConfigIntervals>,
    #[serde(default="default_database")]
    pub database: String
}

static NAME: &str = "store4mqtt";
fn mqtt_client_name_default() -> String { return NAME.to_string() }
fn mqtt_client_user_default() -> String { return NAME.to_string() }
fn mqtt_client_pass_default() -> String { return NAME.to_string() }

#[derive(Deserialize)]
pub struct MqttConfig {
    pub host: String,
    pub port: u16,
    #[serde(default="mqtt_client_user_default")]
    pub user: String,
    #[serde(default="mqtt_client_pass_default")]
    pub pass: String,
    #[serde(default="mqtt_client_name_default")]
    pub client_name: String,
}

impl Default for MqttConfig {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 1883,
            user: mqtt_client_user_default(),
            pass: mqtt_client_pass_default(),
            client_name: mqtt_client_name_default()
        }
    }
}

#[derive(Deserialize)]
pub struct Configuration {
    pub mqtt: MqttConfig,
    pub default: DefaultConfig,
    pub topics: Vec<Topics>,
    #[serde(skip_deserializing)]
    pub is_valid: bool
}

impl Default for Configuration {
    fn default() -> Self {
        Self {
            mqtt: Default::default(),
            default: Default::default(),
            topics: Default::default(),
            is_valid: false
        }
    }
}

impl Configuration {
    
    pub fn get_mqtt_config(&self) -> (String, u16, String, String, String) {
        (
            self.mqtt.host.clone(),
            self.mqtt.port,
            self.mqtt.user.clone(),
            self.mqtt.pass.clone(),
            self.mqtt.client_name.clone()
        )
    }

    pub fn get_subscribed_topics(&self) -> HashSet<String> {
        let mut direct_topics= HashSet::new() ; //self.topics.iter().map(|t| t.topic.clone()).collect();

        /* Loop through the triggers */
        for tc in &self.topics {
            direct_topics.insert(tc.topic.clone());
            for s in &tc.store {
                if let ConfigIntervals::Trigger(trigger) = s {
                    direct_topics.insert(trigger.clone());
                }
            }
        }

        direct_topics
    }


    pub fn match_with_wildcard(topic: &String, needle: &String) -> bool {
        if topic == needle {
            return true;
        }

        if topic.contains('#') {
            let split = topic.split('#').next().unwrap();
            if needle.starts_with(split) {
                return true;
            }
        }

        false
    }

    pub fn is_instant(&self, needle: &String) -> bool {
        let mut ret = false;
        for topic in &self.topics {
            if !Configuration::match_with_wildcard(&topic.topic, needle) {
                continue;
            }
            
            ret = topic.store.contains(&ConfigIntervals::Instant);
        }

        ret
    }

    pub fn get_topic_config(&self, needle: &String) -> Option<Topics> {
        for topic in &self.topics {
            if Configuration::match_with_wildcard(&topic.topic, needle) {
                return Some(topic.clone());
            }
        }

        None
    }

    pub fn get_publish(&self, needle: &String) -> bool {
        if let Some(tc) = self.get_topic_config(needle) {
            tc.publish.unwrap_or(false)
        } else {
            false
        }
    }

    pub fn get_timezone(&self) -> String {
        self.default.timezone.clone()
    }

    pub fn get_retain_time(&self) -> u64 {
        self.default.retention.to_secs() as u64
    }

    pub fn trigger_for_topic(&self, topic: &String) -> HashSet<String> {

        let mut ret = HashSet::new();

        for conf in &self.topics {
            for s in &conf.store {
                if let ConfigIntervals::Trigger(trigger) = s {
                    if trigger == topic {
                        /* Store the topic for which this trigger is used */
                        ret.insert(conf.topic.clone());
                    }
                }
            }
        }

        ret
    }

}

pub fn parse_config(file: &str) -> Configuration {
    match std::fs::read_to_string(file) {
        Ok(yaml) => {
            match serde_yml::from_str::<Configuration>(yaml.as_str()) {
                Ok(mut conf) => { 
                    conf.is_valid = true;
                    return conf;
                },
                Err(e) => {
                    error!("Could not parse {file}, not valid YAML: {e:?}");
                },
            }
        },
        Err(_) => {
            error!("Could not open configuration {file} using default configuration");
        },
    }

    Configuration::default()
}

lazy_static::lazy_static! {
    pub static ref CONFIG: RwLock<Configuration> = RwLock::new(parse_config("config/store.yaml"));
}

pub fn verify_timing(max_variant: &Option<ConfigIntervals>, storage_time: &std::time::SystemTime) -> bool {
    let invalid_duration = std::time::Duration::from_secs(10_000_000*365*24*60*60);

    if let Some(variant) = max_variant {
        let time_since_receivment = storage_time.elapsed().unwrap_or(invalid_duration).as_secs();
        let time_max_delta = variant.to_secs() as u64;
        if time_since_receivment > time_max_delta {
            return false;
        }
    }

    true
}