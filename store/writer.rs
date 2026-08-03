use std::collections::{HashMap, HashSet};
use evalexpr::{ContextWithMutableVariables, DefaultNumericTypes, HashMapContext};
use log::{debug, error};
use rumqttc::{AsyncClient, QoS};
use serde::Serialize;
use crate::configuration::CONFIG;

/// This struct holds the internal process communitication
///
/// All transfers to the database and later MQTT need to be transmitted using this
/// struct.
#[derive(Serialize)]
pub struct WriterData {
    /// The topic the data was received for
    pub topic: String,
    /// The original data fetched from MQTT
    pub data: Vec<u8>,
    /// The time the data was received
    pub timeing: i64,
    /// The reason to store the data (event, timing or trigger)
    pub reason: String,
}

/// This is an internal struct used to republish data to the MQTT
#[derive(Serialize)]
struct MqttData {
    timestamp: i64,
    data: serde_json::Value,
    reason: serde_json::Value,
}

/// Main part of the writer
///
/// We are storing our database connections and the information which tables
/// have already been created.
pub struct Writer {
    databases: HashMap<String, sqlite::Connection>,
    tables_created: HashSet<String>
}

impl Writer {

    /// Create a new instance of Writer
    ///
    /// # Warning
    ///
    /// You should only build one instance and use that in the main loop, because
    /// you may hold multiple SQLite conections to a database in different threads.
    pub fn new() -> Self {
        Self {
            databases: HashMap::new(),
            tables_created: HashSet::new()
        }
    }

    pub async fn store(&mut self, data: WriterData, client: &AsyncClient) {
        /*
         * The first prototype for the code is trival:
         * - Find the right topic for the information about this data
         * - build the correct name for the value based on the name_template
         * - open the correct database
         * - CREATE the Table if it does not exist
         * - INSERT the value into the database
         * - CLOSE the database
         */

        let topic_config = CONFIG.read().await.get_topic_config(&data.topic);
        if topic_config.is_none() {
            error!("Received data for non configured topic: {}", data.topic);
            return;
        }

        let topic_config = topic_config.unwrap();

        // We use evalexpr to buid a nice looking table name which is configurable.
        let mut context = HashMapContext::<DefaultNumericTypes>::new();
        let elements: Vec<&str> = data.topic.split("/").collect();
        for i in 0..elements.len() {
            if let Err(e) = context.set_value(format!("path_{i}"), 
                                                                evalexpr::Value::String(String::from(elements[i]))) {
                error!("Context could not be updated: {e:?}")
            }
        }

        // Build the name or use a default which will allow us to store the data anyway
        let table_name = match evalexpr::eval_string_with_context(&topic_config.name_template, &context) {
            Ok(r) => r,
            Err(e) => {
                error!("Failed to evaluate: {e:?}");
                "faulty".to_string()
            },
        };


        // Create the dir if needed
        let _ = std::fs::create_dir_all("config/store");


        // Build database path
        let db = format!("config/store/{}.sqlite", topic_config.database);

        // Open the database once and fail if that is not possible
        if !self.databases.contains_key(&db) {
            if let Ok(conn) = sqlite::open(&db) {
                self.databases.insert(db.clone(), conn);
            } else {
                error!("Could not open SQLite database {db}");
                return;
            }
        }

        // Check if our connection is up and running
        if let Some(connection) =  self.databases.get(&db){

            // Create the table if needed
            if !self.tables_created.contains(&table_name) {
                let query = format!("CREATE TABLE IF NOT EXISTS {table_name} (timestamp INTEGER PRIMARY KEY UNIQUE, data TEXT, reason TEXT) WITHOUT ROWID;");

                if let Err(e) = connection.execute(query) {
                    error!("Failed to create table: {e:?}");
                    self.databases.remove(&db);
                    return;
                } else {
                    debug!("Table created");
                    self.tables_created.insert(table_name.clone());
                }
            }

            // Build our statement and store the data
            let data_string = String::from_utf8(data.data.clone()).unwrap_or_default();
            let query = format!("INSERT INTO {table_name} VALUES ({}, '{}', '{}');",
                                            data.timeing,
                                            data_string,
                                            data.reason
                                        );

            if let Err(e) = connection.execute(query) {
                // If we fail then close the database and remove the connection
                error!("Failed to run the query to save the data: {e:?}");
                self.databases.remove(&db);
            } else {

                debug!("Data successfully written to {table_name}");

                // Not all data should be published, check for that
                if CONFIG.read().await.get_publish(&data.topic) {

                    // Our publishing topic is fixed based on the table name
                    let topic = format!("energy2mqtt/store/publish/{table_name}");

                    // Build our struct we will publish
                    let mq = MqttData {
                        timestamp: data.timeing,
                        data: serde_json::from_slice(&data.data).unwrap_or(serde_json::Value::String(data_string)),
                        reason: serde_json::from_str(&data.reason).unwrap_or_default(),
                    };

                    // Send the data to MQTT and log errors
                    if let Err(e) = client.publish(topic.clone(), QoS::AtLeastOnce, false,
                                        serde_json::to_string(&mq).unwrap_or_default()).await {
                        error!("Could not publish to {topic}: {e:?}");
                    }
                }
            }
        } else {
            error!("Failed to get SQLITE Database from MAP: {db}");
            self.databases.remove(&db);
        }
    }
}


