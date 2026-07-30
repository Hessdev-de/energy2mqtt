use std::collections::{HashMap, HashSet};
use evalexpr::{ContextWithMutableVariables, DefaultNumericTypes, HashMapContext};
use log::{debug, error};
use crate::configuration::CONFIG;

pub struct WriterData {
    pub topic: String,
    pub data: Vec<u8>,
    pub timeing: i64,
    pub reason: String,
}

pub struct Writer {
    databases: HashMap<String, sqlite::Connection>,
    tables_created: HashSet<String>
}

impl Writer {

    pub fn new() -> Self {
        Self {
            databases: HashMap::new(),
            tables_created: HashSet::new()
        }
    }

    pub async fn store(&mut self, data: WriterData) {
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

        /* No build the name_template part */
        let mut context = HashMapContext::<DefaultNumericTypes>::new();
        let elements: Vec<&str> = data.topic.split("/").collect();
        for i in 0..elements.len() {
            if let Err(e) = context.set_value(format!("path_{i}"), 
                                                                evalexpr::Value::String(String::from(elements[i]))) {
                error!("Context could not be updated: {e:?}")
            }
        }

        let table_name = match evalexpr::eval_string_with_context(&topic_config.name_template, &context) {
            Ok(r) => r,
            Err(e) => {
                error!("Failed to evaluate: {e:?}");
                "faulty".to_string()
            },
        };

        //debug!("Table name build from template: {table_name}");

        /* Create the dir if needed */
        let _ = std::fs::create_dir_all("config/store");


        let db = format!("config/store/{}.sqlite", topic_config.database);

        if !self.databases.contains_key(&db) {
            if let Ok(conn) = sqlite::open(&db) {
                self.databases.insert(db.clone(), conn);
            } else {
                error!("Could not open SQLite database {db}");
                return;
            }
        }

        if let Some(connection) =  self.databases.get(&db){

            /* Create the table if needed */
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

            let query = format!("INSERT INTO {table_name} VALUES ({}, '{}', '{}');",
                                            data.timeing,
                                            String::from_utf8(data.data).unwrap_or_default(),
                                            data.reason
                                        );

            if let Err(e) = connection.execute(query) {
                error!("Failed to run the query to save the data: {e:?}");
                self.databases.remove(&db);
            } else {
                debug!("Data successfully written");
            }
        } else {
            error!("Failed to get SQLITE Database from MAP: {db}");
            self.databases.remove(&db);
        }

    }
}


