use std::{collections::HashMap, path::Path};
use log::{debug, error, info};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct StoredData {
    old_metered_data: HashMap<String, Value>,
    proto: String,
    id: String,
}

fn generate_path( proto: &String) -> String {
    format!("config/storage/{proto}")
}


fn generate_filename( proto: &String, id: &String) -> String {
    format!("config/storage/{proto}/{id}.json")
}

impl StoredData {
    /* This is the main constructor for our storage we will use JSON for now to store data because MQTT uses it */
    pub async fn load(proto: String, id: &String) -> Self {
        let filename = generate_filename(&proto, id);

        let map = match std::fs::read_to_string(&filename) {
            Ok(d) => {
                serde_json::from_str(&d).unwrap_or(HashMap::new())
            }
            Err(e) => {
                error!("Error reading file {filename}: {e:?}");
                HashMap::new()
            },
        };
        
        StoredData {
            old_metered_data: map,
            proto,
            id: id.clone()
        }
    }

    pub fn get_data(&self, key: &String) -> Value {
        match self.old_metered_data.get(key) {
            Some(v) => v.clone(),
            None => Value::Null,
        }
    }

    pub fn set_data(&mut self, key: &String, value: &Value) {
        debug!("Updating Persistance for {} {key} -> {value}", self.id);
        self.old_metered_data.insert(key.clone(), value.clone()); 
    }

    pub fn get_map(&self) -> HashMap<String, Value> {
        self.old_metered_data.clone()
    }
    
}

/* We want our data to be saved on Dropping the object holding the data */
impl Drop for StoredData {
    fn drop(&mut self) {
        /* Always try to create the path */
        let dir = generate_path(&self.proto);
        let _ = std::fs::create_dir_all(&Path::new(&dir));

        match serde_json::to_string(&self.old_metered_data) {
            Ok(d) => {
                let filename = generate_filename(&self.proto, &self.id);
                match std::fs::write(filename, d) {
                    Ok(_) => {
                        info!("Stored persistance of {}", self.id);
                    },
                    Err(e) => {
                        error!("Failed to store persistance of {} -> {e:?}", self.id);
                    },
                }
            },
            Err(e) => {
                error!("Serialization for {} failed: {e:?}", self.id);
            },
        }
    }
}