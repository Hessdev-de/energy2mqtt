use std::time::{Duration, SystemTime};
use log::{debug, error, info};
use sqlite::Connection;

use crate::configuration::CONFIG;

/// Get a list of all tables in a specific database
/// 
/// # Arguments
/// 
/// * `connection` - the already open SQLite Database connection
/// 
/// # Returns
/// 
/// A vector of Strings holding the names of the tables. If no table was found the vector
/// is empty.
/// 
pub fn get_tables(connection: &Connection) -> Vec<String> {

    // Get a list of all tables which should be handled.
    match connection.prepare("SELECT name FROM sqlite_master;") {
        Ok(mut statement) => {
            return statement.iter()
                            .filter(|r| r.is_ok()) /* The data is valid */
                            .map(|r| {
                                    let row = r.unwrap();
                                    let s = row.read::<&str, _>(0);
                                    s.to_string()
                            }) /* Get the first column */
                            .collect() /* Build a vector of the result */
        },
        Err(e) => {
            error!("Statement could not be prepared: {:?}", x.err().unwrap());
            return Vec::new();
        }
    }
}

/// Run the delete process on a specific table of the connection
/// 
/// # Arguments
/// 
/// * `connection` - the already open SQLite Database connection
/// * `table` - the table to delete data from
/// * `max_old` - the maximum seconds of the oldest entry, everything older will be deleted
/// 
pub fn delete_older_than(connection: &Connection, table: &String, max_old: u32) {
    /* 
     * The first column of the table is timestamp which is a unix timestamp.
     * 
     * We delete everything with timestamp lower then now - max_old
     * 
     * If our time is 1233 and the max_old is 33 we will delete everything
     * with timestamp lower than 1200
     */
    let max_timestamp = SystemTime::UNIX_EPOCH.elapsed().unwrap_or(Duration::from_secs(0)).as_secs() - max_old as u64;
    debug!("We will delete from {table} everything older than {max_timestamp}");

    let exec = connection.execute(format!("DELETE FROM {table} WHERE timestamp < {max_timestamp};"));
    if let Err(e) = exec {
        error!("Failed to clean up table {table}: {e:?}");
    } else {
        info!("Cleaned up {table}");
    }
}

/// Main function / trhead to clean all old data from a database
/// 
/// This function will not hold the database open, we will close the database after handling all tables
/// 
pub async fn retainer() {
    let mut retaintime = CONFIG.read().await.get_retain_time();

    if retaintime == u32::MAX as u64 {
        error!("Configuration error, retaintime is wrong, setting to 24 hours");
        retaintime = 24*60*60;
    }

    info!("Will delete all data every {retaintime}s");
    let sleeptime = Duration::from_secs(retaintime);
    
    let topics = CONFIG.read().await.get_subscribed_topics();
    
    loop {
        tokio::time::sleep(sleeptime).await;
        for topic in &topics {
            if let Some(topic_config) = CONFIG.read().await.get_topic_config(topic) {
                let db = format!("config/store/{}.sqlite", topic_config.database);

                let connection = sqlite::open(&db);
                if connection.is_err() {
                    error!("Could not open sqlite db {db}");
                    continue;
                }

                // Get the connection and check which tables need to be handled
                let connection = connection.unwrap();
                let tables = get_tables(&connection);
                // if no retention is set in the configuration of this topic we use the default of 90 days
                let max_duration =  topic_config.retention.unwrap_or(crate::configuration::ConfigIntervals::Days(90)).to_secs();

                // delete everything older than the max_duration
                for table in &tables {
                    delete_older_than(&connection, table, max_duration);
                }
            }
        }
        
    }
}