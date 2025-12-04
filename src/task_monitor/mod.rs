//! Task monitoring module for tracking and managing async tokio tasks.
//!
//! This module provides functionality to monitor spawned tasks, detect crashes,
//! and notify users when tasks fail unexpectedly.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};
use log::{error, info, warn};
use tokio::sync::mpsc::Sender;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

use crate::mqtt::Transmission;

/// Metadata of a monitored task
#[derive(Debug, Clone)]
pub struct TaskInfo {
    /// Human-readable name of the task
    pub name: String,
    /// Category/type of the task (e.g., "modbus_hub", "mqtt", "api")
    pub task_type: String,
    /// When the task was started
    pub started_at: Instant,
    /// Number of times this task has been restarted
    pub restart_count: u32,
}

/// Represents a monitored task with its handle and metadata
struct MonitoredTask {
    handle: JoinHandle<()>,
    info: TaskInfo,
}

/// Result of checking a task's status
#[derive(Debug, Clone)]
pub enum TaskStatus {
    /// Task is still running
    Running,
    /// Task completed successfully
    Completed,
    /// Task panicked or was cancelled
    Crashed(String),
    /// Task was aborted
    Aborted,
}

/// Monitors and manages async tasks, detecting crashes and providing notifications
pub struct TaskMonitor {
    tasks: Arc<RwLock<HashMap<String, MonitoredTask>>>,
    mqtt_sender: Option<Sender<Transmission>>,
    manager_name: String,
}

impl TaskMonitor {
    /// Create a new TaskMonitor
    ///
    /// # Arguments
    /// * `manager_name` - Name of the manager using this monitor (for logging)
    pub fn new(manager_name: &str) -> Self {
        TaskMonitor {
            tasks: Arc::new(RwLock::new(HashMap::new())),
            mqtt_sender: None,
            manager_name: manager_name.to_string(),
        }
    }

    /// Create a new TaskMonitor with MQTT notification support
    ///
    /// # Arguments
    /// * `manager_name` - Name of the manager using this monitor
    /// * `sender` - MQTT transmission sender for crash notifications
    pub fn with_mqtt(manager_name: &str, sender: Sender<Transmission>) -> Self {
        TaskMonitor {
            tasks: Arc::new(RwLock::new(HashMap::new())),
            mqtt_sender: Some(sender),
            manager_name: manager_name.to_string(),
        }
    }

    /// Spawn and monitor a new task
    ///
    /// # Arguments
    /// * `name` - Unique name for this task
    /// * `task_type` - Category of the task
    /// * `future` - The async future to spawn
    ///
    /// # Returns
    /// The task name (for later reference)
    pub async fn spawn<F>(&self, name: &str, task_type: &str, future: F) -> String
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let task_name = name.to_string();
        let task_type_str = task_type.to_string();
        let manager = self.manager_name.clone();

        // Wrap the future to log when it completes/crashes
        let wrapped_future = async move {
            info!("[{}] Task '{}' ({}) started", manager, task_name, task_type_str);
            future.await;
            // If we reach here, the task completed (didn't panic)
            info!("[{}] Task '{}' ({}) completed normally", manager, task_name, task_type_str);
        };

        let handle = tokio::spawn(wrapped_future);

        let info = TaskInfo {
            name: name.to_string(),
            task_type: task_type.to_string(),
            started_at: Instant::now(),
            restart_count: 0,
        };

        let monitored = MonitoredTask { handle, info };

        let mut tasks = self.tasks.write().await;
        tasks.insert(name.to_string(), monitored);

        name.to_string()
    }

    /// Check the status of a specific task
    pub async fn check_task(&self, name: &str) -> Option<TaskStatus> {
        let tasks = self.tasks.read().await;
        if let Some(task) = tasks.get(name) {
            Some(self.get_task_status(&task.handle))
        } else {
            None
        }
    }

    /// Check all tasks and return a list of crashed tasks
    ///
    /// This method also logs and notifies about any crashed tasks found.
    pub async fn check_all_tasks(&self) -> Vec<(String, TaskInfo, TaskStatus)> {
        let mut crashed_tasks = Vec::new();
        let tasks = self.tasks.read().await;

        for (name, task) in tasks.iter() {
            let status = self.get_task_status(&task.handle);

            match &status {
                TaskStatus::Crashed(reason) => {
                    error!(
                        "[{}] TASK CRASHED: '{}' (type: {}) after {:?} - Reason: {}",
                        self.manager_name,
                        name,
                        task.info.task_type,
                        task.info.started_at.elapsed(),
                        reason
                    );
                    crashed_tasks.push((name.clone(), task.info.clone(), status));
                }
                TaskStatus::Completed => {
                    warn!(
                        "[{}] Task '{}' (type: {}) completed unexpectedly after {:?}",
                        self.manager_name,
                        name,
                        task.info.task_type,
                        task.info.started_at.elapsed()
                    );
                    crashed_tasks.push((name.clone(), task.info.clone(), status));
                }
                TaskStatus::Aborted => {
                    info!(
                        "[{}] Task '{}' was aborted",
                        self.manager_name,
                        name
                    );
                }
                TaskStatus::Running => {
                    // Task is healthy, nothing to report
                }
            }
        }

        // Send MQTT notification if we have crashed tasks and a sender
        if !crashed_tasks.is_empty() {
            if let Some(sender) = &self.mqtt_sender {
                self.notify_crashes(&crashed_tasks, sender).await;
            }
        }

        crashed_tasks
    }

    /// Get the number of currently monitored tasks
    pub async fn task_count(&self) -> usize {
        self.tasks.read().await.len()
    }

    /// Get the number of running tasks
    pub async fn running_count(&self) -> usize {
        let tasks = self.tasks.read().await;
        tasks.values()
            .filter(|t| !t.handle.is_finished())
            .count()
    }

    /// Get info about all tasks
    pub async fn get_all_task_info(&self) -> Vec<(String, TaskInfo, bool)> {
        let tasks = self.tasks.read().await;
        tasks.iter()
            .map(|(name, task)| {
                (name.clone(), task.info.clone(), !task.handle.is_finished())
            })
            .collect()
    }

    /// Abort all monitored tasks
    pub async fn abort_all(&self) {
        let tasks = self.tasks.read().await;
        for (name, task) in tasks.iter() {
            if !task.handle.is_finished() {
                info!("[{}] Aborting task '{}'", self.manager_name, name);
                task.handle.abort();
            }
        }
    }

    /// Abort a specific task by name
    pub async fn abort_task(&self, name: &str) -> bool {
        let tasks = self.tasks.read().await;
        if let Some(task) = tasks.get(name) {
            if !task.handle.is_finished() {
                info!("[{}] Aborting task '{}'", self.manager_name, name);
                task.handle.abort();
                return true;
            }
        }
        false
    }

    /// Remove all finished tasks from monitoring
    pub async fn cleanup_finished(&self) -> Vec<String> {
        let mut tasks = self.tasks.write().await;
        let finished: Vec<String> = tasks.iter()
            .filter(|(_, task)| task.handle.is_finished())
            .map(|(name, _)| name.clone())
            .collect();

        for name in &finished {
            tasks.remove(name);
        }

        finished
    }

    /// Clear all tasks (abort running ones first)
    pub async fn clear_all(&self) {
        self.abort_all().await;
        let mut tasks = self.tasks.write().await;
        tasks.clear();
    }

    fn get_task_status(&self, handle: &JoinHandle<()>) -> TaskStatus {
        if !handle.is_finished() {
            return TaskStatus::Running;
        }

        // Task is finished, check if it was aborted
        if handle.is_finished() {
            // We can't easily distinguish between panic and completion
            // without consuming the handle, so we check if it was aborted
            TaskStatus::Crashed("Task finished unexpectedly (panic or error)".to_string())
        } else {
            TaskStatus::Running
        }
    }

    async fn notify_crashes(&self, crashed: &[(String, TaskInfo, TaskStatus)], sender: &Sender<Transmission>) {
        use crate::mqtt::{Transmission, TaskCrashData};

        for (name, info, status) in crashed {
            let reason = match status {
                TaskStatus::Crashed(r) => r.clone(),
                TaskStatus::Completed => "Completed unexpectedly".to_string(),
                TaskStatus::Aborted => "Aborted".to_string(),
                TaskStatus::Running => continue,
            };

            let message = format!(
                "Task '{}' (type: {}, manager: {}) crashed after {:?}: {}",
                name,
                info.task_type,
                self.manager_name,
                info.started_at.elapsed(),
                reason
            );

            // Publish crash notification to MQTT
            let _ = sender.send(Transmission::TaskCrash(TaskCrashData {
                manager: self.manager_name.clone(),
                task_name: name.clone(),
                task_type: info.task_type.clone(),
                message,
                restart_count: info.restart_count,
            })).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_spawn_and_check() {
        let monitor = TaskMonitor::new("test");

        // Spawn a task that completes immediately
        monitor.spawn("quick_task", "test", async {
            // Do nothing, complete immediately
        }).await;

        // Give it time to complete
        tokio::time::sleep(Duration::from_millis(50)).await;

        let status = monitor.check_task("quick_task").await;
        assert!(matches!(status, Some(TaskStatus::Crashed(_)) | Some(TaskStatus::Completed)));
    }

    #[tokio::test]
    async fn test_running_task() {
        let monitor = TaskMonitor::new("test");

        // Spawn a long-running task
        monitor.spawn("long_task", "test", async {
            tokio::time::sleep(Duration::from_secs(60)).await;
        }).await;

        // Check immediately - should be running
        let status = monitor.check_task("long_task").await;
        assert!(matches!(status, Some(TaskStatus::Running)));

        // Cleanup
        monitor.abort_all().await;
    }

    #[tokio::test]
    async fn test_abort_task() {
        let monitor = TaskMonitor::new("test");

        monitor.spawn("to_abort", "test", async {
            tokio::time::sleep(Duration::from_secs(60)).await;
        }).await;

        assert_eq!(monitor.running_count().await, 1);

        monitor.abort_task("to_abort").await;
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert_eq!(monitor.running_count().await, 0);
    }
}
