use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Kubernetes API error: {0}")]
    Kube(#[from] kube::Error),

    #[error("Missing namespace on resource {0}")]
    MissingNamespace(String),

    #[error("Kopia secret {secret} invalid: {reason}")]
    InvalidKopiaSecret { secret: String, reason: String },

    #[error("Kopia error: {0}")]
    Kopia(String),

    #[error("Invalid duration string: {0}")]
    InvalidDuration(String),

    #[error("Invalid cron expression: {0}")]
    InvalidCron(String),

    #[error("Notification error: {0}")]
    Notification(String),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("Missing field: {0}")]
    MissingField(String),
}

pub type Result<T> = std::result::Result<T, Error>;
