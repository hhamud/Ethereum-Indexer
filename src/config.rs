use config::{Config, File, FileFormat};
use eyre::Result;
use serde::Deserialize;

/// A struct representing configuration settings for the application.
#[derive(Debug, Deserialize)]
pub struct Settings {
    /// The username used for authentication.
    pub username: String,

    /// The password used for authentication.
    pub password: String,

    /// The host address where the application connects.
    pub host: String,

    /// The port number to establish the connection.
    pub port: u16,

    /// The name of the Database.
    pub name: String,
}

impl Settings {
    /// Creates a new `Settings` instance by loading configuration from a YAML file.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing the parsed `Settings` if successful, or an error if
    /// configuration parsing fails.
    pub fn new() -> Result<Self> {
        // Create a new configuration builder.
        let settings = Config::builder()
            // Add a source for configuration from a YAML file named "config.yaml".
            .add_source(File::new("config.yaml", FileFormat::Yaml))
            .build()?;

        // Try to deserialize the configuration into a `Settings` instance.
        Ok(settings.try_deserialize::<Settings>()?)
    }
}
