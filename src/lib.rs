#![deny(unsafe_code)]

mod codec;
mod config;
mod key_mapper;
mod runtime;
mod storage;
mod volume;

use zenoh::{try_init_log_from_env, Result};
use zenoh_backend_traits::{config::VolumeConfig, VolumeInstance};
use zenoh_plugin_trait::{plugin_long_version, plugin_version, Plugin};

pub struct RedisBackend;

#[cfg(feature = "dynamic_plugin")]
zenoh_plugin_trait::declare_plugin!(RedisBackend);

impl Plugin for RedisBackend {
    type StartArgs = VolumeConfig;
    type Instance = VolumeInstance;

    const DEFAULT_NAME: &'static str = "redis";
    const PLUGIN_VERSION: &'static str = plugin_version!();
    const PLUGIN_LONG_VERSION: &'static str = plugin_long_version!();

    fn start(_name: &str, config: &VolumeConfig) -> Result<VolumeInstance> {
        try_init_log_from_env();
        Ok(Box::new(volume::RedisVolume::new(config)?))
    }
}
