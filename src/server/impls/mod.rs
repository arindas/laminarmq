//! Module providing implementations of various server abstractions.

#[cfg(target_os = "linux")]
pub mod glommio;
