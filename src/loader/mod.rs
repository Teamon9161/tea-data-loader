mod data_loader;
mod impls;
#[cfg(feature = "io")]
pub(crate) mod io;
mod methods;

pub mod utils;

pub use data_loader::DataLoader;
pub use methods::*;

/// Represents the backend used for data processing.
///
/// This enum defines the available backend options for data loading and manipulation.
#[derive(Default, Clone, Copy, Debug)]
pub enum Backend {
    /// The default backend, using Polars for data processing.
    #[default]
    Polars,
    /// An alternative backend using Tevec for data processing.
    Tevec,
}
