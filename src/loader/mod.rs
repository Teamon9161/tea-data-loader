mod data_loader;
mod impls;
#[cfg(feature = "io")]
pub(crate) mod io;
mod methods;

pub mod utils;

pub use data_loader::DataLoader;
pub use methods::*;

#[derive(Default, Clone, Copy, Debug)]
pub enum Backend {
    #[default]
    Polars,
    Tevec,
}
