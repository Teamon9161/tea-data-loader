mod data_loader;
mod impls;
mod methods;

pub(super) mod utils;

pub use data_loader::DataLoader;
pub use methods::*;

#[derive(Default, Clone, Copy, Debug)]
pub enum Backend {
    #[default]
    Polars,
    Tevec,
}
