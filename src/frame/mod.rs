mod calc_fac;
mod evaluate;
mod frame_core;
mod frames;
mod frames_align;
mod join;
#[cfg(feature = "plot")]
mod plot;

pub use corr::FrameCorrOpt;
pub use evaluate::EvaluateOpt;
pub use frame_core::{Frame, IntoFrame};
pub use frames::Frames;
mod corr;
#[cfg(feature = "plot")]
pub use plot::PlotOpt;
