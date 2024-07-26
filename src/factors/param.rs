use std::fmt::Debug;

use derive_more::From;

#[derive(Default, From)]
pub enum Param {
    I32(i32),
    F64(f64),
    #[default]
    None,
}

impl Debug for Param {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Param::I32(v) => write!(f, "{}", v),
            Param::F64(v) => write!(f, "{}", v),
            Param::None => write!(f, ""),
        }
    }
}

unsafe impl Send for Param {}
unsafe impl Sync for Param {}
