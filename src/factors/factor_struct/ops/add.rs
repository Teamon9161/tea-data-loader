use std::marker::PhantomData;
use std::ops::Add;

use crate::prelude::{Factor, FactorBase, Param, PlFactor};

#[derive(Clone)]
pub struct FactorAdd<F: FactorBase, G: FactorBase> {
    left_param: Param,
    right_param: Param,
    fac: PhantomData<(F, G)>,
}

impl<F: FactorBase, G: FactorBase> Add<Factor<G>> for Factor<F> {
    type Output = FactorAdd<F, G>;
    #[inline]
    fn add(self, rhs: Factor<G>) -> Self::Output {
        FactorAdd {
            fac: PhantomData,
            left_param: self.param,
            right_param: rhs.param,
        }
    }
}

// impl<F, G> FactorBase for FactorAdd<F, G>
// where
//     F: FactorBase,
//     G: FactorBase,
// {
//     fn fac_name() -> Arc<str> {
//         format!("{}+{}", F::fac_name(), G::fac_name()).into()
//     }

//     fn new(param: impl Into<Param>) -> Self {
//         FactorAdd(PhantomData)
//     }
// }

// impl<F, G> PlFactor for FactorAdd<F, G>
// where
//     F: FactorBase + PlFactor + Send + Sync + 'static,
//     G: FactorBase + PlFactor + Send + Sync + 'static,
// {
// }
