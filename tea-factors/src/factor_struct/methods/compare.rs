use polars::prelude::*;

use crate::prelude::*;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    Eq,
    EqMissing,
    Neq,
    NeqMissing,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Clone, Copy)]
pub struct FactorCompare<F, G> {
    left: F,
    right: G,
    op: CompareOp,
}

impl<F, G> std::fmt::Debug for FactorCompare<F, G>
where
    F: FactorBase,
    G: FactorBase,
{
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.op {
            CompareOp::Eq => write!(f, "{} == {}", self.left.name(), self.right.name()),
            CompareOp::EqMissing => write!(f, "{} == {}", self.left.name(), self.right.name()),
            CompareOp::Neq => write!(f, "{} != {}", self.left.name(), self.right.name()),
            CompareOp::NeqMissing => write!(f, "{} != {}", self.left.name(), self.right.name()),
            CompareOp::Lt => write!(f, "{} < {}", self.left.name(), self.right.name()),
            CompareOp::Le => write!(f, "{} <= {}", self.left.name(), self.right.name()),
            CompareOp::Gt => write!(f, "{} > {}", self.left.name(), self.right.name()),
            CompareOp::Ge => write!(f, "{} >= {}", self.left.name(), self.right.name()),
        }
    }
}

impl<F, G> FactorBase for FactorCompare<F, G>
where
    F: FactorBase,
    G: FactorBase,
{
    #[inline]
    fn fac_name() -> Arc<str> {
        format!("{}.cmp({})", F::fac_name(), G::fac_name()).into()
    }

    fn new(_param: impl Into<Param>) -> Self {
        panic!("FactorCompare::new should not be called directly")
    }
}

impl<F, G> PlFactor for FactorCompare<F, G>
where
    F: FactorBase + PlFactor,
    G: FactorBase + PlFactor,
{
    #[inline]
    fn try_expr(&self) -> Result<Expr> {
        let left = self.left.try_expr()?;
        let right = self.right.try_expr()?;
        let res = match self.op {
            CompareOp::Eq => left.eq(right),
            CompareOp::EqMissing => left.eq_missing(right),
            CompareOp::Neq => left.neq(right),
            CompareOp::NeqMissing => left.neq_missing(right),
            CompareOp::Lt => left.lt(right),
            CompareOp::Le => left.lt_eq(right),
            CompareOp::Gt => left.gt(right),
            CompareOp::Ge => left.gt_eq(right),
        };
        Ok(res)
    }
}

pub type CompareFactor<F, G> = Factor<FactorCompare<F, G>>;

pub trait FactorCmpExt: FactorBase {
    #[inline]
    /// Compare `Factor` with other `Factor` on equality.
    fn eq<G: FactorBase>(self, other: G) -> CompareFactor<Self, G> {
        FactorCompare {
            left: self,
            right: other,
            op: CompareOp::Eq,
        }
        .into()
    }

    #[inline]
    /// Compare `Factor` with other `Factor` on equality where `None == None`.
    fn eq_missing<G: FactorBase>(self, other: G) -> CompareFactor<Self, G> {
        FactorCompare {
            left: self,
            right: other,
            op: CompareOp::EqMissing,
        }
        .into()
    }

    #[inline]
    /// Compare `Factor` with other `Factor` on non-equality.
    fn neq<G: FactorBase>(self, other: G) -> CompareFactor<Self, G> {
        FactorCompare {
            left: self,
            right: other,
            op: CompareOp::Neq,
        }
        .into()
    }

    #[inline]
    /// Compare `Factor` with other `Factor` on non-equality where `None == None`.
    fn neq_missing<G: FactorBase>(self, other: G) -> CompareFactor<Self, G> {
        FactorCompare {
            left: self,
            right: other,
            op: CompareOp::NeqMissing,
        }
        .into()
    }

    #[inline]
    /// Compare `Factor` with other `Factor` for "less than" relationship.
    fn lt<G: FactorBase>(self, other: G) -> CompareFactor<Self, G> {
        FactorCompare {
            left: self,
            right: other,
            op: CompareOp::Lt,
        }
        .into()
    }

    #[inline]
    /// Compare `Factor` with other `Factor` for "less than or equal to" relationship.
    fn lt_eq<G: FactorBase>(self, other: G) -> CompareFactor<Self, G> {
        FactorCompare {
            left: self,
            right: other,
            op: CompareOp::Le,
        }
        .into()
    }

    #[inline]
    /// Compare `Factor` with other `Factor` for "greater than" relationship.
    fn gt<G: FactorBase>(self, other: G) -> CompareFactor<Self, G> {
        FactorCompare {
            left: self,
            right: other,
            op: CompareOp::Gt,
        }
        .into()
    }

    #[inline]
    /// Compare `Factor` with other `Factor` for "greater than or equal to" relationship.
    fn gt_eq<G: FactorBase>(self, other: G) -> CompareFactor<Self, G> {
        FactorCompare {
            left: self,
            right: other,
            op: CompareOp::Ge,
        }
        .into()
    }
}

impl<F: FactorBase> FactorCmpExt for F {}
