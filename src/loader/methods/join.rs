use std::path::Path;

use polars::prelude::*;

use crate::prelude::*;

pub struct JoinOpt<P: AsRef<Path>, E: AsRef<[Expr]>> {
    path: P,
    left_on: Option<E>,
    right_on: Option<E>,
    how: JoinType,
    coalesce: Option<bool>,
    flag: bool,
    suffix: Option<&'static str>,
}

impl<P: AsRef<Path>, E: AsRef<[Expr]>> JoinOpt<P, E> {
    #[inline]
    pub fn new_on(path: P, on: &E, how: JoinType, flag: bool) -> JoinOpt<P, &[Expr]>
    where
        E: Clone,
    {
        JoinOpt {
            path,
            left_on: Some(on.as_ref()),
            right_on: Some(on.as_ref()),
            how,
            coalesce: None,
            flag,
            suffix: None,
        }
    }

    #[inline]
    pub fn new(path: P, left_on: E, right_on: E, how: JoinType, flag: bool) -> Self {
        JoinOpt {
            path,
            left_on: Some(left_on),
            right_on: Some(right_on),
            how,
            coalesce: None,
            flag,
            suffix: None,
        }
    }
}

impl DataLoader {
    pub fn join<P: AsRef<Path>, E: AsRef<[Expr]>>(self, option: JoinOpt<P, E>) -> Result<Self> {
        if !option.flag {
            return Ok(self);
        }
        let suffix = option.suffix.unwrap_or(".feather");
        let mut out = self.empty_copy();
        let default_on = [col("time")];
        let coalesce = if let Some(coalesce) = option.coalesce {
            if coalesce {
                JoinCoalesce::CoalesceColumns
            } else {
                JoinCoalesce::KeepColumns
            }
        } else {
            JoinCoalesce::JoinSpecific
        };
        for (symbol, df) in self.into_iter() {
            let other_path = option.path.as_ref().join(symbol + suffix);
            let other = LazyFrame::scan_ipc(&other_path, Default::default())?;
            let df = df.join(
                other.into(),
                option
                    .left_on
                    .as_ref()
                    .map(|e| e.as_ref())
                    .unwrap_or_else(|| default_on.as_ref()),
                option
                    .right_on
                    .as_ref()
                    .map(|e| e.as_ref())
                    .unwrap_or_else(|| default_on.as_ref()),
                JoinArgs::new(option.how.clone()).with_coalesce(coalesce),
            )?;
            out.dfs.push(df);
        }
        Ok(out)
    }

    #[inline]
    pub fn left_join<P: AsRef<Path>, E: AsRef<[Expr]>>(
        self,
        path: P,
        left_on: E,
        right_on: E,
        flag: bool,
    ) -> Result<Self> {
        self.join(JoinOpt::new(path, left_on, right_on, JoinType::Left, flag))
    }
}
