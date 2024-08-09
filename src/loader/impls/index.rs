use std::ops::{Index, IndexMut};

use crate::prelude::*;

impl Index<&str> for DataLoader {
    type Output = Frame;

    #[inline]
    fn index(&self, index: &str) -> &Self::Output {
        let symbols = self.symbols.as_ref().unwrap();
        let idx = symbols.iter().position(|s| &**s == index);
        if let Some(idx) = idx {
            &self.dfs[idx]
        } else {
            panic!("Symbol not found: {}", index);
        }
    }
}

impl Index<usize> for DataLoader {
    type Output = Frame;

    #[inline]
    fn index(&self, index: usize) -> &Self::Output {
        &self.dfs[index]
    }
}

impl IndexMut<&str> for DataLoader {
    #[inline]
    fn index_mut(&mut self, index: &str) -> &mut Self::Output {
        let symbols = self.symbols.as_ref().unwrap();
        let idx = symbols.iter().position(|s| &**s == index);
        if let Some(idx) = idx {
            &mut self.dfs[idx]
        } else {
            panic!("Symbol not found: {}", index);
        }
    }
}

impl IndexMut<usize> for DataLoader {
    #[inline]
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.dfs[index]
    }
}
