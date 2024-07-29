use rayon::prelude::*;

use crate::prelude::*;

impl IntoIterator for Frames {
    type Item = Frame;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl IntoParallelIterator for Frames {
    type Item = Frame;
    type Iter = rayon::vec::IntoIter<Self::Item>;

    #[inline]
    fn into_par_iter(self) -> Self::Iter {
        self.0.into_par_iter()
    }
}

impl IntoIterator for DataLoader {
    type Item = (String, Frame);
    type IntoIter = std::iter::Zip<std::vec::IntoIter<String>, std::vec::IntoIter<Frame>>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.symbols.unwrap().into_iter().zip(self.dfs)
    }
}

impl IntoParallelIterator for DataLoader {
    type Item = (String, Frame);
    type Iter = rayon::iter::Zip<rayon::vec::IntoIter<String>, rayon::vec::IntoIter<Frame>>;

    #[inline]
    fn into_par_iter(self) -> Self::Iter {
        self.symbols
            .unwrap()
            .into_par_iter()
            .zip(self.dfs.into_par_iter())
    }
}
