use std::sync::Arc;

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
    type Item = (Arc<str>, Frame);
    type IntoIter = std::iter::Zip<std::vec::IntoIter<Arc<str>>, std::vec::IntoIter<Frame>>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        let len = self.len();
        let symbols = self.symbols.unwrap_or_else(|| vec!["".into(); len]);
        symbols.into_iter().zip(self.dfs)
    }
}

impl IntoParallelIterator for DataLoader {
    type Item = (Arc<str>, Frame);
    type Iter = rayon::iter::Zip<rayon::vec::IntoIter<Arc<str>>, rayon::vec::IntoIter<Frame>>;

    #[inline]
    fn into_par_iter(self) -> Self::Iter {
        self.symbols
            .unwrap()
            .into_par_iter()
            .zip(self.dfs.into_par_iter())
    }
}

impl DataLoader {
    /// Returns an iterator over the symbols and frames in the DataLoader.
    ///
    /// This method provides a way to iterate over the pairs of symbols and frames
    /// stored in the DataLoader. It uses a zip iterator to combine the symbols
    /// and frames into tuples.
    ///
    /// # Returns
    ///
    /// A `std::iter::Zip` iterator yielding pairs of `(&Arc<str>, &Frame)`.
    pub fn iter(&self) -> std::iter::Zip<std::slice::Iter<Arc<str>>, std::slice::Iter<Frame>> {
        self.symbols.as_ref().unwrap().iter().zip(self.dfs.iter())
    }

    /// Returns a parallel iterator over the symbols and frames in the DataLoader.
    ///
    /// This method provides a way to iterate over the pairs of symbols and frames
    /// stored in the DataLoader in parallel. It uses Rayon's parallel iterators
    /// to potentially improve performance on multi-core systems.
    ///
    /// # Returns
    ///
    /// A `rayon::iter::Zip` parallel iterator yielding pairs of `(&Arc<str>, &Frame)`.
    pub fn par_iter(
        &self,
    ) -> rayon::iter::Zip<rayon::slice::Iter<Arc<str>>, rayon::slice::Iter<Frame>> {
        self.symbols
            .as_ref()
            .unwrap()
            .par_iter()
            .zip(self.dfs.par_iter())
    }
}
