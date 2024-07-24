use crate::prelude::*;

impl IntoIterator for Frames {
    type Item = Frame;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
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
