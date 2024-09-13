use itertools::Itertools;
use polars::prelude::*;

use crate::prelude::*;
const POST_ALIGN_COLLECT_NUM: usize = 200;

impl Frames {
    /// Aligns multiple frames based on specified columns and join type.
    ///
    /// This method aligns the frames in the `Frames` collection by performing a series of joins
    /// on the specified columns. It creates a master alignment frame and then extracts
    /// individual aligned frames from it.
    ///
    /// # Arguments
    ///
    /// * `on` - An expression or slice of expressions specifying the columns to align on.
    /// * `how` - An optional `JoinType` specifying the type of join to perform. Defaults to `JoinType::Full` if not provided.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing a new `Frames` instance with aligned frames, or an error if the alignment process fails.
    ///
    /// # Notes
    ///
    /// - If the `Frames` collection is empty, it returns the original `Frames` instance.
    /// - For large numbers of frames (more than `POST_ALIGN_COLLECT_NUM`), it may need to collect eagerly to avoid stack overflow.
    /// - The method sorts the resulting frames based on the alignment columns.
    pub fn align<E: AsRef<[Expr]>>(self, on: E, how: Option<JoinType>) -> Result<Self> {
        if self.is_empty() {
            return Ok(self);
        }
        let len = self.len();
        // use the same method as python `polars.align_frames`
        let on = on.as_ref();
        let how = how.unwrap_or(JoinType::Full);
        let align_on: Vec<_> = on
            .iter()
            .map(|o| o.clone().meta().output_name())
            .try_collect()?;
        // note: can stackoverflow if the join becomes too large, so we
        // collect eagerly when hitting a large enough number of frames
        let post_align_collect = len > POST_ALIGN_COLLECT_NUM;
        // create aligned master frame (this is the most expensive part; afterwards
        // we just subselect out the columns representing the component frames)
        let idx_frames = self.into_iter().enumerate();
        let mut alignment_frame = idx_frames
            .clone()
            .reduce(|(_l_idx, ldf), (r_idx, rdf)| {
                (
                    r_idx,
                    ldf.join(
                        rdf,
                        &on,
                        &on,
                        JoinArgs {
                            how: how.clone(),
                            suffix: Some(format!(":{}", r_idx)),
                            coalesce: JoinCoalesce::CoalesceColumns,
                            ..Default::default()
                        },
                    )
                    .unwrap(),
                )
            })
            .unwrap()
            .1
            .sort(align_on, SortMultipleOptions::default())?;
        if post_align_collect {
            eprintln!("too much frames, shold collect eagerly, but not implemented yet");
        }
        // select-out aligned components from the master frame
        let schema = alignment_frame.schema()?;
        let aligned_cols = schema.get_names().into_iter().unique().collect_vec();
        let aligned_frames: Vec<_> = idx_frames
            .map(|(idx, mut df)| -> Result<_> {
                let sfx = format!(":{}", idx);
                let df_cols = df
                    .schema()?
                    .iter_names()
                    .map(|c| {
                        let name_with_sfx = format!("{}{}", c, sfx);
                        if aligned_cols.contains(&name_with_sfx.as_str()) {
                            col(&name_with_sfx).alias(c)
                        } else {
                            col(c)
                        }
                    })
                    .collect_vec();
                let f = alignment_frame.clone().select(df_cols)?;
                Ok(f)
            })
            .try_collect()?;

        Ok(aligned_frames.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_align_frames() -> Result<()> {
        let df1 = df! [
            "time" => [1, 2, 3, 4],
            "a" => [1, 2, 3, 4],
            "b" => [10, 20, 30, 40]
        ]?;

        let df2 = df! [
            "time" => [2, 3, 4, 5],
            "c" => [100, 200, 300, 400],
            "d" => [1000, 2000, 3000, 4000]
        ]?;

        let df3 = df! [
            "time" => [3, 4, 5, 6],
            "e" => ["a", "b", "c", "d"],
            "f" => [true, false, true, false]
        ]?;

        let frames = Frames(vec![Frame::from(df1), Frame::from(df2), Frame::from(df3)])
            .align([col("time")], None)?
            .collect(true)?;
        assert_eq!(frames.len(), 3);

        // Verify the content of the aligned frames
        let expected1 = df! [
            "time" => [Some(1), Some(2), Some(3), Some(4), Some(5), Some(6)],
            "a" => [Some(1), Some(2), Some(3), Some(4), None, None],
            "b" => [Some(10), Some(20), Some(30), Some(40), None, None]
        ]?;

        let expected2 = df! [
            "time" => [Some(1), Some(2), Some(3), Some(4), Some(5), Some(6)],
            "c" => [None, Some(100), Some(200), Some(300), Some(400), None],
            "d" => [None, Some(1000), Some(2000), Some(3000), Some(4000), None]
        ]?;

        let expected3 = df! [
            "time" => [Some(1), Some(2), Some(3), Some(4), Some(5), Some(6)],
            "e" => [None, None, Some("a"), Some("b"), Some("c"), Some("d")],
            "f" => [None, None, Some(true), Some(false), Some(true), Some(false)]
        ]?;

        assert!(frames[0].as_eager().unwrap().equals_missing(&expected1));
        assert!(frames[1].as_eager().unwrap().equals_missing(&expected2));
        assert!(frames[2].as_eager().unwrap().equals_missing(&expected3));

        Ok(())
    }
}
