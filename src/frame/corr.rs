use tea_strategy::tevec::prelude::*;

use crate::prelude::*;

#[derive(Default, Clone, Copy)]
pub struct FrameCorrOpt<'a> {
    pub method: CorrMethod,
    pub min_periods: Option<usize>,
    pub plot: bool,
    pub save_path: Option<&'a str>,
    pub title: Option<&'a str>,
}

impl Frame {
    pub fn corr(self, opt: FrameCorrOpt) -> Result<Vec<Vec<f64>>> {
        let data = self.collect()?.into_frame().inner_corr(opt)?;
        Ok(data)
    }

    fn inner_corr(&self, opt: FrameCorrOpt) -> Result<Vec<Vec<f64>>> {
        let df = self.as_eager().unwrap();
        df.iter()
            .map(|s1| {
                let s1 = s1.cast_f64()?;
                let corr_series = df
                    .iter()
                    .map(|s2| {
                        let s2 = s2.cast_f64()?;
                        Ok(s1
                            .f64()
                            .unwrap()
                            .vcorr(&s2.f64().unwrap(), opt.min_periods, opt.method)
                            .cast())
                    })
                    .collect::<Result<Vec<f64>>>()?;
                Ok(corr_series)
            })
            .collect::<Result<Vec<_>>>()
    }

    #[cfg(feature = "plotly-plot")]
    fn plot_corr(self, opt: FrameCorrOpt) -> Result<()> {
        let square = true;
        use plotly::layout::{Axis, AxisConstrain, AxisType};
        use plotly::HeatMap;
        let df = self.collect()?;
        let factors = df.get_column_names_owned();
        let data = df.into_frame().inner_corr(opt)?;
        let trace = HeatMap::new(factors.clone(), factors, data).zauto(true);
        let (x_axis, y_axis) = if square {
            (
                Axis::new()
                    .type_(AxisType::Category)
                    .scale_anchor("y")
                    .constrain(AxisConstrain::Domain),
                Axis::new()
                    .type_(AxisType::Category)
                    .constrain(AxisConstrain::Domain),
            )
        } else {
            (
                Axis::new().type_(AxisType::Category),
                Axis::new().type_(AxisType::Category),
            )
        };
        let layout = plotly::Layout::new()
            .title(opt.title.unwrap_or("Correlation"))
            .x_axis(x_axis)
            .y_axis(y_axis);
        let mut plot = plotly::Plot::new();
        plot.add_trace(trace);
        plot.set_layout(layout);
        if let Some(save_path) = opt.save_path {
            let save_path = std::path::Path::new(save_path);
            if !save_path.parent().map(|p| p.exists()).unwrap_or(true) {
                std::fs::create_dir(save_path.parent().unwrap())?;
            }
            plot.write_html(save_path);
        }
        Ok(())
    }
}
