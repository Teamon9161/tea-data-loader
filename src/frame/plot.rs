use std::path::Path;

use crate::prelude::*;

#[derive(Debug, Clone)]
pub struct PlotOpt<'a> {
    pub x: &'a str,
    pub y: &'a str,
    pub save_name: Option<&'a Path>,
    pub title: &'a str,
    pub show: bool,
    pub save: bool,
    pub slider: bool,
}

impl Default for PlotOpt<'_> {
    fn default() -> Self {
        Self {
            x: "time",
            y: "equity_curve",
            save_name: None,
            title: "Equity Curve",
            show: false,
            save: true,
            slider: false,
        }
    }
}

impl Frame {
    #[allow(unreachable_code)]
    pub fn plot<S: AsRef<str>>(&self, strategies: &[S], opt: &PlotOpt) -> Result<()> {
        #[cfg(feature = "plotly")]
        return self.plotly_plot_equity_curve(strategies, opt);
        #[cfg(feature = "poloto")]
        return self.poloto_plot_equity_curve(strategies, opt);
    }

    #[cfg(feature = "plotly")]
    pub fn plotly_plot_equity_curve<S: AsRef<str>>(
        &self,
        strategies: &[S],
        opt: &PlotOpt,
    ) -> Result<()> {
        use plotly::common::Mode;
        use plotly::layout::{Axis, RangeSlider};
        use plotly::Scatter;
        use polars::prelude::TimeUnit as PlTimeUnit;
        use tea_strategy::tevec::prelude::{unit, DateTime};
        let df = self
            .as_eager()
            .ok_or_else(|| anyhow::Error::msg("not a eager dataframe"))?;
        let x_data = df[opt.x].datetime()?;
        let time_unit = x_data.time_unit();
        // TODO: time zone is ignoredhere
        let mut plot = plotly::Plot::new();
        strategies.iter().for_each(|s| {
            let (x_vec, y_vec): (Vec<_>, Vec<_>) = x_data
                .iter()
                .zip(df[s.as_ref()].f64().unwrap())
                .filter_map(|(x, y)| {
                    if x.is_some() && y.is_some() {
                        let time = match time_unit {
                            PlTimeUnit::Nanoseconds => {
                                DateTime::<unit::Nanosecond>::from_opt_i64(x)
                                    .strftime(Some("%Y-%m-%d %H:%M:%S"))
                            },
                            PlTimeUnit::Milliseconds => {
                                DateTime::<unit::Millisecond>::from_opt_i64(x)
                                    .strftime(Some("%Y-%m-%d %H:%M:%S"))
                            },
                            _ => todo!(),
                        };
                        Some((time, y.unwrap()))
                    } else {
                        None
                    }
                })
                .unzip();
            let trace = Scatter::new(x_vec, y_vec)
                .mode(Mode::Lines)
                .name(s.as_ref());

            plot.add_trace(trace);
        });
        let mut x_axis = Axis::new();
        if opt.slider {
            x_axis = x_axis.range_slider(RangeSlider::new().visible(true));
        }
        let layout = plotly::Layout::new().title(opt.title).x_axis(x_axis);
        plot.set_layout(layout);
        if opt.show {
            plot.show();
        }
        if opt.save {
            let save_name = opt
                .save_name
                .unwrap_or_else(|| Path::new("equity_curve.html"));
            plot.write_html(save_name);
        }
        Ok(())
    }

    #[cfg(feature = "poloto")]
    fn poloto_plot_equity_curve<S: AsRef<str>>(
        &self,
        strategies: &[S],
        opt: &PlotOpt,
    ) -> Result<()> {
        use polars::prelude::DataType;
        use poloto::build;
        use poloto_chrono::UnixTime;
        let save_name = opt
            .save_name
            .unwrap_or_else(|| Path::new("equity_curve.svg"));
        let df = self
            .as_eager()
            .ok_or_else(|| anyhow::Error::msg("not a eager dataframe"))?;
        let x_data = df[opt.x].datetime()?;
        // TODO: ms, us timeunit are not covered
        let plots = strategies.iter().map(|s| {
            build::plot(s.as_ref()).line(
                x_data
                    .iter()
                    .zip(
                        df[s.as_ref()]
                            .cast(&DataType::Float64)
                            .unwrap()
                            .f64()
                            .unwrap(),
                    )
                    .filter_map(|(t, c)| {
                        if let Some(t) = t {
                            if let Some(c) = c {
                                return Some((UnixTime(t / 1_000_000_000), c));
                            }
                        }
                        None
                    }),
            )
        });
        let svg_file = std::fs::File::create(save_name)?;
        let svg = poloto::header();
        poloto::frame()
            .build()
            .data(plots)
            .build_and_label((opt.title, opt.x, opt.y))
            .append_to(svg.light_theme())
            .render_io_write(svg_file)?;
        Ok(())
    }
}
