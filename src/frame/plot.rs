use std::path::Path;

use anyhow::Result;

use super::Frame;

/// Plotting options for Frame visualization.
///
/// This struct contains various options to customize the plot output.
#[derive(Debug, Clone)]
pub struct PlotOpt<'a> {
    /// The column name to use for the x-axis.
    pub x: &'a str,
    /// The column name to use for the y-axis.
    pub y: &'a str,
    /// The file path to save the plot. If None, a default path will be used.
    pub save_name: Option<&'a Path>,
    /// The title of the plot.
    pub title: &'a str,
    /// Whether to display the plot interactively.
    pub show: bool,
    /// Whether to save the plot to a file.
    pub save: bool,
    /// Whether to include a range slider in the plot (for interactive plots).
    pub slider: bool,
}

/// Default implementation for PlotOpt.
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

impl<'a> PlotOpt<'a> {
    #[inline]
    pub fn with_x(mut self, x: &'a str) -> Self {
        self.x = x;
        self
    }
}

impl Frame {
    /// Plot the Frame data using the specified plotting library.
    ///
    /// This method will use either Plotly or Poloto based on the feature flags.
    ///
    /// # Arguments
    ///
    /// * `strategies` - A slice of strategy names to plot.
    /// * `opt` - The plotting options.
    ///
    /// # Returns
    ///
    /// A Result indicating success or failure of the plotting operation.
    #[allow(unreachable_code)]
    pub fn plot<S: AsRef<str>>(&self, strategies: &[S], opt: &PlotOpt) -> Result<()> {
        #[cfg(feature = "plotly")]
        return self.plotly_plot_equity_curve(strategies, opt);
        #[cfg(feature = "poloto")]
        return self.poloto_plot_equity_curve(strategies, opt);
    }

    /// Plot the Frame data using Plotly.
    ///
    /// This method is only available when the "plotly" feature is enabled.
    ///
    /// # Arguments
    ///
    /// * `strategies` - A slice of strategy names to plot.
    /// * `opt` - The plotting options.
    ///
    /// # Returns
    ///
    /// A Result indicating success or failure of the plotting operation.
    #[cfg(feature = "plotly")]
    fn plotly_plot_equity_curve<S: AsRef<str>>(
        &self,
        strategies: &[S],
        opt: &PlotOpt,
    ) -> Result<()> {
        use plotly::common::Mode;
        use plotly::layout::{Axis, RangeSlider};
        use plotly::Scatter;
        use polars::prelude::{DataType, TimeUnit as PlTimeUnit};
        use tea_strategy::tevec::prelude::{unit, DateTime};
        let df = self
            .as_eager()
            .ok_or_else(|| anyhow::Error::msg("not a eager dataframe"))?;
        let time_series = df[opt.x].cast(&DataType::Datetime(PlTimeUnit::Milliseconds, None))?;
        let x_data = time_series.datetime()?;
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

    /// Plot the Frame data using Poloto.
    ///
    /// This method is only available when the "poloto" feature is enabled.
    ///
    /// # Arguments
    ///
    /// * `strategies` - A slice of strategy names to plot.
    /// * `opt` - The plotting options.
    ///
    /// # Returns
    ///
    /// A Result indicating success or failure of the plotting operation.
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
