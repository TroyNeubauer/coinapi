//! Bindings to the [coinapi](https://www.coinapi.io/jq) cryptocurrency api
//! Currently only the Market Data REST API is supported
use std::time::{Duration, SystemTime};

use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("url parse {0}")]
    UrlParse(#[from] url::ParseError),

    #[error("reqwest {0}")]
    Reqwest(#[from] reqwest::Error),
}

pub struct Coinapi {
    key: String,
}

/// The name of an asset such as `BTC`, `UTD`, `ETH`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AssetName(String);

/// Represents the quantity of a period.
/// For the 2SEC period, this value would be 2
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum PeriodInner {
    Second(u8),
    Minute(u8),
    Hour(u8),
    Day(u8),
}

/// A supported period for which historical data can be obtained
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Period(PeriodInner);

/// Returned when the requested period is not supported by coinapi
pub struct ExactError {
    /// The period that was requested by the user
    pub requested: Duration,

    /// The closest period what would have been used if [`Period::new`] was used
    pub closest: Period,
}

const fn p(inner: PeriodInner) -> Period {
    Period(inner)
}

const SUPPORTED_PERIODS: [Period; 33] = [
    // seconds
    p(PeriodInner::Second(1)),
    p(PeriodInner::Second(2)),
    p(PeriodInner::Second(3)),
    p(PeriodInner::Second(4)),
    p(PeriodInner::Second(5)),
    p(PeriodInner::Second(6)),
    p(PeriodInner::Second(10)),
    p(PeriodInner::Second(15)),
    p(PeriodInner::Second(20)),
    p(PeriodInner::Second(30)),
    // minutes
    p(PeriodInner::Minute(1)),
    p(PeriodInner::Minute(2)),
    p(PeriodInner::Minute(3)),
    p(PeriodInner::Minute(4)),
    p(PeriodInner::Minute(5)),
    p(PeriodInner::Minute(6)),
    p(PeriodInner::Minute(10)),
    p(PeriodInner::Minute(15)),
    p(PeriodInner::Minute(20)),
    p(PeriodInner::Minute(30)),
    // hours
    p(PeriodInner::Hour(1)),
    p(PeriodInner::Hour(2)),
    p(PeriodInner::Hour(3)),
    p(PeriodInner::Hour(4)),
    p(PeriodInner::Hour(6)),
    p(PeriodInner::Hour(8)),
    p(PeriodInner::Hour(12)),
    // days
    p(PeriodInner::Day(1)),
    p(PeriodInner::Day(2)),
    p(PeriodInner::Day(3)),
    p(PeriodInner::Day(5)),
    p(PeriodInner::Day(7)),
    p(PeriodInner::Day(10)),
];

lazy_static::lazy_static! {
    static ref SUPPORTED_PERIOD_DURATIONS: [Duration; 33] = SUPPORTED_PERIODS.map(|p| match p.0 {
        PeriodInner::Second(s) => Duration::from_secs(s as u64),
        PeriodInner::Minute(s) => Duration::from_secs(s as u64) * 60,
        PeriodInner::Hour(s) => Duration::from_secs(s as u64) * 60 * 60,
        PeriodInner::Day(s) => Duration::from_secs(s as u64) * 60 * 60 * 24,
    });
}

impl Period {
    /// Creates a new period which represents the supported period that is closest to `duration`.
    ///
    /// Don't use this method
    pub fn new(duration: Duration) -> Self {
        match Self::new_exact(duration) {
            Ok(p) => p,
            Err(err) => err.closest,
        }
    }

    pub fn new_exact(duration: Duration) -> Result<Self, ExactError> {
        Self::get_nearest(duration).map_err(|closest| ExactError {
            requested: duration,
            closest,
        })
    }

    /// Returns the span of this period as a duration
    pub fn duration(&self) -> Duration {
        todo!()
    }

    /// Returns the period that is nearest to `duration`.
    /// If a period is natively supported that is the same as `duration`, Ok(..) is returned
    /// Otherwise Err(..) is returned containing the peroid nearest to the requested duration
    pub fn get_nearest(duration: Duration) -> Result<Period, Period> {
        let durations = &SUPPORTED_PERIOD_DURATIONS;
        let periods = &SUPPORTED_PERIODS;
        match durations.binary_search(&duration) {
            Ok(i) => Ok(periods[i]),
            Err(i) => {
                dbg!(i);
                // i is the position where it would be inserted to keep ascending order
                // Because we arent adding anything this means that:
                // `periods[i - 1] < duration < periods[i]`
                if i == 0 {
                    Err(periods[0])
                } else if i == periods.len() {
                    Err(periods[periods.len() - 1])
                } else {
                    // Find the nearest one
                    dbg!(duration, durations[i - 1], durations[i]);
                    let lower_dist = duration - durations[i - 1];
                    let higher_dist = durations[i] - duration;
                    if lower_dist < higher_dist {
                        Err(periods[i - 1])
                    } else {
                        Err(periods[i])
                    }
                }
            }
        }
    }
}

pub struct TimeseriesData {}

impl Coinapi {
    /// Sends a get request to the server with api v1 at `route` with params as URL parameters
    async fn get<'k, 'v>(
        &self,
        route: impl AsRef<str>,
        params: impl Iterator<Item = (&'k str, &'v str)>,
    ) -> Result<String, Error> {
        let url = reqwest::Url::parse_with_params(
            &format!("https://rest.coinapi.io/v1/{}", route.as_ref()),
            params.into_iter(),
        )?;
        let resp = reqwest::get(url).await?;
        let json = resp.text().await?;

        Ok(json)
    }

    pub async fn timeseries_data(
        &self,
        base: AssetName,
        quote: AssetName,
        period: Period,
        start: SystemTime,
        end: SystemTime,
        limit: usize,
    ) -> Result<TimeseriesData, Error> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn supported_periods_sorted() {
        // SUPPORTED_PERIOD_DURATIONS must be sorted because we binary search it
        for (i, current) in SUPPORTED_PERIOD_DURATIONS.iter().enumerate().skip(1) {
            let last = &SUPPORTED_PERIOD_DURATIONS[i - 1];
            assert!(current > last);
        }
    }

    #[test]
    fn periods() {
        for (i, period) in SUPPORTED_PERIOD_DURATIONS.iter().enumerate() {
            assert_eq!(Period::get_nearest(*period).unwrap(), SUPPORTED_PERIODS[i]);
        }

        fn test_near(query: Duration, expected: Period) {
            assert_eq!(Period::get_nearest(query).unwrap_err(), expected);
        }

        test_near(Duration::from_secs_f32(0.0), p(PeriodInner::Second(1)));
        test_near(Duration::from_secs_f32(0.5), p(PeriodInner::Second(1)));
        test_near(Duration::from_secs_f32(0.9), p(PeriodInner::Second(1)));
        test_near(Duration::from_secs_f32(1.49), p(PeriodInner::Second(1)));

        test_near(Duration::from_secs_f32(1.51), p(PeriodInner::Second(2)));
        test_near(Duration::from_secs_f32(40.0), p(PeriodInner::Second(30)));

        test_near(
            Duration::from_secs_f32(12.0 * 60.0),
            p(PeriodInner::Minute(10)),
        );
        test_near(
            Duration::from_secs_f32(40.0 * 60.0),
            p(PeriodInner::Minute(30)),
        );

        test_near(
            Duration::from_secs_f32(7.1 * 60.0 * 60.0),
            p(PeriodInner::Hour(8)),
        );

        test_near(
            Duration::from_secs_f32(14.0 * 60.0 * 60.0 * 24.0),
            p(PeriodInner::Day(10)),
        );
    }
}
