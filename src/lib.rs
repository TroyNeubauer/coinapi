//! Bindings to the [coinapi](https://www.coinapi.io/jq) cryptocurrency api
//! Currently only the Market Data REST API is supported
use chrono::{Date, DateTime, NaiveDate, Utc};
use std::time::Duration;

use serde::{Deserialize, Deserializer};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("url parse {0}")]
    UrlParse(#[from] url::ParseError),

    #[error("reqwest {0}")]
    Reqwest(#[from] reqwest::Error),

    #[error("json decode {0}")]
    Json(#[from] serde_json::Error),
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

impl std::fmt::Display for Period {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            PeriodInner::Second(s) => f.write_fmt(format_args!("{s}SEC")),
            PeriodInner::Minute(m) => f.write_fmt(format_args!("{m}MIN")),
            PeriodInner::Hour(h) => f.write_fmt(format_args!("{h}HRS")),
            PeriodInner::Day(d) => f.write_fmt(format_args!("{d}DAY")),
        }
    }
}

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

#[derive(Deserialize)]
pub struct TimeseriesData(pub Vec<TimeseriesDatum>);

#[derive(Deserialize)]
pub struct TimeseriesDatum {
    #[serde(deserialize_with = "de_date_time")]
    pub time_period_start: DateTime<Utc>,
    #[serde(deserialize_with = "de_date_time")]
    pub time_period_end: DateTime<Utc>,
    #[serde(deserialize_with = "de_date_time")]
    pub time_open: DateTime<Utc>,
    #[serde(deserialize_with = "de_date_time")]
    pub time_close: DateTime<Utc>,

    pub rate_open: f64,
    pub rate_high: f64,
    pub rate_low: f64,
    pub rate_close: f64,
}

fn de_date_time<'de, D>(d: D) -> Result<DateTime<Utc>, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(d)?;
    let fixed = DateTime::parse_from_rfc3339(&s).map_err(serde::de::Error::custom)?;
    Ok(fixed.with_timezone(&Utc))
}

fn de_date_time_option<'de, D>(d: D) -> Result<Option<DateTime<Utc>>, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(match Option::<String>::deserialize(d)? {
        Some(s) => {
            let fixed = DateTime::parse_from_rfc3339(&s).map_err(serde::de::Error::custom)?;
            Some(fixed.with_timezone(&Utc))
        }
        None => None,
    })
}

#[derive(Deserialize)]
pub struct Exchanges(pub Vec<Exchange>);

#[derive(Deserialize)]
pub struct Exchange {
    pub exchange_id: String,
    pub website: String,
    pub name: String,

    #[serde(default)]
    #[serde(deserialize_with = "de_date_option")]
    pub data_start: Option<NaiveDate>,
    #[serde(default)]
    #[serde(deserialize_with = "de_date_option")]
    pub data_end: Option<NaiveDate>,

    #[serde(default)]
    #[serde(deserialize_with = "de_date_time_option")]
    pub data_quote_start: Option<DateTime<Utc>>,
    #[serde(default)]
    #[serde(deserialize_with = "de_date_time_option")]
    pub data_quote_end: Option<DateTime<Utc>>,

    #[serde(default)]
    #[serde(deserialize_with = "de_date_time_option")]
    pub data_orderbook_start: Option<DateTime<Utc>>,
    #[serde(default)]
    #[serde(deserialize_with = "de_date_time_option")]
    pub data_orderbook_end: Option<DateTime<Utc>>,
    #[serde(default)]
    #[serde(deserialize_with = "de_date_time_option")]
    pub data_trade_start: Option<DateTime<Utc>>,
    #[serde(default)]
    #[serde(deserialize_with = "de_date_time_option")]
    pub data_trade_end: Option<DateTime<Utc>>,

    pub data_symbols_count: usize,
    pub volume_1hrs_usd: f64,
    pub volume_1day_usd: f64,
    pub volume_1mth_usd: f64,
}

fn de_date_option<'de, D>(d: D) -> Result<Option<NaiveDate>, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(match Option::<String>::deserialize(d)? {
        Some(s) => {
            dbg!(&s);
            Some(NaiveDate::parse_from_str(&s, "%Y-%m-%d").map_err(serde::de::Error::custom)?)
        }
        None => {
            println!("at none");
            None
        }
    })
}

fn de_date<'de, D>(d: D) -> Result<NaiveDate, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(d)?;
    Ok(NaiveDate::parse_from_str(&s, "%Y-%m-%d").map_err(serde::de::Error::custom)?)
}

fn de_int_bool<'de, D>(d: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    match u8::deserialize(d)? {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(serde::de::Error::custom("")),
    }
}

#[derive(Deserialize)]
pub struct Assets(pub Vec<Asset>);

#[derive(Deserialize)]
pub struct Asset {
    asset_id: String,
    name: String,
    #[serde(deserialize_with = "de_int_bool")]
    type_is_crypto: bool,

    #[serde(default)]
    #[serde(deserialize_with = "de_date_option")]
    data_start: Option<NaiveDate>,
    #[serde(default)]
    #[serde(deserialize_with = "de_date_option")]
    data_end: Option<NaiveDate>,

    #[serde(default)]
    #[serde(deserialize_with = "de_date_time_option")]
    data_quote_start: Option<DateTime<Utc>>,
    #[serde(default)]
    #[serde(deserialize_with = "de_date_time_option")]
    data_quote_end: Option<DateTime<Utc>>,

    #[serde(default)]
    #[serde(deserialize_with = "de_date_time_option")]
    data_orderbook_start: Option<DateTime<Utc>>,
    #[serde(deserialize_with = "de_date_time_option")]
    #[serde(default)]
    data_orderbook_end: Option<DateTime<Utc>>,

    #[serde(default)]
    #[serde(deserialize_with = "de_date_time_option")]
    data_trade_start: Option<DateTime<Utc>>,
    #[serde(default)]
    #[serde(deserialize_with = "de_date_time_option")]
    data_trade_end: Option<DateTime<Utc>>,

    data_symbols_count: usize,
    volume_1hrs_usd: f64,
    volume_1day_usd: f64,
    volume_1mth_usd: f64,
    #[serde(default)]
    price_usd: Option<f64>,
}

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

    /// Queries the `exchangerate/{asset_id_base}/{asset_id_quote}/history` endpoint for historical
    /// data for a security pair during a time interval
    pub async fn timeseries_data(
        &self,
        base: AssetName,
        quote: AssetName,
        period: Period,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        limit: usize,
    ) -> Result<TimeseriesData, Error> {
        let base = &base.0;
        let quote = &quote.0;
        let period = period.to_string();
        let start = start.to_rfc3339();
        let end = end.to_rfc3339();
        let limit = limit.to_string();
        let json = self
            .get(
                format!("exchangerate/{base}/{quote}/history"),
                [
                    ("period_id", period.as_str()),
                    ("time_start", &start),
                    ("time_end", &end),
                    ("limit", &limit),
                ]
                .into_iter(),
            )
            .await?;

        Ok(serde_json::from_str(&json)?)
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

    #[test]
    fn period_formattintg() {
        let s = [
            "1SEC", "2SEC", "3SEC", "4SEC", "5SEC", "6SEC", "10SEC", "15SEC", "20SEC", "30SEC",
            "1MIN", "2MIN", "3MIN", "4MIN", "5MIN", "6MIN", "10MIN", "15MIN", "20MIN", "30MIN",
            "1HRS", "2HRS", "3HRS", "4HRS", "6HRS", "8HRS", "12HRS", "1DAY", "2DAY", "3DAY",
            "5DAY", "7DAY", "10DAY",
        ];
        assert_eq!(s.len(), SUPPORTED_PERIODS.len());
        let real: Vec<_> = SUPPORTED_PERIODS.iter().map(|p| p.to_string()).collect();
        for expected in s {
            assert!(real.contains(&expected.to_owned()));
        }
    }

    #[test]
    fn xdb_history_format() {
        let _: TimeseriesData =
            serde_json::from_str(include_str!("../test_files/xdb_history.json")).unwrap();
    }

    #[test]
    fn assets_format() {
        let _: Assets = serde_json::from_str(include_str!("../test_files/assets.json")).unwrap();
    }

    #[test]
    fn exchanges_format() {
        let _: Exchanges =
            serde_json::from_str(include_str!("../test_files/exchanges.json")).unwrap();
    }
}
