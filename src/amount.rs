//! Decimal amounts as strings, and order-book levels.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::error::Error;
use crate::models_gen::ServerTime;

/// An exact decimal amount as a string, such as `"0.00150000"`. The API never uses JSON numbers
/// for money. Do arithmetic with a decimal crate (for example `rust_decimal`), never with `f64`.
///
/// [`Amount::new`] (and `parse`) accept only plain decimal strings: digits, an optional `.` with
/// digits after it, and an optional leading `-`. Amounts received from the API are taken as sent.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Amount(String);

impl Amount {
    /// A validated amount: `Amount::new("0.5")`.
    pub fn new(value: impl Into<String>) -> Result<Self, Error> {
        let value = value.into();
        if is_decimal(&value) {
            Ok(Amount(value))
        } else {
            Err(Error::InvalidAmount {
                field: String::new(),
                message: format!(
                    "an amount must be a plain decimal string such as \"0.5\" (got {value:?})"
                ),
            })
        }
    }

    /// The decimal text.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Whether the text is a plain decimal string.
    pub fn is_valid(&self) -> bool {
        is_decimal(&self.0)
    }

    /// The decimal text, by value.
    pub fn into_string(self) -> String {
        self.0
    }
}

fn is_decimal(s: &str) -> bool {
    let s = s.strip_prefix('-').unwrap_or(s);
    let (int, frac) = match s.split_once('.') {
        Some((i, f)) => (i, Some(f)),
        None => (s, None),
    };
    !int.is_empty()
        && int.bytes().all(|b| b.is_ascii_digit())
        && frac.is_none_or(|f| !f.is_empty() && f.bytes().all(|b| b.is_ascii_digit()))
}

impl fmt::Debug for Amount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Amount({:?})", self.0)
    }
}

impl fmt::Display for Amount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for Amount {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Error> {
        Amount::new(s)
    }
}

impl TryFrom<&str> for Amount {
    type Error = Error;
    fn try_from(s: &str) -> Result<Self, Error> {
        Amount::new(s)
    }
}

impl TryFrom<String> for Amount {
    type Error = Error;
    fn try_from(s: String) -> Result<Self, Error> {
        Amount::new(s)
    }
}

/// Returns an [`Error::InvalidAmount`] for the first malformed amount; `None` (optional fields)
/// passes.
pub(crate) fn check_amounts(
    context: &str,
    fields: &[(&str, Option<&Amount>)],
) -> Result<(), Error> {
    for (name, value) in fields {
        if let Some(v) = value
            && !v.is_valid()
        {
            return Err(Error::InvalidAmount {
                field: (*name).to_string(),
                message: format!(
                    "{context}: {name} must be a plain decimal string such as \"0.5\" (got {:?})",
                    v.as_str()
                ),
            });
        }
    }
    Ok(())
}

/// One `[price, quantity]` level of an order book.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BookLevel {
    /// Price.
    pub price: Amount,
    /// Quantity at that price.
    pub quantity: Amount,
}

/// Converts the raw `[price, quantity]` pairs of an order-book side. Entries with fewer than two
/// values are skipped.
pub fn levels(raw: &[Vec<Amount>]) -> Vec<BookLevel> {
    raw.iter()
        .filter(|l| l.len() >= 2)
        .map(|l| BookLevel {
            price: l[0].clone(),
            quantity: l[1].clone(),
        })
        .collect()
}

impl ServerTime {
    /// The server time, parsed from `iso`.
    pub fn time(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        chrono::DateTime::parse_from_rfc3339(&self.iso)
            .ok()
            .map(|t| t.with_timezone(&chrono::Utc))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validation() {
        for ok in ["0", "0.5", "-1.25", "60000.00", "123"] {
            assert!(Amount::new(ok).is_ok(), "{ok}");
        }
        for bad in [
            "", ".5", "5.", "1e3", "0x10", "1,5", " 1", "NaN", "--1", "+1",
        ] {
            assert!(Amount::new(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn serde_is_a_plain_string() {
        let a = Amount::new("0.5").unwrap();
        assert_eq!(serde_json::to_string(&a).unwrap(), "\"0.5\"");
        let b: Amount = serde_json::from_str("\"1.00\"").unwrap();
        assert_eq!(b.as_str(), "1.00");
    }

    #[test]
    fn levels_skip_short_entries() {
        let a = |s: &str| Amount::new(s).unwrap();
        let raw = vec![vec![a("1"), a("2")], vec![a("3")]];
        assert_eq!(
            levels(&raw),
            vec![BookLevel {
                price: a("1"),
                quantity: a("2")
            }]
        );
    }
}
