//! Generated model behaviour that the API relies on: tagged unions and id aliases.

use serde_json::json;

use crate::{LedgerReference, LedgerReferenceTrade};

#[test]
fn a_known_ledger_reference_decodes_to_its_variant() {
    let r: LedgerReference =
        serde_json::from_value(json!({"type": "trade", "trade_id": "t1", "order_id": "o1"}))
            .unwrap();
    assert_eq!(r.kind(), "trade");
    match &r {
        LedgerReference::Trade(LedgerReferenceTrade {
            trade_id, order_id, ..
        }) => {
            assert_eq!(trade_id, "t1");
            assert_eq!(order_id, "o1");
        }
        other => panic!("unexpected {other:?}"),
    }
    // Round trip: the tag is written back.
    assert_eq!(
        serde_json::to_value(&r).unwrap(),
        json!({"type": "trade", "trade_id": "t1", "order_id": "o1"})
    );
}

#[test]
fn an_unknown_or_malformed_ledger_reference_is_kept_raw() {
    let raw = json!({"type": "airdrop", "campaign": "x"});
    let r: LedgerReference = serde_json::from_value(raw.clone()).unwrap();
    assert_eq!(r, LedgerReference::Unknown(raw.clone()));
    assert_eq!(r.kind(), "airdrop");
    assert_eq!(serde_json::to_value(&r).unwrap(), raw);

    // A known kind missing a required field does not fail the decode either.
    let bad = json!({"type": "deposit"});
    let r: LedgerReference = serde_json::from_value(bad.clone()).unwrap();
    assert_eq!(r, LedgerReference::Unknown(bad));
}

#[test]
fn a_ledger_entry_without_a_reference_still_decodes() {
    let entry = json!({
        "id": "e1", "asset": "BTC", "kind": "deposit", "available_delta": "1", "locked_delta": "0",
        "pending_delta": "0", "available_after": "1", "sequence": 1,
        "created_at": "2026-09-28T00:00:00Z"
    });
    let e: crate::LedgerEntry = serde_json::from_value(entry).unwrap();
    assert_eq!(
        e.reference,
        LedgerReference::Unknown(serde_json::Value::Null)
    );
}

#[test]
fn ids_are_plain_strings() {
    let id: crate::OrderId = "not-24-hex".to_string();
    assert_eq!(id, "not-24-hex");
}
