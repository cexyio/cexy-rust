//! Offline tests: mocked HTTP (wiremock), a local WebSocket server, and the shared conformance
//! cases from cexy-api-spec.

mod cancel_all;
mod client;
mod conformance;
mod errors;
mod futures;
mod helpers;
mod models;
mod pagination;
mod paths;
mod redirect;
mod signing;
mod trading;
mod ws;
mod ws_live_balances;
mod ws_signout;
