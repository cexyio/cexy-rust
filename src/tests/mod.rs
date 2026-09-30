//! Offline tests: mocked HTTP (wiremock), a local WebSocket server, and the shared conformance
//! cases from cexy-api-spec.

mod cancel_all;
mod client;
mod conformance;
mod errors;
mod helpers;
mod models;
mod pagination;
mod paths;
mod redirect;
mod trading;
mod ws;
mod ws_signout;
