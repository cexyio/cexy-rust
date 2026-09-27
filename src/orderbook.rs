//! A local order book fed by the `orderbook:{symbol}` channel.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use crate::amount::{BookLevel, levels};
use crate::error::Result;
use crate::models_gen::OrderBook;
use crate::operations_gen::GetOrderBookParams;
use crate::ws::{BookEvent, Inner, OrderBookUpdate, WsEvent, WsFrame};

/// How many levels per side the WebSocket book carries. REST levels deeper than this are never
/// used.
pub const WS_BOOK_DEPTH: usize = 50;

const SNAPSHOT_RETRY_BASE: Duration = Duration::from_secs(1);
const SNAPSHOT_RETRY_MAX: Duration = Duration::from_secs(30);

/// A copy of a [`LiveOrderBook`]'s state. Levels are best first, at most 50 a side.
#[derive(Debug, Clone, PartialEq)]
pub struct BookSnapshot {
    /// Market.
    pub symbol: String,
    /// Bids, best first.
    pub bids: Vec<BookLevel>,
    /// Asks, best first.
    pub asks: Vec<BookLevel>,
    /// Sequence of the last applied snapshot or update on the current connection.
    pub sequence: Option<i64>,
    /// True after a sequence gap, until the next in-order update.
    pub stale: bool,
    /// False while waiting for a snapshot (after a reconnect or a resync).
    pub synced: bool,
}

impl BookSnapshot {
    /// The best bid and ask (`None` for an empty side).
    pub fn best(&self) -> (Option<&BookLevel>, Option<&BookLevel>) {
        (self.bids.first(), self.asks.first())
    }
}

/// A local order book fed by the `orderbook:{symbol}` channel. Create it with
/// [`crate::WebSocket::order_book`]. Cloning is cheap; clones share the book. Changes are
/// reported as [`WsEvent::Book`] events.
#[derive(Clone)]
pub struct LiveOrderBook {
    inner: Arc<BookInner>,
}

struct BookInner {
    symbol: String,
    ws: Weak<Inner>,
    closed: AtomicBool,
    st: Mutex<BookState>,
}

#[derive(Default)]
struct BookState {
    bids: Vec<BookLevel>,
    asks: Vec<BookLevel>,
    seq: Option<i64>,
    stale: bool,
    synced: bool,
    buffer: std::collections::VecDeque<WsFrame>,
    generation: u64,
    /// Buffered updates dropped (oldest first) while no snapshot was available.
    dropped: u64,
}

/// Updates kept while waiting for a snapshot. Each update is a complete top 50, so only the
/// newest matters; the bound keeps a long snapshot outage from growing memory.
pub(crate) const MAX_BUFFERED_UPDATES: usize = 256;

impl std::fmt::Debug for LiveOrderBook {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "cexy::LiveOrderBook({})", self.inner.symbol)
    }
}

impl LiveOrderBook {
    pub(crate) fn new(symbol: &str, ws: Weak<Inner>) -> LiveOrderBook {
        LiveOrderBook {
            inner: Arc::new(BookInner {
                symbol: symbol.to_string(),
                ws,
                closed: AtomicBool::new(false),
                st: Mutex::new(BookState::default()),
            }),
        }
    }

    pub(crate) fn same(&self, other: &LiveOrderBook) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    /// The market symbol.
    pub fn symbol(&self) -> &str {
        &self.inner.symbol
    }

    /// A copy of the current state.
    pub fn snapshot(&self) -> BookSnapshot {
        let s = self.inner.st.lock().unwrap();
        self.snapshot_of(&s)
    }

    fn snapshot_of(&self, s: &BookState) -> BookSnapshot {
        BookSnapshot {
            symbol: self.inner.symbol.clone(),
            bids: s.bids.clone(),
            asks: s.asks.clone(),
            sequence: s.seq,
            stale: s.stale,
            synced: s.synced,
        }
    }

    /// Stops following the book and unsubscribes.
    pub fn close(&self) {
        if self.inner.closed.swap(true, Ordering::SeqCst) {
            return;
        }
        self.inner.st.lock().unwrap().generation += 1;
        if let Some(ws) = self.inner.ws.upgrade() {
            ws.remove_book(&self.inner.symbol, self);
            Inner::unsubscribe_later(&ws, format!("orderbook:{}", self.inner.symbol));
        }
    }

    pub(crate) fn mark_closed(&self) {
        self.inner.closed.store(true, Ordering::SeqCst);
    }

    fn emit(&self, ev: BookEvent) {
        if let Some(ws) = self.inner.ws.upgrade() {
            ws.emit(WsEvent::Book(ev));
        }
    }

    /// Buffered updates dropped while waiting for a snapshot (at most 256 are kept).
    pub fn dropped_updates(&self) -> u64 {
        self.inner.st.lock().unwrap().dropped
    }

    /// The connection dropped, so sequences from the next one are unrelated.
    pub(crate) fn mark_disconnected(&self) {
        let mut s = self.inner.st.lock().unwrap();
        s.generation += 1;
        s.synced = false;
        s.seq = None;
        s.buffer.clear();
    }

    async fn fetch_snapshot(&self) -> Result<OrderBook> {
        let ws = self
            .inner
            .ws
            .upgrade()
            .ok_or_else(|| crate::Error::config("WebSocket dropped"))?;
        let client = ws
            .snapshots
            .clone()
            .ok_or_else(|| crate::Error::config("no REST client"))?;
        let params = GetOrderBookParams {
            depth: Some(WS_BOOK_DEPTH as i64),
        };
        client
            .markets()
            .order_book(&self.inner.symbol, Some(&params))
            .await
    }

    /// Takes the first snapshot and returns its error, if any.
    pub(crate) async fn initial_sync(&self) -> Result<()> {
        let generation = {
            let mut s = self.inner.st.lock().unwrap();
            s.generation += 1;
            s.generation
        };
        let snap = self.fetch_snapshot().await?;
        self.apply_snapshot(generation, &snap);
        Ok(())
    }

    /// Takes a fresh REST snapshot (in a task) and replays buffered updates newer than it.
    pub(crate) fn spawn_resync(&self, attempt: u32) {
        if self.inner.closed.load(Ordering::SeqCst) {
            return;
        }
        let generation = {
            let mut s = self.inner.st.lock().unwrap();
            s.generation += 1;
            s.synced = false;
            s.seq = None; // keep the buffer: newer updates are replayed
            s.generation
        };
        self.emit(BookEvent::Resync {
            symbol: self.inner.symbol.clone(),
        });
        let book = self.clone();
        tokio::spawn(async move {
            let res = tokio::time::timeout(Duration::from_secs(30), book.fetch_snapshot()).await;
            match res {
                Ok(Ok(snap)) => book.apply_snapshot(generation, &snap),
                failed => {
                    if book.inner.st.lock().unwrap().generation != generation
                        || book.inner.closed.load(Ordering::SeqCst)
                    {
                        return;
                    }
                    let message = match failed {
                        Ok(Err(e)) => e.to_string(),
                        _ => "snapshot timed out".to_string(),
                    };
                    book.emit(BookEvent::SnapshotFailed {
                        symbol: book.inner.symbol.clone(),
                        message,
                    });
                    let delay = SNAPSHOT_RETRY_BASE
                        .saturating_mul(1 << attempt.min(10))
                        .min(SNAPSHOT_RETRY_MAX);
                    tokio::time::sleep(delay).await;
                    if book.inner.st.lock().unwrap().generation == generation {
                        book.spawn_resync(attempt + 1);
                    }
                }
            }
        });
    }

    fn apply_snapshot(&self, generation: u64, snap: &OrderBook) {
        let events = {
            let mut s = self.inner.st.lock().unwrap();
            if generation != s.generation || self.inner.closed.load(Ordering::SeqCst) {
                return; // superseded
            }
            s.bids = first_levels(&snap.bids);
            s.asks = first_levels(&snap.asks);
            s.seq = Some(snap.sequence);
            s.stale = false;
            s.synced = true;
            let buffered: Vec<WsFrame> = std::mem::take(&mut s.buffer).into();
            let mut events = vec![BookEvent::Updated(self.snapshot_of(&s))];
            for ev in &buffered {
                events.extend(self.apply(&mut s, ev));
            }
            events
        };
        for e in events {
            self.emit(e);
        }
    }

    /// Runs on the connection task for every `orderbook.update` of this symbol.
    pub(crate) fn on_update(&self, ev: &WsFrame) {
        if self.inner.closed.load(Ordering::SeqCst) {
            return;
        }
        let events = {
            let mut s = self.inner.st.lock().unwrap();
            if !s.synced {
                s.buffer.push_back(ev.clone());
                if s.buffer.len() > MAX_BUFFERED_UPDATES {
                    s.buffer.pop_front();
                    s.dropped += 1;
                    let first = s.dropped == 1;
                    drop(s);
                    if first && let Some(ws) = self.inner.ws.upgrade() {
                        ws.emit(WsEvent::Warning(format!(
                            "order book {}: no snapshot yet; keeping only the newest {MAX_BUFFERED_UPDATES} updates (older ones dropped; each update is a full top 50)",
                            self.inner.symbol
                        )));
                    }
                }
                return;
            }
            self.apply(&mut s, ev)
        };
        for e in events {
            self.emit(e);
        }
    }

    /// Applies one update and returns the events to report.
    fn apply(&self, s: &mut BookState, ev: &WsFrame) -> Vec<BookEvent> {
        let mut events = vec![];
        if let (Some(seq), Some(cur)) = (ev.sequence, s.seq) {
            if seq <= cur {
                return events; // already covered by the snapshot or applied
            }
            if seq != cur + 1 {
                s.stale = true;
                events.push(BookEvent::Stale {
                    symbol: self.inner.symbol.clone(),
                    expected: cur + 1,
                    received: seq,
                });
            } else if s.stale {
                s.stale = false;
                events.push(BookEvent::Healed {
                    symbol: self.inner.symbol.clone(),
                });
            }
        }
        let Ok(d) = ev.decode::<OrderBookUpdate>() else {
            return events;
        };
        // Each update is the complete top 50 of both sides: replace, never merge.
        s.bids = first_levels(&d.bids);
        s.asks = first_levels(&d.asks);
        if ev.sequence.is_some() {
            s.seq = ev.sequence;
        }
        events.push(BookEvent::Updated(self.snapshot_of(s)));
        events
    }
}

fn first_levels(raw: &[Vec<crate::Amount>]) -> Vec<BookLevel> {
    let mut l = levels(raw);
    l.truncate(WS_BOOK_DEPTH);
    l
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_update_buffer_is_bounded_while_no_snapshot_arrives() {
        let book = LiveOrderBook::new("BTC/USDT", std::sync::Weak::new());
        for seq in 0..(MAX_BUFFERED_UPDATES as i64 + 44) {
            let frame: WsFrame = serde_json::from_value(serde_json::json!({
                "type": "orderbook.update", "channel": "orderbook:BTC/USDT", "sequence": seq,
                "data": {"bids": [], "asks": [], "full": true}
            }))
            .unwrap();
            book.on_update(&frame);
        }
        let s = book.inner.st.lock().unwrap();
        assert_eq!(s.buffer.len(), MAX_BUFFERED_UPDATES);
        assert_eq!(s.buffer.front().and_then(|f| f.sequence), Some(44));
        drop(s);
        assert_eq!(book.dropped_updates(), 44);
    }
}
