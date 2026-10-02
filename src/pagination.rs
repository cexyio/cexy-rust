//! Cursor pagination: one page at a time, or every item as a stream.

use std::collections::{HashSet, VecDeque};
use std::future::Future;
use std::pin::Pin;

use futures_util::Stream;
use serde::Deserialize;

use crate::error::{Error, Result};

/// One page of a cursor-paginated listing.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Page<T> {
    /// The items of this page.
    #[serde(default = "Vec::new")]
    pub items: Vec<T>,
    /// Whether another page exists.
    #[serde(default)]
    pub has_more: bool,
    /// Pass it back as `cursor` to get the next page. `None` on the last page.
    #[serde(default)]
    pub next_cursor: Option<String>,
}

/// Every item of a listing, fetched page by page as it is consumed. Use it with
/// `futures_util::StreamExt` (`next().await`) or `TryStreamExt` (`try_collect`).
pub type ItemStream<'a, T> = Pin<Box<dyn Stream<Item = Result<T>> + Send + 'a>>;

struct State<F, T> {
    fetch: F,
    cursor: Option<String>,
    buffer: VecDeque<T>,
    seen: HashSet<String>,
    yielded: usize,
    max_items: Option<usize>,
    done: bool,
}

/// Walks a listing page by page, yielding items one at a time and fetching the next page
/// lazily. It stops on the last page (`has_more` false or no `next_cursor`), after `max_items`,
/// or at the first error (yielded once).
pub(crate) fn paginate<'a, T, F, Fut>(
    start: Option<String>,
    max_items: Option<usize>,
    fetch: F,
) -> ItemStream<'a, T>
where
    T: Send + 'a,
    F: FnMut(Option<String>) -> Fut + Send + 'a,
    Fut: Future<Output = Result<Page<T>>> + Send + 'a,
{
    let state = State {
        fetch,
        cursor: start,
        buffer: VecDeque::new(),
        seen: HashSet::new(),
        yielded: 0,
        max_items,
        done: false,
    };
    Box::pin(futures_util::stream::unfold(state, |mut s| async move {
        loop {
            if s.max_items.is_some_and(|m| s.yielded >= m) {
                return None;
            }
            if let Some(item) = s.buffer.pop_front() {
                s.yielded += 1;
                return Some((Ok(item), s));
            }
            if s.done {
                return None;
            }
            match (s.fetch)(s.cursor.clone()).await {
                Err(e) => {
                    s.done = true;
                    return Some((Err(e), s));
                }
                Ok(page) => {
                    for item in page.items {
                        s.buffer.push_back(item);
                    }
                    match page.next_cursor.filter(|c| page.has_more && !c.is_empty()) {
                        // A repeated cursor would loop forever.
                        Some(next) if s.seen.insert(next.clone()) => s.cursor = Some(next),
                        _ => s.done = true,
                    }
                }
            }
        }
    }))
}

/// One page of a futures account history (fills, funding), reduced to what paging needs.
pub(crate) struct HistoryPage<T> {
    pub(crate) rows: Vec<T>,
    pub(crate) has_account: bool,
    pub(crate) next_cursor: Option<String>,
}

struct HistoryState<F, W, T> {
    fetch: F,
    wait: W,
    operation: &'static str,
    max_busy_retries: u32,
    cursor: Option<String>,
    retries: u32,
    buffer: VecDeque<T>,
    seen: HashSet<String>,
    yielded: usize,
    max_items: Option<usize>,
    done: bool,
    /// An error to yield once the buffered rows are out.
    pending: Option<Error>,
}

/// Walks a futures account history (conformance/futures/history_paging.json):
///
/// - the first request sends no cursor; each `next_cursor` is sent back exactly as given;
/// - a page may be short, even empty, and still have a `next_cursor`: paging goes on until it is
///   `None`;
/// - an EMPTY page whose `next_cursor` is the cursor just sent means the provider is busy: `wait`
///   (the transport's backoff, on the client's clock) and ask for the same cursor again, at most
///   `max_busy_retries` times in a row (its own setting, not the client's request retries), then
///   yield [`Error::PagingStalled`] (retryable) and stop;
/// - a page WITH rows whose `next_cursor` was already sent would repeat rows: its rows are
///   yielded, then [`Error::PagingCursorRepeated`] (not retryable), never a loop;
/// - `has_account` false ends the stream with no rows.
pub(crate) fn paginate_history<'a, T, F, Fut, W, WFut>(
    operation: &'static str,
    max_busy_retries: u32,
    max_items: Option<usize>,
    fetch: F,
    wait: W,
) -> ItemStream<'a, T>
where
    T: Send + 'a,
    F: FnMut(Option<String>) -> Fut + Send + 'a,
    Fut: Future<Output = Result<HistoryPage<T>>> + Send + 'a,
    W: FnMut(u32, Error) -> WFut + Send + 'a,
    WFut: Future<Output = ()> + Send + 'a,
{
    let state = HistoryState {
        fetch,
        wait,
        operation,
        max_busy_retries,
        cursor: None,
        retries: 0,
        buffer: VecDeque::new(),
        seen: HashSet::new(),
        yielded: 0,
        max_items,
        done: false,
        pending: None,
    };
    Box::pin(futures_util::stream::unfold(state, |mut s| async move {
        loop {
            if s.max_items.is_some_and(|m| s.yielded >= m) {
                return None;
            }
            if let Some(item) = s.buffer.pop_front() {
                s.yielded += 1;
                return Some((Ok(item), s));
            }
            if s.done {
                return s.pending.take().map(|e| (Err(e), s));
            }
            let page = match (s.fetch)(s.cursor.clone()).await {
                Ok(page) => page,
                Err(e) => {
                    s.done = true;
                    return Some((Err(e), s));
                }
            };
            if !page.has_account {
                s.done = true;
                continue;
            }
            let next = page.next_cursor;
            if page.rows.is_empty() && next.is_some() && next == s.cursor {
                let stalled = Error::PagingStalled {
                    operation: s.operation,
                    cursor: next.unwrap_or_default(),
                    retries: s.retries,
                };
                if s.retries >= s.max_busy_retries {
                    s.done = true;
                    return Some((Err(stalled), s));
                }
                (s.wait)(s.retries, stalled).await;
                s.retries += 1;
                continue;
            }
            s.retries = 0;
            s.buffer.extend(page.rows);
            match next {
                None => s.done = true,
                Some(n) if !s.seen.insert(n.clone()) => {
                    s.done = true;
                    // The rows of this page first, then the error.
                    s.pending = Some(Error::PagingCursorRepeated {
                        operation: s.operation,
                        cursor: n,
                    });
                }
                Some(n) => s.cursor = Some(n),
            }
        }
    }))
}
