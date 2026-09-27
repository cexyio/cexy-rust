//! Cursor pagination: one page at a time, or every item as a stream.

use std::collections::{HashSet, VecDeque};
use std::future::Future;
use std::pin::Pin;

use futures_util::Stream;
use serde::Deserialize;

use crate::error::Result;

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
