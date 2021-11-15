use crate::acl::{Permission, Rule};
use crate::id::PeerId;
use crate::path::Path;
use crate::PathBuf;
use futures::stream::BoxStream;
use futures::Stream;
use rkyv::archived_root;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use vec_collections::IterKey;

/// Event returned from a subscription.
#[derive(Debug)]
pub enum Event {
    /// [`Path`] was inserted into the store.
    Insert(PathBuf),
    /// [`Path`] was removed from the store.
    Remove(PathBuf),
    /// [`PeerId`] was granted [`Permission`] for [`Path`].
    Granted(PathBuf, Option<PeerId>, Permission),
    /// [`PeerId`] has it's [`Permission`] for [`Path`] revoked.
    Revoked(PathBuf, Option<PeerId>),
}

#[allow(clippy::type_complexity)]
enum InnerIter<'a> {
    State(Box<dyn Iterator<Item = (IterKey<u8>, Option<&'a ()>)> + 'a>),
    Acl(Box<dyn Iterator<Item = (IterKey<u8>, Option<&'a Arc<[u8]>>)> + 'a>),
}

/// [`Event`] iterator returned from `[`Batch`].into_iter()`.
pub struct Iter<'a>(InnerIter<'a>);

impl<'a> Iterator for Iter<'a> {
    type Item = Event;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.0 {
            InnerIter::State(state) => match state.next() {
                Some((k, Some(_))) => Some(Event::Insert(Path::new(&k).to_owned())),
                Some((k, None)) => Some(Event::Remove(Path::new(&k).to_owned())),
                None => None,
            },
            InnerIter::Acl(acl) => match acl.next() {
                Some((k, Some(v))) => {
                    let (peer, path) = Path::new(&k).child().unwrap().split_first().unwrap();
                    let peer = peer.peer().unwrap();
                    let actor = if peer == PeerId::new([0; 32]) {
                        None
                    } else {
                        Some(peer)
                    };
                    let perm = unsafe { archived_root::<Rule>(v) }.perm;
                    Some(Event::Granted(path.to_owned(), actor, perm))
                }
                Some((k, None)) => {
                    let (peer, path) = Path::new(&k).child().unwrap().split_first().unwrap();
                    let peer = peer.peer().unwrap();
                    let actor = if peer == PeerId::new([0; 32]) {
                        None
                    } else {
                        Some(peer)
                    };
                    Some(Event::Revoked(path.to_owned(), actor))
                }
                None => None,
            },
        }
    }
}

enum InnerBatch {
    State(crate::radixdb::Diff<u8, ()>),
    Acl(crate::radixdb::Diff<u8, Arc<[u8]>>),
}

/// Batch of [`Event`]s returned from [`Subscriber`].
pub struct Batch(InnerBatch);

impl<'a> IntoIterator for &'a Batch {
    type Item = Event;
    type IntoIter = Iter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        match &self.0 {
            InnerBatch::State(ev) => Iter(InnerIter::State(Box::new(ev.iter()))),
            InnerBatch::Acl(ev) => Iter(InnerIter::Acl(Box::new(ev.iter()))),
        }
    }
}

/// [`Event`] [`Stream`] subscription.
pub struct Subscriber {
    state: BoxStream<'static, crate::radixdb::Diff<u8, ()>>,
    acl: BoxStream<'static, crate::radixdb::Diff<u8, Arc<[u8]>>>,
}

impl Subscriber {
    pub(crate) fn new(
        state: BoxStream<'static, crate::radixdb::Diff<u8, ()>>,
        acl: BoxStream<'static, crate::radixdb::Diff<u8, Arc<[u8]>>>,
    ) -> Self {
        Self { state, acl }
    }
}

impl Stream for Subscriber {
    type Item = Batch;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        if let Poll::Ready(Some(ev)) = Pin::new(&mut self.state).poll_next(cx) {
            return Poll::Ready(Some(Batch(InnerBatch::State(ev))));
        }
        if let Poll::Ready(Some(ev)) = Pin::new(&mut self.acl).poll_next(cx) {
            return Poll::Ready(Some(Batch(InnerBatch::Acl(ev))));
        }
        Poll::Pending
    }
}
