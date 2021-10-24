use crate::acl::{Permission, Rule};
use crate::id::PeerId;
use crate::path::Path;
use futures::{Future, Stream};
use rkyv::archived_root;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Event returned from a subscription.
#[derive(Debug)]
pub enum Event<'a> {
    /// [`Path`] was inserted into the store.
    Insert(Path<'a>),
    /// [`Path`] was removed from the store.
    Remove(Path<'a>),
    /// [`PeerId`] was granted [`Permission`] for [`Path`].
    Granted(Path<'a>, Option<PeerId>, Permission),
    /// [`PeerId`] has it's [`Permission`] for [`Path`] revoked.
    Revoked(Path<'a>, Option<PeerId>),
}

enum InnerIter<'a> {
    State(Box<dyn Iterator<Item = (&'a sled::Tree, &'a sled::IVec, &'a Option<sled::IVec>)> + 'a>),
    Acl(Box<dyn Iterator<Item = (&'a sled::Tree, &'a sled::IVec, &'a Option<sled::IVec>)> + 'a>),
}

/// [`Event`] iterator returned from `[`Batch`].into_iter()`.
pub struct Iter<'a>(InnerIter<'a>);

impl<'a> Iterator for Iter<'a> {
    type Item = Event<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.0 {
            InnerIter::State(state) => match state.next() {
                Some((_, k, Some(_))) => Some(Event::Insert(Path::new(k))),
                Some((_, k, None)) => Some(Event::Remove(Path::new(k))),
                None => None,
            },
            InnerIter::Acl(acl) => match acl.next() {
                Some((_, k, Some(v))) => {
                    let (peer, path) = Path::new(k).child().unwrap().split_first().unwrap();
                    let peer = peer.peer().unwrap();
                    let actor = if peer == PeerId::new([0; 32]) {
                        None
                    } else {
                        Some(peer)
                    };
                    let perm = unsafe { archived_root::<Rule>(v) }.perm;
                    Some(Event::Granted(path, actor, perm))
                }
                Some((_, k, None)) => {
                    let (peer, path) = Path::new(k).child().unwrap().split_first().unwrap();
                    let peer = peer.peer().unwrap();
                    let actor = if peer == PeerId::new([0; 32]) {
                        None
                    } else {
                        Some(peer)
                    };
                    Some(Event::Revoked(path, actor))
                }
                None => None,
            },
        }
    }
}

enum InnerBatch {
    State(sled::Event),
    Acl(sled::Event),
}

/// Batch of [`Event`]s returned from [`Subscriber`].
pub struct Batch(InnerBatch);

impl<'a> IntoIterator for &'a Batch {
    type Item = Event<'a>;
    type IntoIter = Iter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        match &self.0 {
            InnerBatch::State(ev) => Iter(InnerIter::State(ev.into_iter())),
            InnerBatch::Acl(ev) => Iter(InnerIter::Acl(ev.into_iter())),
        }
    }
}

/// [`Event`] [`Stream`] subscription.
pub struct Subscriber {
    state: sled::Subscriber,
    acl: sled::Subscriber,
}

impl Subscriber {
    pub(crate) fn new(state: sled::Subscriber, acl: sled::Subscriber) -> Self {
        Self { state, acl }
    }
}

impl Stream for Subscriber {
    type Item = Batch;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        if let Poll::Ready(Some(ev)) = Pin::new(&mut self.state).poll(cx) {
            return Poll::Ready(Some(Batch(InnerBatch::State(ev))));
        }
        if let Poll::Ready(Some(ev)) = Pin::new(&mut self.acl).poll(cx) {
            return Poll::Ready(Some(Batch(InnerBatch::Acl(ev))));
        }
        Poll::Pending
    }
}
