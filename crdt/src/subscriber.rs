use crate::acl::Rule;
use crate::{Path, PeerId, Permission};
use futures::{Future, Stream};
use rkyv::archived_root;
use std::pin::Pin;
use std::task::{Context, Poll};

#[derive(Debug)]
pub enum Event<'a> {
    Insert(Path<'a>),
    Remove(Path<'a>),
    Granted(Path<'a>, Option<PeerId>, Permission),
    Revoked(Path<'a>, Option<PeerId>),
}

pub enum Iter<'a> {
    State(Box<dyn Iterator<Item = (&'a sled::Tree, &'a sled::IVec, &'a Option<sled::IVec>)> + 'a>),
    Acl(Box<dyn Iterator<Item = (&'a sled::Tree, &'a sled::IVec, &'a Option<sled::IVec>)> + 'a>),
}

impl<'a> Iterator for Iter<'a> {
    type Item = Event<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::State(state) => match state.next() {
                Some((_, k, Some(_))) => Some(Event::Insert(Path::new(k))),
                Some((_, k, None)) => Some(Event::Remove(Path::new(k))),
                None => None,
            },
            Self::Acl(acl) => match acl.next() {
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

pub enum Batch {
    State(sled::Event),
    Acl(sled::Event),
}

impl<'a> IntoIterator for &'a Batch {
    type Item = Event<'a>;
    type IntoIter = Iter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            Batch::State(ev) => Iter::State(ev.into_iter()),
            Batch::Acl(ev) => Iter::Acl(ev.into_iter()),
        }
    }
}

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
            return Poll::Ready(Some(Batch::State(ev)));
        }
        if let Poll::Ready(Some(ev)) = Pin::new(&mut self.acl).poll(cx) {
            return Poll::Ready(Some(Batch::Acl(ev)));
        }
        Poll::Pending
    }
}
