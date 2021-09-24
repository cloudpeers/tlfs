use rkyv::ser::serializers::AllocSerializer;
use rkyv::ser::Serializer;
use tlfs_cambria::{Cambria, Ptr};

mod schema;
mod schema2;

use schema::Doc;
use schema2::Doc2;

fn main() {
    let doc = Doc {
        done: true,
        xanswer: 42,
        shopping: vec!["cheese".into(), "eggs".into(), "milk".into()],
    };

    let mut ser = AllocSerializer::<256>::default();
    ser.serialize_value(&doc).unwrap();
    let bytes = ser.into_serializer().into_inner().to_vec();
    let ptr = Ptr::new(&bytes, Doc::schema());

    assert_eq!(
        ptr.keys().unwrap().collect::<Vec<_>>(),
        vec!["done", "shopping", "xanswer"]
    );
    let done = ptr.get("done").unwrap().boolean().unwrap();
    assert!(done);

    let answer = ptr.get("xanswer").unwrap().number().unwrap();
    assert_eq!(answer, 42);

    let shopping = ptr.get("shopping").unwrap();
    assert_eq!(shopping.len().unwrap(), 3);
    let cheese = shopping.idx(0).unwrap();
    assert_eq!(cheese.string().unwrap(), "cheese");
    let eggs = shopping.idx(1).unwrap();
    assert_eq!(eggs.string().unwrap(), "eggs");

    let doc2 = Doc2::transform(Doc::lenses(), &bytes).unwrap();
    println!("{:?}", doc);
    println!("{:?}", doc2);
}
