use tlfs_cambria::{Kind, Lens, Lenses, PrimitiveKind};

fn main() {
    let mut lenses = vec![
        Lens::Make(Kind::Object),
        Lens::AddProperty("shopping".into()),
        Lens::LensIn("shopping".into(), Box::new(Lens::Make(Kind::Array))),
        Lens::LensIn(
            "shopping".into(),
            Box::new(Lens::LensMap(Box::new(Lens::Make(Kind::Primitive(
                PrimitiveKind::Text,
            ))))),
        ),
        Lens::AddProperty("done".into()),
        Lens::LensIn(
            "done".into(),
            Box::new(Lens::Make(Kind::Primitive(PrimitiveKind::Boolean))),
        ),
        Lens::AddProperty("xanswer".into()),
        Lens::LensIn(
            "xanswer".into(),
            Box::new(Lens::Make(Kind::Primitive(PrimitiveKind::Number))),
        ),
    ];
    let doc = tlfs_cambria::precompile("Doc", Lenses::new(lenses.clone()));
    lenses.push(Lens::RenameProperty(
        "shopping".into(),
        "shopping_list".into(),
    ));
    let doc2 = tlfs_cambria::precompile("Doc2", Lenses::new(lenses));
    tlfs_cambria::write_tokens("src/schema.rs", &doc);
    tlfs_cambria::write_tokens("src/schema2.rs", &doc2);
}
