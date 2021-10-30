use anyhow::Result;
use fnv::FnvHashMap;
use pest::iterators::Pair;
use pest::Parser;
use pest_derive::Parser;
use std::path::Path;
use tlfs_crdt::{Kind, Lens, Lenses, Package, PrimitiveKind, Ref, Schema};

#[derive(Parser)]
#[grammar = "grammar.pest"]
struct GrammarParser;

pub fn compile<P: AsRef<Path>>(input: P, output: P) -> Result<()> {
    let input = std::fs::read(input)?;
    let input = std::str::from_utf8(&input)?;
    let lenses = compile_lenses(input)?;
    let lenses = Ref::archive(&lenses);
    std::fs::write(output, lenses.as_bytes())?;
    Ok(())
}

pub fn compile_lenses(input: &str) -> Result<Vec<Package>> {
    let root = GrammarParser::parse(Rule::root, input)?;
    let mut interpreter = Interpreter::default();
    for pair in root {
        for pair in pair.into_inner() {
            if pair.as_rule() == Rule::schema {
                interpreter.schema(pair);
            }
        }
    }
    Ok(interpreter.into_packages())
}

#[derive(Debug, Default)]
pub struct Interpreter {
    name: Option<String>,
    builder: Option<SchemaBuilder>,
    schemas: FnvHashMap<String, SchemaBuilder>,
}

impl Interpreter {
    fn start_schema(&mut self, name: String) {
        self.name = Some(name);
        self.builder = Some(SchemaBuilder::default());
    }

    fn end_schema(&mut self) {
        let name = self.name.take().unwrap();
        let builder = self.builder.take().unwrap();
        if self.schemas.contains_key(&name) {
            panic!("schema with name {} already exists", name);
        }
        self.schemas.insert(name, builder);
    }

    pub fn schema(&mut self, pair: Pair<Rule>) {
        for pair in pair.into_inner() {
            match pair.as_rule() {
                Rule::ident => {
                    self.start_schema(pair.as_str().into());
                }
                Rule::schema_version => {
                    self.builder.as_mut().unwrap().schema_version(pair);
                }
                _ => {}
            }
        }
        self.end_schema();
    }

    pub fn into_packages(self) -> Vec<Package> {
        let mut lenses = vec![];
        for (name, builder) in self.schemas {
            lenses.push(Package::new(
                name,
                builder.lenses.len() as u32,
                &Lenses::new(builder.lenses),
            ));
        }
        lenses
    }
}

#[derive(Debug)]
enum Segment {
    LensMap,
    LensMapValue,
    Field(String),
    Remove,
    Rename(String),
    Hoist,
    Plunge(String),
}

#[derive(Debug, Default)]
pub struct SchemaBuilder {
    version: Option<String>,
    schema: Schema,
    lenses: Vec<Lens>,
    versions: Vec<(String, u32)>,
}

impl SchemaBuilder {
    fn start_version(&mut self, version: String) {
        self.version = Some(version);
    }

    fn end_version(&mut self) {
        let version = self.version.take().unwrap();
        self.versions.push((version, self.lenses.len() as u32));
    }

    pub fn schema_version(&mut self, pair: Pair<Rule>) {
        for pair in pair.into_inner() {
            match pair.as_rule() {
                Rule::version => {
                    self.start_version(pair.as_str().into());
                }
                Rule::rule => {
                    self.rule(pair);
                }
                _ => {}
            }
        }
        self.end_version();
    }

    fn add_lens(&mut self, segments: &[Segment], mut lens: Lens) {
        for seg in segments.iter().rev() {
            match seg {
                Segment::LensMap => lens = Lens::LensMap(Box::new(lens)),
                Segment::LensMapValue => lens = Lens::LensMapValue(Box::new(lens)),
                Segment::Field(field) => lens = Lens::LensIn(field.into(), Box::new(lens)),
                _ => unreachable!(),
            }
        }
        Ref::archive(&lens)
            .as_ref()
            .to_ref()
            .transform_schema(&mut self.schema)
            .unwrap();
        self.lenses.push(lens);
    }

    fn kind_of(&mut self, segments: &[Segment]) -> Kind {
        let mut schema = &self.schema;
        for seg in segments {
            match (seg, schema) {
                (Segment::Field(field), Schema::Struct(fields)) => {
                    schema = fields.get(field).unwrap();
                }
                (Segment::LensMap, Schema::Array(array)) => {
                    schema = array;
                }
                (Segment::LensMapValue, Schema::Table(_, value)) => {
                    schema = value;
                }
                (seg, schema) => panic!("invalid segment {:?} {:?}", seg, schema),
            }
        }
        match schema {
            Schema::Flag => Kind::Flag,
            Schema::Reg(kind) => Kind::Reg(*kind),
            Schema::Table(kind, _) => Kind::Table(*kind),
            Schema::Struct(_) => Kind::Struct,
            Schema::Array(_) => Kind::Array,
            Schema::Null => panic!("unexpected schema null"),
        }
    }

    fn rule(&mut self, pair: Pair<Rule>) {
        let mut segments = None;
        let mut kind = None;
        for pair in pair.into_inner() {
            match pair.as_rule() {
                Rule::path => {
                    segments = Some(self.path(pair));
                }
                Rule::ty => {
                    kind = Some(self.ty(pair));
                }
                _ => {}
            }
        }
        let mut segments = segments.unwrap();
        if let Some(kind) = kind {
            match segments.pop() {
                Some(Segment::Field(field)) => {
                    self.add_lens(&segments, Lens::AddProperty(field.clone()));
                    segments.push(Segment::Field(field));
                    self.add_lens(&segments, Lens::Make(kind));
                }
                Some(seg) => {
                    segments.push(seg);
                    self.add_lens(&segments, Lens::Make(kind));
                }
                None => {
                    self.add_lens(&segments, Lens::Make(kind));
                }
            }
        } else {
            match segments.pop() {
                Some(Segment::Remove) => {
                    let kind = self.kind_of(&segments);
                    self.add_lens(&segments, Lens::Destroy(kind));
                    if let Some(Segment::Field(field)) = segments.pop() {
                        self.add_lens(&segments, Lens::RemoveProperty(field));
                    }
                }
                Some(Segment::Rename(to)) => {
                    if let Some(Segment::Field(from)) = segments.pop() {
                        self.add_lens(&segments, Lens::RenameProperty(from, to));
                    } else {
                        panic!("invalid rename operation");
                    }
                }
                Some(Segment::Hoist) => {
                    let target = segments.pop();
                    let host = segments.pop();
                    if let (Some(Segment::Field(host)), Some(Segment::Field(target))) =
                        (host, target)
                    {
                        self.add_lens(&segments, Lens::HoistProperty(host, target));
                    } else {
                        panic!("invalid hoist operation");
                    }
                }
                Some(Segment::Plunge(host)) => {
                    if let Some(Segment::Field(target)) = segments.pop() {
                        self.add_lens(&segments, Lens::PlungeProperty(host, target));
                    } else {
                        panic!("invalid plunge operation");
                    }
                }
                Some(seg) => panic!("unexpected segment {:?}", seg),
                None => panic!("expected segment"),
            }
        }
    }

    fn path(&mut self, pair: Pair<Rule>) -> Vec<Segment> {
        let mut segments = vec![];
        for pair in pair.into_inner().flatten() {
            if pair.as_rule() == Rule::segment {
                match pair.as_str() {
                    "[]" => segments.push(Segment::LensMap),
                    "{}" => segments.push(Segment::LensMapValue),
                    _ => {
                        for pair in pair.into_inner() {
                            if pair.as_rule() == Rule::invocation {
                                if pair.as_str().ends_with(')') {
                                    segments.push(self.invocation(pair));
                                } else {
                                    segments.push(Segment::Field(pair.as_str().into()));
                                }
                            }
                        }
                    }
                }
            }
        }
        segments
    }

    fn invocation(&mut self, pair: Pair<Rule>) -> Segment {
        let mut method = None;
        let mut segment = None;
        for pair in pair.into_inner().into_iter() {
            if pair.as_rule() == Rule::ident {
                match (method, pair.as_str()) {
                    (None, "remove") => {
                        method = Some("remove");
                        segment = Some(Segment::Remove);
                    }
                    (None, "hoist") => {
                        method = Some("hoist");
                        segment = Some(Segment::Hoist);
                    }
                    (None, "rename") => method = Some("rename"),
                    (None, "plunge") => method = Some("plunge"),
                    (Some("rename"), arg) => segment = Some(Segment::Rename(arg.into())),
                    (Some("plunge"), arg) => segment = Some(Segment::Plunge(arg.into())),
                    _ => panic!("unexpected lens {}", pair.as_str()),
                }
            }
        }
        segment.unwrap()
    }

    fn ty(&mut self, pair: Pair<Rule>) -> Kind {
        let mut prim_kind = None;
        let mut kind = None;
        for pair in pair.into_inner().into_iter().rev() {
            if pair.as_rule() == Rule::ident {
                match (prim_kind, pair.as_str()) {
                    (None, "bool") => prim_kind = Some(PrimitiveKind::Bool),
                    (None, "u64") => prim_kind = Some(PrimitiveKind::U64),
                    (None, "i64") => prim_kind = Some(PrimitiveKind::I64),
                    (None, "String") => prim_kind = Some(PrimitiveKind::Str),
                    (None, "EWFlag") => kind = Some(Kind::Flag),
                    (None, "Struct") => kind = Some(Kind::Struct),
                    (None, "Array") => kind = Some(Kind::Array),
                    (Some(prim_kind), "MVReg") => kind = Some(Kind::Reg(prim_kind)),
                    (Some(prim_kind), "Table") => kind = Some(Kind::Table(prim_kind)),
                    _ => panic!("unexpected type {}", pair.as_str()),
                }
            }
        }
        kind.unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compile() -> Result<()> {
        let lenses = r#"
todoapp {
  0.1.0 {
    .: Struct
    .todos: Table<u64>
    .todos.{}: Struct
    .todos.{}.title: MVReg<String>
    .todos.{}.complete: EWFlag
  }
  0.1.1 {
    .todos.rename(tasks)
    .tasks.{}.attrs: Struct
    .tasks.{}.title.plunge(attrs)
    .tasks.{}.attrs.title.hoist()
    .tasks.{}.attrs.obsolete: Struct
    .tasks.{}.attrs.obsolete.remove()
    .tasks.{}.attrs.remove()
  }
}
    "#;
        compile_lenses(lenses)?;
        Ok(())
    }
}
