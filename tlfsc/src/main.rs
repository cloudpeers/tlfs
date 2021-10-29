use anyhow::Result;
use clap::Parser;
use tlfs_crdt::Ref;

#[derive(Parser)]
struct Cli {
    #[clap(short, long)]
    input: String,
    #[clap(short, long)]
    output: String,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let input = std::fs::read(&cli.input)?;
    let input = std::str::from_utf8(&input)?;
    let lenses = tlfsc::compile_lenses(&input)?;
    let lenses = Ref::archive(&lenses);
    std::fs::write(&cli.output, lenses.as_bytes())?;
    Ok(())
}
