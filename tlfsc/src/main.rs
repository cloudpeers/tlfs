use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
struct Cli {
    #[clap(short, long)]
    input: PathBuf,
    #[clap(short, long)]
    output: PathBuf,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    tlfsc::compile(&cli.input, &cli.output)?;
    Ok(())
}
