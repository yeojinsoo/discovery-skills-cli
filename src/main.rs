#![allow(unused)]

mod commands;
mod config;
mod lockfile;
mod registry;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "discovery-skills")]
#[command(about = "CLI tool for managing Claude Code custom skills")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Install a skill from the registry
    Install {
        /// Name of the skill to install
        name: String,
    },
    /// Uninstall a previously installed skill
    Uninstall {
        /// Name of the skill to uninstall
        name: String,
    },
    /// List installed skills
    List,
    /// Update installed skills
    Update {
        /// Name of a specific skill to update (updates all if omitted)
        name: Option<String>,
    },
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Install { name } => commands::install::run(&name),
        Commands::Uninstall { name } => commands::uninstall::run(&name),
        Commands::List => commands::list::run(),
        Commands::Update { name } => commands::update::run(name.as_deref()),
    }
}
