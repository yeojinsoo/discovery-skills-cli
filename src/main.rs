mod commands;
mod config;
mod lockfile;
mod registry;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "discovery-skills")]
#[command(about = "CLI tool for managing Claude Code custom skills")]
#[command(long_about = "Discover, install, and manage custom skills for Claude Code.\n\nSkills are fetched from the discovery-skills-registry and installed\ninto ~/.claude/skills so that Claude Code can use them automatically.")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Install a skill from the registry
    Install {
        /// Skill name to install (omit to install all available skills)
        name: Option<String>,
    },
    /// Uninstall a previously installed skill
    Uninstall {
        /// Skill name to uninstall (omit to uninstall all)
        name: Option<String>,
    },
    /// List installed skills and their versions
    List,
    /// Update installed skills to the latest version
    Update {
        /// Skill name to update (omit to update all installed skills)
        name: Option<String>,
    },
}

fn main() {
    let cli = Cli::parse();

    let result = match cli.command {
        Commands::Install { name } => commands::install::run(name.as_deref()),
        Commands::Uninstall { name } => commands::uninstall::run(name.as_deref()),
        Commands::List => commands::list::run(),
        Commands::Update { name } => commands::update::run(name.as_deref()),
    };

    if let Err(e) = result {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}
