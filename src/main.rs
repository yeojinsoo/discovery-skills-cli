mod commands;
mod config;
mod deps;
mod jsonl_merge;
mod lockfile;
mod registry;
mod s3_client;
mod sync_config;
mod sync_manifest;
mod ui;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "discovery-skills")]
#[command(about = "CLI tool for managing Claude Code custom skills")]
#[command(long_about = "Discover, install, and manage custom skills for Claude Code.\n\nSkills are fetched from the discovery-skills-registry and installed\ninto ~/.claude/skills so that Claude Code can use them automatically.\nCLI state (lockfile) is stored in ~/.discovery-skills/.")]
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
    /// Sync skills to/from cloud storage
    Sync {
        #[command(subcommand)]
        action: SyncAction,
    },
}

#[derive(Subcommand)]
enum SyncAction {
    /// Initialize sync configuration for this machine
    Init {
        /// S3 bucket name for skill synchronisation
        #[arg(long)]
        bucket: Option<String>,
        /// AWS region of the bucket (e.g. ap-northeast-2)
        #[arg(long)]
        region: Option<String>,
    },
    /// Push local skills to cloud storage
    Push {
        /// Overwrite remote changes without conflict check
        #[arg(long)]
        force: bool,
        /// Suppress output, exit code only
        #[arg(long)]
        quiet: bool,
        /// Only push changed files (default behaviour)
        #[arg(long)]
        changed_only: bool,
    },
    /// Pull skills from cloud storage to local
    Pull {
        /// Overwrite local changes without conflict resolution
        #[arg(long)]
        force: bool,
        /// Suppress output, exit code only
        #[arg(long)]
        quiet: bool,
    },
    /// Show sync status (local vs remote diff)
    Status,
}

/// Migrate the lockfile from the legacy location (~/.claude/skills/.skill-manager.toml)
/// to the new location (~/.discovery-skills/lockfile.toml).
fn migrate_lockfile() {
    let legacy = match config::legacy_lockfile_path() {
        Ok(p) => p,
        Err(_) => return,
    };
    let new_path = match config::lockfile_path() {
        Ok(p) => p,
        Err(_) => return,
    };

    if legacy.exists() && !new_path.exists() {
        if let Some(parent) = new_path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        match std::fs::rename(&legacy, &new_path) {
            Ok(()) => {
                eprintln!(
                    "  Lockfile migrated: {} → {}",
                    legacy.display(),
                    new_path.display()
                );
            }
            Err(e) => {
                eprintln!(
                    "  Warning: lockfile migration failed ({}), falling back to copy",
                    e
                );
                if let Ok(content) = std::fs::read(&legacy) {
                    if std::fs::write(&new_path, &content).is_ok() {
                        let _ = std::fs::remove_file(&legacy);
                        eprintln!(
                            "  Lockfile migrated (copy): {} → {}",
                            legacy.display(),
                            new_path.display()
                        );
                    }
                }
            }
        }
    }
}

fn main() {
    let cli = Cli::parse();

    migrate_lockfile();

    let result = match cli.command {
        Commands::Install { name } => commands::install::run(name.as_deref()),
        Commands::Uninstall { name } => commands::uninstall::run(name.as_deref()),
        Commands::List => commands::list::run(),
        Commands::Update { name } => commands::update::run(name.as_deref()),
        Commands::Sync { action } => commands::sync::run(action),
    };

    if let Err(e) = result {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}
