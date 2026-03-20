mod init;
mod pull;
mod push;
mod status;

use crate::SyncAction;

pub fn run(action: SyncAction) -> Result<(), Box<dyn std::error::Error>> {
    match action {
        SyncAction::Init { bucket, region } => init::run(bucket.as_deref(), region.as_deref()),
        SyncAction::Push {
            force,
            quiet,
            changed_only,
        } => push::run(force, quiet, changed_only),
        SyncAction::Pull { force, quiet } => pull::run(force, quiet),
        SyncAction::Status => status::run(),
    }
}
