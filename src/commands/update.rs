use crate::config;
use crate::lockfile;
use crate::registry;

/// Update installed skills. If a name is provided, update only that skill;
/// otherwise update all installed skills.
pub fn run(name: Option<&str>) {
    match name {
        Some(n) => println!("Updating skill: {}", n),
        None => println!("Updating all skills..."),
    }
    // TODO: Implement in S7
    //  1. Read lockfile for installed skills
    //  2. Fetch latest versions from registry
    //  3. Re-install if newer version available
    //  4. Update lockfile
}
