use crate::config;
use crate::lockfile;

/// Uninstall a previously installed skill by name.
pub fn run(name: &str) {
    println!("Uninstalling skill: {}", name);
    // TODO: Implement in S5
    //  1. Check lockfile for installed skill
    //  2. Remove skill directory
    //  3. Update lockfile
}
