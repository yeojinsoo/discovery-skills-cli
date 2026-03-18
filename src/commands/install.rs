use crate::config;
use crate::lockfile;
use crate::registry;

/// Install a skill from the registry by name.
pub fn run(name: &str) {
    println!("Installing skill: {}", name);
    // TODO: Implement in S4
    //  1. Fetch registry index
    //  2. Resolve skill metadata
    //  3. Download and extract skill archive
    //  4. Update lockfile
}
