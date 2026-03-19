use std::io::{self, Write};

/// Prompt the user for y/n confirmation.
pub fn confirm(message: &str) -> Result<bool, Box<dyn std::error::Error>> {
    print!("{} (y/n): ", message);
    io::stdout().flush()?;

    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    let answer = input.trim().to_lowercase();
    Ok(answer == "y" || answer == "yes")
}
