use clap::{Command, CommandFactory};
use clap_complete::{generate_to, shells};
use clap_mangen::Man;
use ratash::cli::Cli;
use std::collections::BTreeMap;
use std::env;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let check = match env::args().nth(1).as_deref() {
        None => false,
        Some("--check") if env::args().len() == 2 => true,
        _ => return Err("usage: cargo run --example generate-release-assets [--check]".into()),
    };
    let committed = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("packaging/generated");
    if check {
        let generated = TemporaryDirectory::new()?;
        generate_assets(&generated.path)?;
        verify_assets(&committed, &generated.path)?;
    } else {
        generate_assets(&committed)?;
    }
    Ok(())
}

fn generate_assets(root: &Path) -> Result<(), io::Error> {
    let completions = root.join("completions");
    let man_pages = root.join("man/man1");
    fs::create_dir_all(&completions)?;
    fs::create_dir_all(&man_pages)?;
    generate_to(shells::Bash, &mut Cli::command(), "ratash", &completions)?;
    generate_to(shells::Zsh, &mut Cli::command(), "ratash", &completions)?;
    let fish = generate_to(shells::Fish, &mut Cli::command(), "ratash", &completions)?;
    writeln!(
        OpenOptions::new().append(true).open(fish)?,
        "complete -c ratash -n \"__fish_ratash_using_subcommand help\" -f -a \"agent\" -d \"Stable operation guidance for AI Agents and scripts\""
    )?;
    generate_man_pages(Cli::command(), &man_pages)?;
    Ok(())
}

fn generate_man_pages(mut command: Command, directory: &Path) -> Result<(), io::Error> {
    command.build();
    for subcommand in command
        .get_subcommands()
        .filter(|value| !value.is_hide_set())
    {
        generate_man_pages(subcommand.clone(), directory)?;
    }
    let title = command
        .get_display_name()
        .unwrap_or_else(|| command.get_name())
        .to_ascii_uppercase();
    let path = Man::new(command)
        .title(title)
        .date("2026-08-01")
        .source(format!("Ratash {}", env!("CARGO_PKG_VERSION")))
        .manual("Ratash User Commands")
        .generate_to(directory)?;
    let content = fs::read_to_string(&path)?;
    let normalized = content
        .lines()
        .map(str::trim_end)
        .collect::<Vec<_>>()
        .join("\n")
        .replace(".br\n\n.br\n", ".sp\n");
    fs::write(path, format!("{normalized}\n"))
}

fn verify_assets(committed: &Path, generated: &Path) -> Result<(), io::Error> {
    let committed = read_tree(committed)?;
    let generated = read_tree(generated)?;
    if committed == generated {
        Ok(())
    } else {
        Err(io::Error::other("generated release assets are stale"))
    }
}

fn read_tree(root: &Path) -> Result<BTreeMap<PathBuf, Vec<u8>>, io::Error> {
    fn visit(
        root: &Path,
        directory: &Path,
        files: &mut BTreeMap<PathBuf, Vec<u8>>,
    ) -> Result<(), io::Error> {
        for entry in fs::read_dir(directory)? {
            let path = entry?.path();
            if path.is_dir() {
                visit(root, &path, files)?;
            } else {
                let relative = path
                    .strip_prefix(root)
                    .map_err(|_| io::Error::other("generated asset escaped its root"))?;
                files.insert(relative.to_owned(), fs::read(path)?);
            }
        }
        Ok(())
    }

    let mut files = BTreeMap::new();
    visit(root, root, &mut files)?;
    Ok(files)
}

struct TemporaryDirectory {
    path: PathBuf,
}

impl TemporaryDirectory {
    fn new() -> Result<Self, io::Error> {
        let path = env::temp_dir().join(format!("ratash-release-assets-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&path)?;
        Ok(Self { path })
    }
}

impl Drop for TemporaryDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}
