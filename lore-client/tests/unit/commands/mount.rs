// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use clap::Parser;
use lore_client::cli::LoreCli;
use lore_client::cli::LoreCommands;

#[test]
fn mount_requires_experimental_opt_in_explicit_revision_and_directory() {
    for args in [
        vec!["lore", "mount"],
        vec!["lore", "mount", "--revision", "main@LATEST", "/tmp/view"],
        vec!["lore", "mount", "--experimental", "/tmp/view"],
        vec![
            "lore",
            "mount",
            "--experimental",
            "--revision",
            "main@LATEST",
        ],
        vec![
            "lore",
            "mount",
            "--experimental",
            "--revision",
            "",
            "/tmp/view",
        ],
    ] {
        assert!(LoreCli::try_parse_from(&args).is_err(), "accepted {args:?}");
    }
}

#[test]
fn mount_preserves_read_only_defaults_and_requires_writable_metadata() {
    let cli = LoreCli::try_parse_from([
        "lore",
        "--repository",
        "/tmp/source",
        "--offline",
        "mount",
        "--experimental",
        "--revision",
        "main@LATEST",
        "/tmp/view",
    ])
    .unwrap();
    assert!(cli.offline);
    let Some(LoreCommands::Mount(args)) = cli.command else {
        panic!("expected mount");
    };
    assert!(args.experimental);
    assert!(args.digest_cache.is_none());
    assert_eq!(args.revision.as_deref(), Some("main@LATEST"));
    assert_eq!(args.mountpoint, std::path::PathBuf::from("/tmp/view"));
    assert!(
        LoreCli::try_parse_from([
            "lore",
            "mount",
            "--experimental",
            "--revision",
            "main@LATEST",
            "--writable",
            "/tmp/view"
        ])
        .is_err()
    );
}

#[test]
fn digest_cache_is_explicit_and_requires_a_directory_value() {
    let cli = LoreCli::try_parse_from([
        "lore",
        "mount",
        "--experimental",
        "--revision",
        "main@LATEST",
        "/tmp/view",
        "--digest-cache",
        "/tmp/digests",
    ])
    .unwrap();
    let Some(LoreCommands::Mount(args)) = cli.command else {
        panic!("expected mount");
    };
    assert_eq!(
        args.digest_cache,
        Some(std::path::PathBuf::from("/tmp/digests"))
    );
    assert!(
        LoreCli::try_parse_from([
            "lore",
            "mount",
            "--experimental",
            "--revision",
            "main@LATEST",
            "/tmp/view",
            "--digest-cache"
        ])
        .is_err()
    );
}

#[test]
fn writable_fresh_import_and_reuse_have_explicit_paths() {
    let cli = LoreCli::try_parse_from([
        "lore",
        "--offline",
        "mount",
        "--experimental",
        "--writable",
        "--source",
        "/tmp/source",
        "--revision",
        "main@LATEST",
        "--metadata",
        "/tmp/metadata",
        "--backing",
        "/tmp/new-backing",
        "/tmp/view",
    ])
    .unwrap();
    let Some(LoreCommands::Mount(args)) = cli.command else {
        panic!("expected mount")
    };
    assert!(args.writable && !args.reuse);
    assert_eq!(args.source, Some("/tmp/source".into()));
    assert_eq!(args.backing, Some("/tmp/new-backing".into()));
    for source in [vec![], vec!["--source", "/tmp/source"]] {
        let mut arguments = vec![
            "lore",
            "--offline",
            "mount",
            "--experimental",
            "--writable",
            "--reuse",
            "--metadata",
            "/tmp/metadata",
            "/tmp/view",
        ];
        arguments.extend(source);
        let cli = LoreCli::try_parse_from(arguments).unwrap();
        let Some(LoreCommands::Mount(args)) = cli.command else {
            panic!("expected mount")
        };
        assert!(args.reuse && args.revision.is_none() && args.backing.is_none());
    }
    assert!(
        LoreCli::try_parse_from([
            "lore",
            "mount",
            "--experimental",
            "--writable",
            "--reuse",
            "--metadata",
            "/tmp/metadata",
            "--revision",
            "main@LATEST",
            "/tmp/view",
        ])
        .is_err()
    );
}

#[test]
fn remote_contents_requires_writable_source_and_online_execution() {
    let base = [
        "lore",
        "mount",
        "--experimental",
        "--writable",
        "--remote-contents",
        "--source",
        "/tmp/source",
        "--revision",
        "main@LATEST",
        "--metadata",
        "/tmp/metadata",
        "--backing",
        "/tmp/backing",
        "/tmp/view",
    ];
    let cli = LoreCli::try_parse_from(base).unwrap();
    let Some(LoreCommands::Mount(args)) = cli.command else {
        panic!("expected mount")
    };
    assert!(args.remote_contents && args.writable);
    let mut offline = vec!["lore", "--offline"];
    offline.extend_from_slice(&base[1..]);
    assert!(LoreCli::try_parse_from(offline).is_err());
    let missing_source: Vec<_> = base
        .into_iter()
        .filter(|arg| *arg != "--source" && *arg != "/tmp/source")
        .collect();
    assert!(LoreCli::try_parse_from(missing_source).is_err());
    let no_writable: Vec<_> = base
        .into_iter()
        .filter(|arg| *arg != "--writable")
        .collect();
    assert!(LoreCli::try_parse_from(no_writable).is_err());
    let reuse = LoreCli::try_parse_from([
        "lore",
        "mount",
        "--experimental",
        "--writable",
        "--remote-contents",
        "--reuse",
        "--source",
        "/tmp/source",
        "--metadata",
        "/tmp/metadata",
        "/tmp/view",
    ])
    .unwrap();
    let Some(LoreCommands::Mount(args)) = reuse.command else {
        panic!("expected mount")
    };
    assert!(args.remote_contents && args.reuse && args.revision.is_none());
}
