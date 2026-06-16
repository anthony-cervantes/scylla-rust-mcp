const RELEASE_WORKFLOW: &str = include_str!("../.github/workflows/release.yml");

#[test]
fn release_workflow_does_not_use_third_party_actions_in_privileged_release_path() {
    assert!(
        !RELEASE_WORKFLOW.contains("dtolnay/rust-toolchain"),
        "install Rust with rustup so publish jobs do not execute a mutable third-party action"
    );
    assert!(
        !RELEASE_WORKFLOW.contains("softprops/action-gh-release"),
        "create releases with gh so contents:write is not delegated to a mutable third-party action"
    );
}

#[test]
fn crates_io_token_is_not_job_scoped() {
    let publish_job = RELEASE_WORKFLOW
        .split("  publish-crate:")
        .nth(1)
        .and_then(|rest| rest.split("  create-release:").next())
        .expect("publish-crate job should exist");

    assert!(
        !publish_job.contains("\n    env:\n      CARGO_REGISTRY_TOKEN:"),
        "CARGO_REGISTRY_TOKEN should be scoped to the exact steps that need it"
    );
    assert_eq!(
        publish_job.matches("CARGO_REGISTRY_TOKEN:").count(),
        2,
        "token should appear only on the token check and publish steps"
    );
}

#[test]
fn contents_write_is_scoped_to_github_release_job() {
    assert!(
        RELEASE_WORKFLOW.contains("permissions:\n  contents: read"),
        "default workflow permission should be read-only"
    );

    let create_release_job = RELEASE_WORKFLOW
        .split("  create-release:")
        .nth(1)
        .expect("create-release job should exist");
    assert!(
        create_release_job.contains("permissions:\n      contents: write"),
        "only the GitHub Release job should request contents:write"
    );
}
