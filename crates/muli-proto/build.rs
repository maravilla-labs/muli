// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Protobuf compilation build script.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    for proto in [
        "../../proto/muli/v1/common.proto",
        "../../proto/muli/v1/job.proto",
        "../../proto/muli/v1/log.proto",
        "../../proto/muli/v1/agent.proto",
        "../../proto/muli/v1/health.proto",
        "../../proto/muli/v1/registry.proto",
        "../../proto/muli/v1/git.proto",
        "../../proto/muli/v1/user.proto",
        "../../proto/muli/v1/org.proto",
        "../../proto/muli/v1/tenant.proto",
        "../../proto/muli/v1/pipeline.proto",
    ] {
        println!("cargo:rerun-if-changed={proto}");
    }

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &[
                "../../proto/muli/v1/common.proto",
                "../../proto/muli/v1/job.proto",
                "../../proto/muli/v1/log.proto",
                "../../proto/muli/v1/agent.proto",
                "../../proto/muli/v1/health.proto",
                "../../proto/muli/v1/registry.proto",
                "../../proto/muli/v1/git.proto",
                "../../proto/muli/v1/user.proto",
                "../../proto/muli/v1/org.proto",
                "../../proto/muli/v1/tenant.proto",
                "../../proto/muli/v1/pipeline.proto",
            ],
            &["../../proto"],
        )?;
    Ok(())
}
