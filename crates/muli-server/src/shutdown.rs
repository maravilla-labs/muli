// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Graceful shutdown signal handling.

use anyhow::Result;
use tracing::info;

/// Wait for a shutdown signal (ctrl-c or SIGTERM).
///
/// Returns an error if signal handlers cannot be installed.
pub async fn shutdown_signal() -> Result<()> {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .map_err(|e| anyhow::anyhow!("failed to install ctrl+c handler: {e}"))
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .map_err(|e| anyhow::anyhow!("failed to install SIGTERM handler: {e}"))?
            .recv()
            .await;
        Ok::<(), anyhow::Error>(())
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<Result<()>>();

    tokio::select! {
        result = ctrl_c => {
            result?;
            info!("Received ctrl-c, shutting down");
        }
        result = terminate => {
            result?;
            info!("Received SIGTERM, shutting down");
        }
    }

    Ok(())
}
