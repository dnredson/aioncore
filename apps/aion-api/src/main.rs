use std::net::SocketAddr;

#[tokio::main]
async fn main() {
    let (state, diagnostics) = match aion_api::AppState::from_env_with_diagnostics() {
        Ok(value) => value,
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(1);
        }
    };

    let app = aion_api::app_with_state(state.clone());

    eprintln!(
        "startup storage backend={}, database_url_provided={}, migrations_applied={}, auth_mode={}, auth_enforced={}, auth_dev_bypass={}",
        diagnostics.storage_backend.as_str(),
        diagnostics.database_url_provided,
        diagnostics
            .migrations_applied
            .map(|value| value.to_string())
            .unwrap_or_else(|| "n/a".to_string()),
        diagnostics.auth_mode.as_str(),
        diagnostics.auth_enforced,
        diagnostics.auth_dev_bypass
    );

    match diagnostics.auth_mode {
        aion_api::AuthMode::Dev => {
            eprintln!("warning: authentication is not enforced; development-mode bypass is active");
        }
        aion_api::AuthMode::Disabled => {
            eprintln!("warning: authentication is explicitly disabled for this runtime");
        }
        aion_api::AuthMode::Token => {}
    }

    if let Err(error) = aion_api::start_connector_workers_if_enabled(state.clone()).await {
        eprintln!("{error}");
        std::process::exit(1);
    }

    if let Err(error) = aion_api::start_mqtt_ingest_if_enabled(state).await {
        eprintln!("{error}");
        std::process::exit(1);
    }

    let addr = SocketAddr::from(([0, 0, 0, 0], 8080));
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("failed to bind aion-api listener");

    axum::serve(listener, app)
        .await
        .expect("aion-api server failed");
}
