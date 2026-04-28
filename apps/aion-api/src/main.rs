use std::net::SocketAddr;

#[tokio::main]
async fn main() {
    let (app, diagnostics) = match aion_api::app_from_env_with_diagnostics() {
        Ok(value) => value,
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(1);
        }
    };

    eprintln!(
        "startup storage backend={}, database_url_provided={}, migrations_applied={}",
        diagnostics.storage_backend.as_str(),
        diagnostics.database_url_provided,
        diagnostics
            .migrations_applied
            .map(|value| value.to_string())
            .unwrap_or_else(|| "n/a".to_string())
    );

    let addr = SocketAddr::from(([0, 0, 0, 0], 8080));
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("failed to bind aion-api listener");

    axum::serve(listener, app)
        .await
        .expect("aion-api server failed");
}
