use std::net::SocketAddr;

#[tokio::main]
async fn main() {
    let app = match aion_api::app_from_env() {
        Ok(app) => app,
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(1);
        }
    };
    let addr = SocketAddr::from(([0, 0, 0, 0], 8080));
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("failed to bind aion-api listener");

    axum::serve(listener, app)
        .await
        .expect("aion-api server failed");
}
