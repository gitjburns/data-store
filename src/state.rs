use crate::{
    config::ServiceConfig,
    error::ApiError,
    inference::InferenceRuntime,
    types::{HealthComponent, HealthResponse},
};

#[derive(Debug)]
pub struct AppState {
    pub config: ServiceConfig,
    inference: Result<InferenceRuntime, ApiError>,
}

impl AppState {
    /// Build shared application state for HTTP handlers.
    pub fn new(config: ServiceConfig, inference: Result<InferenceRuntime, ApiError>) -> Self {
        Self { config, inference }
    }

    /// Return current service health and readiness diagnostics.
    pub fn health(&self) -> HealthResponse {
        let inference_component = match &self.inference {
            Ok(runtime) => HealthComponent {
                name: "inference".to_string(),
                ready: true,
                details: runtime.health_details(),
            },
            Err(error) => HealthComponent {
                name: "inference".to_string(),
                ready: false,
                details: vec![error.to_string()],
            },
        };
        let components = vec![
            inference_component,
            HealthComponent {
                name: "indexes".to_string(),
                ready: false,
                details: vec!["LanceDB index loading is not implemented yet".to_string()],
            },
        ];
        let ready = components.iter().all(|component| component.ready);

        HealthResponse {
            service: "data-store".to_string(),
            ready,
            components,
        }
    }
}
