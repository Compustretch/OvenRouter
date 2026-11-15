#[derive(Debug)]
pub enum DownstreamError {
    Disabled,
    CircuitOpen,
    Timeout,
    NotEligible,
    Transport(String),
    HttpStatus(u16, String),
    InvalidResponse(String),
}

impl std::fmt::Display for DownstreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Disabled => write!(f, "downstream decision strategy is disabled by configuration"),
            Self::CircuitOpen => write!(f, "circuit breaker is open"),
            Self::Timeout => write!(f, "downstream request timed out"),
            Self::NotEligible => write!(f, "Ollaya rejected the decision as ineligible"),
            Self::Transport(error) => write!(f, "downstream transport error: {error}"),
            Self::HttpStatus(status, body) => {
                write!(f, "downstream returned HTTP {status}: {body}")
            }
            Self::InvalidResponse(error) => write!(f, "invalid downstream JSON response: {error}"),
        }
    }
}
