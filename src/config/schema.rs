use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct AppConfig {
    #[serde(default)]
    pub router: RouterConfig,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouterConfig {
    #[serde(default)]
    pub tier1: TierConfig,
    #[serde(default)]
    pub tier2: TierConfig,
    #[serde(default)]
    pub tier3: TierConfig,
    #[serde(default)]
    pub tier4: TierConfig,
    /// Terminal typed-decision execution through Ollaya.
    #[serde(default)]
    pub decision: ExecutionConfig,
    /// Terminal generative execution through Ollama.
    #[serde(default)]
    pub generative: ExecutionConfig,
}

impl RouterConfig {
    /// Settings for the four historical cascade ordinals only.
    pub fn legacy_tier(&self, ordinal: u8) -> Option<&TierConfig> {
        match ordinal {
            1 => Some(&self.tier1),
            2 => Some(&self.tier2),
            3 => Some(&self.tier3),
            4 => Some(&self.tier4),
            _ => None,
        }
    }
}

#[derive(Debug, Default, Deserialize)]
pub struct TierConfig {
    #[serde(default)]
    pub enabled: bool,
    /// gRPC scorer endpoint for encoder tiers.
    #[serde(default)]
    pub endpoint: Option<String>,
    /// Per-call timeout cap in milliseconds for gRPC scorer tiers.
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

/// Configuration for a terminal execution strategy. Provider and model identity
/// are deployment settings; they are deliberately separate from Strategy.
#[derive(Debug, Default, Deserialize)]
pub struct ExecutionConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}
