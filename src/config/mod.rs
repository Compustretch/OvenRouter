mod schema;

use std::{error::Error, fs, path::Path};

pub use schema::{AppConfig, ExecutionConfig, RouterConfig};

pub fn load_from_path(path: impl AsRef<Path>) -> Result<AppConfig, Box<dyn Error>> {
    let contents = fs::read_to_string(path)?;
    parse(&contents)
}

fn parse(contents: &str) -> Result<AppConfig, Box<dyn Error>> {
    let config: AppConfig = toml::from_str(contents)?;

    // Cascade executor settings remain keyed by their historical 1..=4
    // compatibility ordinal. Decision and generative have named settings.
    for (name, tier) in [
        ("tier3", &config.router.tier3),
        ("tier4", &config.router.tier4),
    ] {
        validate_endpoint_timeout(name, tier.enabled, tier.endpoint.as_deref(), tier.timeout_ms)?;
    }
    for (name, execution) in [
        ("decision", &config.router.decision),
        ("generative", &config.router.generative),
    ] {
        validate_endpoint_timeout(name, execution.enabled, execution.endpoint.as_deref(), execution.timeout_ms)?;
        if execution.enabled && execution.model.as_deref().is_some_and(|model| model.trim().is_empty()) {
            return Err(format!("router.{name}.model must be non-empty when configured").into());
        }
    }

    Ok(config)
}

fn validate_endpoint_timeout(
    name: &str,
    enabled: bool,
    endpoint: Option<&str>,
    timeout_ms: Option<u64>,
) -> Result<(), Box<dyn Error>> {
    if !enabled {
        return Ok(());
    }
    if endpoint.is_some_and(|value| value.trim().is_empty()) {
        return Err(format!("router.{name}.endpoint must be non-empty when configured").into());
    }
    if timeout_ms == Some(0) {
        return Err(format!("router.{name}.timeout_ms must be greater than zero").into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn omitted_strategies_are_disabled() {
        let config = parse("[router.tier1]\nenabled = true\n[router.tier3]\nenabled = false\n").unwrap();
        assert!(config.router.tier1.enabled);
        assert!(!config.router.tier2.enabled);
        assert!(!config.router.tier3.enabled);
        assert!(!config.router.tier4.enabled);
        assert!(!config.router.decision.enabled);
        assert!(!config.router.generative.enabled);
    }

    #[test]
    fn parses_named_terminal_strategy_settings() {
        let config = parse(r#"
            [router.decision]
            enabled = true
            endpoint = "http://127.0.0.1:11435/api/decide"
            model = "laya:en"
            timeout_ms = 90000

            [router.generative]
            enabled = true
            endpoint = "http://127.0.0.1:11434/api/generate"
            model = "gemma3:latest"
            timeout_ms = 120000
        "#).unwrap();

        assert_eq!(config.router.decision.model.as_deref(), Some("laya:en"));
        assert_eq!(config.router.generative.model.as_deref(), Some("gemma3:latest"));
        assert_eq!(config.router.decision.timeout_ms, Some(90000));
    }

    #[test]
    fn rejects_legacy_downstream_tier_numbering() {
        assert!(parse("[router.tier5]\nenabled = true\n").is_err());
        assert!(parse("[router.tier6]\nenabled = true\n").is_err());
    }

    #[test]
    fn rejects_blank_named_endpoint_and_zero_timeout() {
        assert!(parse("[router.decision]\nenabled=true\nendpoint=\" \"\n").is_err());
        assert!(parse("[router.generative]\nenabled=true\ntimeout_ms=0\n").is_err());
    }
}
