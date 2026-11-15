use std::time::{Duration, Instant};

use serde_json::Value;
use tracing::{info, warn};

use crate::pipeline::tier::Strategy;

use super::decision_runner::DecisionRunner;
use super::downstream::DownstreamError;
use super::generative_runner::GenerativeRunner;

/// Provider identity is deliberately independent of the execution strategy.
/// A strategy describes the operation; a provider names the service executing it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Provider {
    Ollaya,
    Ollama,
}

impl Provider {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ollaya => "ollaya",
            Self::Ollama => "ollama",
        }
    }
}

/// One ordered dispatch attempt. Disabled and rejected attempts are recorded too;
/// model_id is absent only when no concrete provider/model could be resolved.
#[derive(Debug, Clone)]
pub struct DispatchAttempt {
    pub strategy: Strategy,
    pub provider: Option<Provider>,
    pub model_id: Option<String>,
    pub outcome: String,
    pub latency: Duration,
    pub failure_reason: Option<String>,
}

/// The terminal outcome delivered to the caller. On success this identifies the
/// provider/model that produced the payload. On failure it identifies the final
/// strategy/provider that failed to produce one.
#[derive(Debug, Clone)]
pub struct FinalResult {
    pub strategy: Strategy,
    pub provider: Option<Provider>,
    pub model_id: Option<String>,
    pub status: &'static str,
    pub failure_reason: Option<String>,
}

#[derive(Debug)]
pub struct DispatchOutcome {
    pub payload: Value,
    pub attempts: Vec<DispatchAttempt>,
    pub final_result: FinalResult,
    pub request_latency: Duration,
}

#[derive(Debug)]
pub struct DispatchFailure {
    pub error: DownstreamError,
    pub attempts: Vec<DispatchAttempt>,
    pub final_result: FinalResult,
    pub request_latency: Duration,
}

pub struct DownstreamDispatcher {
    decision: Option<DecisionRunner>,
    generative: Option<GenerativeRunner>,
    decision_model: String,
    generative_model: String,
}

impl DownstreamDispatcher {
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_enabled(
        decision_endpoint: String,
        decision_model: String,
        generative_endpoint: String,
        generative_model: String,
        decision_timeout: Duration,
        generative_timeout: Duration,
        breaker_threshold: usize,
        breaker_window: Duration,
        breaker_open_duration: Duration,
        decision_enabled: bool,
        generative_enabled: bool,
    ) -> Result<Self, reqwest::Error> {
        Ok(Self {
            decision: if decision_enabled {
                Some(DecisionRunner::new(
                    decision_endpoint,
                    decision_model.clone(),
                    decision_timeout,
                    breaker_threshold,
                    breaker_window,
                    breaker_open_duration,
                )?)
            } else {
                None
            },
            generative: if generative_enabled {
                Some(GenerativeRunner::new(
                    generative_endpoint,
                    generative_model.clone(),
                    generative_timeout,
                )?)
            } else {
                None
            },
            decision_model,
            generative_model,
        })
    }

    pub async fn execute(
        &self,
        request_id: &str,
        strategy: Strategy,
        prompt: &str,
        state: Option<&Value>,
        questions: Option<&serde_json::Map<String, Value>>,
    ) -> Result<DispatchOutcome, DispatchFailure> {
        let started = Instant::now();
        let mut attempts = Vec::with_capacity(2);

        match strategy {
            Strategy::Decision => {
                let decision = match self.decision.as_ref() {
                    Some(decision) => decision,
                    None => {
                        attempts.push(DispatchAttempt {
                            strategy: Strategy::Decision,
                            provider: Some(Provider::Ollaya),
                            model_id: Some(self.decision_model.clone()),
                            outcome: "disabled".into(),
                            latency: Duration::ZERO,
                            failure_reason: Some("decision_disabled".into()),
                        });
                        return self.invoke_generative(
                            request_id, prompt, attempts, started,
                        ).await;
                    }
                };

                let decision_result = match (state, questions) {
                    (Some(state), Some(questions)) => decision.invoke(request_id, state, questions).await,
                    _ => Err(DownstreamError::InvalidResponse(
                        "typed decision request requires both state and questions".into(),
                    )),
                };

                match decision_result {
                    Ok((payload, elapsed)) => {
                        attempts.push(DispatchAttempt {
                            strategy: Strategy::Decision,
                            provider: Some(Provider::Ollaya),
                            model_id: Some(self.decision_model.clone()),
                            outcome: "success".into(),
                            latency: elapsed,
                            failure_reason: None,
                        });
                        log_attempt(request_id, Strategy::Decision, Provider::Ollaya, "success", elapsed);
                        let final_result = FinalResult {
                            strategy: Strategy::Decision,
                            provider: Some(Provider::Ollaya),
                            model_id: Some(self.decision_model.clone()),
                            status: "success",
                            failure_reason: None,
                        };
                        Ok(DispatchOutcome {
                            payload,
                            attempts,
                            final_result,
                            request_latency: started.elapsed(),
                        })
                    }
                    Err(error) => {
                        let (outcome, reason) = classify_decision_error(&error);
                        let elapsed = match &error {
                            DownstreamError::Disabled => Duration::ZERO,
                            _ => started.elapsed(),
                        };
                        attempts.push(DispatchAttempt {
                            strategy: Strategy::Decision,
                            provider: Some(Provider::Ollaya),
                            model_id: Some(self.decision_model.clone()),
                            outcome: outcome.into(),
                            latency: elapsed,
                            failure_reason: Some(reason.into()),
                        });
                        log_attempt(request_id, Strategy::Decision, Provider::Ollaya, outcome, elapsed);
                        warn!(
                            request_id,
                            strategy = "decision",
                            provider = "ollaya",
                            reason_code = reason,
                            error = %error,
                            "decision attempt did not produce an accepted result; trying generative"
                        );
                        self.invoke_generative(request_id, prompt, attempts, started).await
                    }
                }
            }
            Strategy::Generative => {
                self.invoke_generative(request_id, prompt, attempts, started).await
            }
            Strategy::Rules | Strategy::FastText | Strategy::Encoder => {
                let error = DownstreamError::InvalidResponse(format!(
                    "strategy {} is a cascade strategy and cannot be dispatched as a terminal executor",
                    strategy.as_str()
                ));
                Err(DispatchFailure {
                    error,
                    attempts,
                    final_result: FinalResult {
                        strategy,
                        provider: None,
                        model_id: None,
                        status: "failure",
                        failure_reason: Some("non_terminal_strategy".into()),
                    },
                    request_latency: started.elapsed(),
                })
            }
        }
    }

    async fn invoke_generative(
        &self,
        request_id: &str,
        prompt: &str,
        mut attempts: Vec<DispatchAttempt>,
        started: Instant,
    ) -> Result<DispatchOutcome, DispatchFailure> {
        let Some(generative) = self.generative.as_ref() else {
            let reason = "generative_disabled";
            attempts.push(DispatchAttempt {
                strategy: Strategy::Generative,
                provider: Some(Provider::Ollama),
                model_id: Some(self.generative_model.clone()),
                outcome: "disabled".into(),
                latency: Duration::ZERO,
                failure_reason: Some(reason.into()),
            });
            return Err(DispatchFailure {
                error: DownstreamError::Disabled,
                attempts,
                final_result: FinalResult {
                    strategy: Strategy::Generative,
                    provider: Some(Provider::Ollama),
                    model_id: Some(self.generative_model.clone()),
                    status: "failure",
                    failure_reason: Some(reason.into()),
                },
                request_latency: started.elapsed(),
            });
        };

        match generative.invoke(request_id, prompt).await {
            Ok((payload, elapsed)) => {
                attempts.push(DispatchAttempt {
                    strategy: Strategy::Generative,
                    provider: Some(Provider::Ollama),
                    model_id: Some(self.generative_model.clone()),
                    outcome: "success".into(),
                    latency: elapsed,
                    failure_reason: None,
                });
                log_attempt(request_id, Strategy::Generative, Provider::Ollama, "success", elapsed);
                Ok(DispatchOutcome {
                    payload,
                    attempts,
                    final_result: FinalResult {
                        strategy: Strategy::Generative,
                        provider: Some(Provider::Ollama),
                        model_id: Some(self.generative_model.clone()),
                        status: "success",
                        failure_reason: None,
                    },
                    request_latency: started.elapsed(),
                })
            }
            Err(error) => {
                let (outcome, reason) = classify_generative_error(&error);
                let elapsed = started.elapsed();
                attempts.push(DispatchAttempt {
                    strategy: Strategy::Generative,
                    provider: Some(Provider::Ollama),
                    model_id: Some(self.generative_model.clone()),
                    outcome: outcome.into(),
                    latency: elapsed,
                    failure_reason: Some(reason.into()),
                });
                log_attempt(request_id, Strategy::Generative, Provider::Ollama, outcome, elapsed);
                warn!(
                    request_id,
                    strategy = "generative",
                    provider = "ollama",
                    reason_code = reason,
                    error = %error,
                    "generative attempt failed"
                );
                Err(DispatchFailure {
                    error,
                    attempts,
                    final_result: FinalResult {
                        strategy: Strategy::Generative,
                        provider: Some(Provider::Ollama),
                        model_id: Some(self.generative_model.clone()),
                        status: "failure",
                        failure_reason: Some(reason.into()),
                    },
                    request_latency: started.elapsed(),
                })
            }
        }
    }
}

fn classify_decision_error(error: &DownstreamError) -> (&'static str, &'static str) {
    match error {
        DownstreamError::Timeout => ("timeout", "timeout"),
        DownstreamError::NotEligible => ("rejected", "decision_not_eligible"),
        DownstreamError::CircuitOpen => ("connection_error", "circuit_open"),
        DownstreamError::Transport(_) => ("connection_error", "transport_error"),
        DownstreamError::HttpStatus(status, _) => {
            if *status == 503 { ("connection_error", "upstream_503") }
            else { ("connection_error", "upstream_http_error") }
        }
        DownstreamError::InvalidResponse(_) => ("invalid_response", "invalid_response"),
        DownstreamError::Disabled => ("disabled", "decision_disabled"),
    }
}

fn classify_generative_error(error: &DownstreamError) -> (&'static str, &'static str) {
    match error {
        DownstreamError::Timeout => ("timeout", "timeout"),
        DownstreamError::CircuitOpen => ("connection_error", "circuit_open"),
        DownstreamError::Transport(_) => ("connection_error", "transport_error"),
        DownstreamError::HttpStatus(status, _) => {
            if *status == 503 { ("connection_error", "upstream_503") }
            else { ("connection_error", "upstream_http_error") }
        }
        DownstreamError::InvalidResponse(_) => ("invalid_response", "invalid_response"),
        DownstreamError::NotEligible => ("rejected", "unexpected_not_eligible"),
        DownstreamError::Disabled => ("disabled", "generative_disabled"),
    }
}

fn log_attempt(
    request_id: &str,
    strategy: Strategy,
    provider: Provider,
    outcome: &str,
    elapsed: Duration,
) {
    info!(
        request_id,
        strategy = strategy.as_str(),
        provider = provider.as_str(),
        outcome,
        invocation_latency_ms = elapsed.as_secs_f64() * 1000.0,
        "dispatch attempt"
    );
}

#[cfg(test)]
mod tests {
    use super::{DownstreamDispatcher, Provider};
    use crate::pipeline::tier::Strategy;
    use serde_json::Value;
    use std::time::Duration;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    async fn stub(status: &'static str, body: &str, delay: Duration) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let body = body.to_string();
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut request = [0u8; 4096];
                let _ = stream.read(&mut request).await;
                tokio::time::sleep(delay).await;
                let response = format!(
                    "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes()).await;
            }
        });
        format!("http://{address}/")
    }

    async fn stub_capture_request_id(
        body: &'static str,
    ) -> (String, tokio::sync::oneshot::Receiver<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut request = [0u8; 8192];
                let n = stream.read(&mut request).await.unwrap_or(0);
                let text = String::from_utf8_lossy(&request[..n]).to_ascii_lowercase();
                let request_id = text.lines()
                    .find_map(|line| line.strip_prefix("x-request-id: "))
                    .unwrap_or("")
                    .trim().to_string();
                let _ = tx.send(request_id);
                let response = format!(
                    "HTTP/1.1 200 OK\\r\\ncontent-type: application/json\\r\\ncontent-length: {}\\r\\nconnection: close\\r\\n\\r\\n{body}",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes()).await;
            }
        });
        (format!("http://{address}/"), rx)
    }

    fn dispatcher(decision: String, generative: String, timeout: Duration) -> DownstreamDispatcher {
        DownstreamDispatcher::new_with_enabled(
            decision,
            "laya:en".into(),
            generative,
            "gemma3:latest".into(),
            timeout,
            timeout,
            2,
            Duration::from_secs(10),
            Duration::from_secs(30),
            true,
            true,
        ).unwrap()
    }

    fn decision_request() -> (Value, serde_json::Map<String, Value>) {
        (
            Value::String("Can Jev answer this?".into()),
            serde_json::from_value(serde_json::json!({
                "answer": {
                    "type": "choice",
                    "criteria": {"yes": "yes", "no": "no"}
                }
            })).unwrap(),
        )
    }

    #[tokio::test]
    async fn decision_success_is_final_result() {
        let decision = stub(
            "200 OK",
            r#"{"answers":{"answer":{"type":"choice","choice":"yes","confidence":0.94},"__router_jev_eligible":{"type":"choice","choice":"yes","confidence":0.95}}}"#,
            Duration::ZERO,
        ).await;
        let generative = stub("200 OK", r#"{"response":"should not run"}"#, Duration::ZERO).await;
        let router = dispatcher(decision, generative, Duration::from_millis(500));
        let (state, questions) = decision_request();
        let result = router.execute("test-decision-success", Strategy::Decision, "prompt", Some(&state), Some(&questions)).await.unwrap();

        assert_eq!(result.final_result.strategy, Strategy::Decision);
        assert_eq!(result.final_result.provider, Some(Provider::Ollaya));
        assert_eq!(result.final_result.model_id.as_deref(), Some("laya:en"));
        assert_eq!(result.final_result.status, "success");
        assert_eq!(result.attempts.len(), 1);
        assert_eq!(result.attempts[0].outcome, "success");
        assert!(result.payload["answers"].get("__router_jev_eligible").is_none());
    }

    #[tokio::test]
    async fn decision_ineligibility_falls_back_and_records_both_attempts() {
        let decision = stub(
            "200 OK",
            r#"{"answers":{"answer":{"type":"choice","choice":"other","confidence":0.8},"__router_jev_eligible":{"type":"choice","choice":"no","confidence":0.99}}}"#,
            Duration::ZERO,
        ).await;
        let generative = stub("200 OK", r#"{"response":"frontier"}"#, Duration::ZERO).await;
        let router = dispatcher(decision, generative, Duration::from_millis(500));
        let (state, questions) = decision_request();
        let result = router.execute("test-ineligible", Strategy::Decision, "prompt", Some(&state), Some(&questions)).await.unwrap();

        assert_eq!(result.attempts.len(), 2);
        assert_eq!(result.attempts[0].strategy, Strategy::Decision);
        assert_eq!(result.attempts[0].outcome, "rejected");
        assert_eq!(result.attempts[0].failure_reason.as_deref(), Some("decision_not_eligible"));
        assert_eq!(result.attempts[1].strategy, Strategy::Generative);
        assert_eq!(result.attempts[1].provider, Some(Provider::Ollama));
        assert_eq!(result.final_result.strategy, Strategy::Generative);
        assert_eq!(result.final_result.status, "success");
        assert!(result.final_result.failure_reason.is_none());
    }

    #[tokio::test]
    async fn decision_transport_failure_falls_back_to_generative() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let generative = stub("200 OK", r#"{"response":"frontier"}"#, Duration::ZERO).await;
        let router = dispatcher(format!("http://{address}/"), generative, Duration::from_millis(300));
        let (state, questions) = decision_request();

        let result = router.execute("test-transport", Strategy::Decision, "prompt", Some(&state), Some(&questions)).await.unwrap();
        assert_eq!(result.attempts[0].outcome, "connection_error");
        assert_eq!(result.attempts[1].strategy, Strategy::Generative);
        assert_eq!(result.final_result.strategy, Strategy::Generative);
    }

    #[tokio::test]
    async fn decision_timeout_falls_back_to_generative() {
        let decision = stub(
            "200 OK",
            r#"{"answers":{"answer":{"type":"choice","choice":"yes","confidence":0.9},"__router_jev_eligible":{"choice":"yes","confidence":0.95}}}"#,
            Duration::from_millis(200),
        ).await;
        let generative = stub("200 OK", r#"{"response":"frontier"}"#, Duration::ZERO).await;
        let router = dispatcher(decision, generative, Duration::from_millis(50));
        let (state, questions) = decision_request();

        let result = router.execute("test-timeout", Strategy::Decision, "prompt", Some(&state), Some(&questions)).await.unwrap();
        assert_eq!(result.attempts[0].outcome, "timeout");
        assert_eq!(result.final_result.strategy, Strategy::Generative);
        assert_eq!(result.final_result.status, "success");
    }

    #[tokio::test]
    async fn generative_failure_is_an_explicit_final_failure() {
        let decision = stub("200 OK", "{}", Duration::ZERO).await;
        let generative = stub("503 Service Unavailable", r#"{"error":"offline"}"#, Duration::ZERO).await;
        let router = dispatcher(decision, generative, Duration::from_millis(300));

        let result = router.execute("test-gen-failure", Strategy::Generative, "prompt", None, None).await;
        let error = result.unwrap_err();
        assert_eq!(error.attempts.len(), 1);
        assert_eq!(error.attempts[0].strategy, Strategy::Generative);
        assert_eq!(error.final_result.strategy, Strategy::Generative);
        assert_eq!(error.final_result.provider, Some(Provider::Ollama));
        assert_eq!(error.final_result.status, "failure");
    }

    #[tokio::test]
    async fn disabled_generative_is_explicit() {
        let router = DownstreamDispatcher::new_with_enabled(
            "http://127.0.0.1:1".into(),
            "laya:en".into(),
            "http://127.0.0.1:1".into(),
            "gemma3:latest".into(),
            Duration::from_millis(100),
            Duration::from_millis(100),
            2,
            Duration::from_secs(10),
            Duration::from_secs(30),
            false,
            false,
        ).unwrap();
        let result = router.execute("disabled", Strategy::Generative, "prompt", None, None).await;
        let error = result.unwrap_err();
        assert_eq!(error.attempts[0].outcome, "disabled");
        assert_eq!(error.final_result.failure_reason.as_deref(), Some("generative_disabled"));
    }
    #[tokio::test]
    async fn request_id_is_identical_on_decision_and_generative_fallback() {
        let (decision, decision_id) = stub_capture_request_id(
            r#"{"answers":{"answer":{"choice":"other","confidence":0.8},"__router_jev_eligible":{"choice":"no","confidence":0.99}}}"#
        ).await;
        let (generative, generative_id) = stub_capture_request_id(r#"{"response":"fallback"}"#).await;
        let router = dispatcher(decision, generative, Duration::from_millis(500));
        let (state, questions) = decision_request();
        let request_id = "123e4567-e89b-42d3-a456-426614174000";
        let result = router.execute(request_id, Strategy::Decision, "prompt", Some(&state), Some(&questions))
            .await.unwrap();
        assert_eq!(result.attempts.len(), 2);
        assert_eq!(result.attempts[0].strategy, Strategy::Decision);
        assert_eq!(result.attempts[1].strategy, Strategy::Generative);
        assert_eq!(decision_id.await.unwrap(), request_id);
        assert_eq!(generative_id.await.unwrap(), request_id);
    }

}
