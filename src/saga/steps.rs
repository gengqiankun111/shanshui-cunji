//! 步骤定义：ClosureStep（闭包式）、HttpStep（HTTP 业务端点）+ 极简 HTTP POST 客户端。
//!
//! 每个 SAGA 步骤 = 正向（forward）+ 反向补偿（compensate，幂等）。
//! HttpStep：正向 POST `action_url`、补偿 POST `compensate_url`（Ex-2.5 网关接入）。

use crate::error::{Error, Result};

use super::SagaStep;

/// 闭包式步骤（测试/简单场景：直接给正向与补偿闭包）。
pub struct ClosureStep {
    name: String,
    forward: Box<dyn Fn() -> Result<()> + Send + Sync>,
    compensate: Box<dyn Fn() -> Result<()> + Send + Sync>,
}

impl ClosureStep {
    pub fn new(
        name: impl Into<String>,
        forward: impl Fn() -> Result<()> + Send + Sync + 'static,
        compensate: impl Fn() -> Result<()> + Send + Sync + 'static,
    ) -> Self {
        Self {
            name: name.into(),
            forward: Box::new(forward),
            compensate: Box::new(compensate),
        }
    }
}

impl SagaStep for ClosureStep {
    fn name(&self) -> &str {
        &self.name
    }
    fn forward(&self) -> Result<()> {
        (self.forward)()
    }
    fn compensate(&self) -> Result<()> {
        (self.compensate)()
    }
}

/// 极简 HTTP/1.1 POST 客户端（Ex-2.5 网关协调器调用业务步骤端点）。
/// 返回 HTTP 状态码；2xx 视为成功，其余由调用方视为步骤失败（触发 SAGA 补偿）。
/// 阻塞式同步（协调器为串行编排；超时防业务节点悬挂拖死编排）。
pub fn http_post(url: &str, body: &[u8], timeout_ms: u64) -> Result<u16> {
    use std::io::{Read, Write};
    let rest = url.strip_prefix("http://").ok_or_else(|| {
        Error::Config(format!("SAGA 步骤 URL 需 http:// 前缀: {url}"))
    })?;
    let (hostport, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    };
    let (host, port) = match hostport.rsplit_once(':') {
        Some((h, p)) => (h, p.parse().unwrap_or(80)),
        None => (hostport, 80),
    };
    let mut s = std::net::TcpStream::connect((host, port)).map_err(Error::Io)?;
    s.set_read_timeout(Some(std::time::Duration::from_millis(timeout_ms)))
        .map_err(Error::Io)?;
    write!(
        s,
        "POST {path} HTTP/1.1\r\nHost: {host}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .map_err(Error::Io)?;
    s.write_all(body).map_err(Error::Io)?;
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).map_err(Error::Io)?;
    let text = String::from_utf8_lossy(&buf);
    Ok(text
        .split_whitespace()
        .nth(1)
        .and_then(|x| x.parse().ok())
        .unwrap_or(0))
}

/// HTTP SAGA 步骤（Ex-2.5 网关接入）：正向 POST `action_url`、补偿 POST `compensate_url`，
/// body = `payload`（业务节点实现幂等：重复调用不叠加副作用）。
/// 非 2xx / 超时 → 步骤失败（协调器对已登记分支逆序补偿；超时未登记分支屏障空转）。
pub struct HttpStep {
    name: String,
    action_url: String,
    compensate_url: String,
    payload: Vec<u8>,
    timeout_ms: u64,
}

impl HttpStep {
    pub fn new(
        name: impl Into<String>,
        action_url: impl Into<String>,
        compensate_url: impl Into<String>,
        payload: Vec<u8>,
    ) -> Self {
        Self {
            name: name.into(),
            action_url: action_url.into(),
            compensate_url: compensate_url.into(),
            payload,
            timeout_ms: 5000,
        }
    }

    /// 自定义调用超时（默认 5000ms）。
    pub fn with_timeout(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }
}

impl SagaStep for HttpStep {
    fn name(&self) -> &str {
        &self.name
    }
    fn forward(&self) -> Result<()> {
        let st = http_post(&self.action_url, &self.payload, self.timeout_ms)?;
        if (200..300).contains(&st) {
            Ok(())
        } else {
            Err(Error::Config(format!(
                "步骤 {} 正向返回非 2xx: {st}",
                self.name
            )))
        }
    }
    fn compensate(&self) -> Result<()> {
        let st = http_post(&self.compensate_url, &self.payload, self.timeout_ms)?;
        if (200..300).contains(&st) {
            Ok(())
        } else {
            Err(Error::Config(format!(
                "步骤 {} 补偿返回非 2xx: {st}",
                self.name
            )))
        }
    }
}
