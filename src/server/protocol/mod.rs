//! MySQL wire 协议编解码 / 握手认证 / 响应构造（reconstruct.md server/protocol/ 规划）。
//! 内容拆分自原 src/db_adapter.rs；各子模块项聚合到本层，再由 server/mod.rs 汇总。

pub(crate) mod handshake;
pub(crate) mod packet;
pub(crate) mod response;

pub(crate) use handshake::*;
pub(crate) use packet::*;
pub(crate) use response::*;
