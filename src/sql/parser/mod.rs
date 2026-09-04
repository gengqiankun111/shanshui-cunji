//! 解析层（原 sqlish.rs 词法/AST/递归下降段）：`Lexer` → `Parser` → AST。
//!
//! - `ast.rs`：AST 定义（CmpOp/Cond/WhereExpr/HavingCond/HavingExpr/Select/JoinClause/JoinKind）；
//! - `lexer.rs`：词法（Tok/Lexer）；
//! - `parser.rs`：递归下降解析入口 `parse_select` / `parse_where_expr`。

/// 解析器内部结果（错误为人类可读 String，入口统一转 crate::Error::Config）。
type PRes<T> = std::result::Result<T, String>;

mod ast;
mod lexer;
mod parser;

pub use ast::{CmpOp, Cond, HavingCond, HavingExpr, JoinClause, JoinKind, Select, WhereExpr};
pub use parser::{parse_select, parse_where_expr};
