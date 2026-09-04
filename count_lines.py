#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""count_lines.py — 统计指定文件夹内代码文件的行数构成（代码/注释/空行）。

用法：
  python count_lines.py <folder> [--ignore-file PATH] [--show-files N]

- 递归扫描 <folder>；默认忽略规则文件为 <folder>/.ignore（不存在则只忽略 .git 等）；
- 也可用 --ignore-file 指定其它规则文件；规则格式兼容 .gitignore 常用子集：
  * 空行与 # 开头为注释；每行一条规则
  * 支持 * ? [..] 通配（fnmatch）；含 / 的规则按仓库相对根匹配，
    不含 / 的规则匹配任意层级的同名文件/目录
  * 目录匹配即整棵子树忽略
- 只统计能识别注释语法的代码文件；按扩展名识别（Rust/C 系/Python/Shell/SQL/Toml 等，
  见 DIALECT 表）；其余文件不纳入（--show-files 展示明细）。
输出：总文件/总行/代码/注释/空行 + 注释占比，并按扩展名分表。
"""

import argparse
import fnmatch
import os
import sys

# 注释方言：ext -> (行注释前缀元组, 块注释对元组)
_DASH = (["//"], [("/*", "*/")])
DIALECT = {
    ".rs": _DASH,
    ".c": _DASH, ".h": _DASH, ".cpp": _DASH, ".cc": _DASH, ".cxx": _DASH,
    ".hpp": _DASH, ".hh": _DASH, ".java": _DASH, ".js": _DASH, ".jsx": _DASH,
    ".ts": _DASH, ".tsx": _DASH, ".cs": _DASH, ".swift": _DASH, ".kt": _DASH,
    ".kts": _DASH, ".go": _DASH, ".php": _DASH, ".scala": _DASH, ".m": _DASH,
    ".mm": _DASH,
    ".py": (["#"], []),
    ".pyw": (["#"], []),
    ".sh": (["#"], []), ".bash": (["#"], []), ".zsh": (["#"], []),
    ".rb": (["#"], []), ".pl": (["#"], []),
    ".toml": (["#"], []),
    ".yaml": (["#"], []), ".yml": (["#"], []),
    ".ini": (["#", ";"], []),
    ".cfg": (["#", ";"], []),
    ".sql": (["--"], [("/*", "*/")]),
    ".lua": (["--"], [("--[[", "]]")]),
}
# 忽略模式（.ignore 缺省自带）
DEFAULT_IGNORE = {".git", ".svn", ".hg", "__pycache__", "node_modules",
                  "target", "build", "dist", ".idea", ".vscode", "vendor"}


def load_ignore_rules(path):
    rules = []
    if not path or not os.path.exists(path):
        return rules
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("!"):  # 反选规则：简化实现不支持，跳过
                continue
            rules.append(line.rstrip("/"))
    return rules


def ignored_rel(rules, rel, is_dir):
    """rel 为仓库相对路径（POSIX 风格）。"""
    base = rel.rsplit("/", 1)[-1]
    for r in rules:
        if "/" in r:
            if fnmatch.fnmatch(rel, r) or fnmatch.fnmatch(rel, r + "/*"):
                return True
        else:
            if fnmatch.fnmatch(base, r):
                return True
    return False


def count_file(path, line_toks, block_pairs):
    """返回 (代码, 注释, 空行)。状态机处理行注释与块注释。"""
    code = comment = blank = 0
    in_block = None  # (结束符)
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            for raw in f:
                line = raw.rstrip("\n").rstrip("\r")
                if line.startswith("\ufeff"):
                    line = line[1:]
                s = line.strip()
                if in_block is not None:
                    # 块注释中：先找本行结束标记
                    end = in_block
                    comment += 1
                    idx = line.find(end)
                    if idx >= 0:
                        in_block = None
                        tail = line[idx + len(end):].strip()
                        # 结束标记后若还有行注释/下一块起始，本行已计 comment，
                        # 简化：不继续拆 tail（极少出现，容忍误差）
                    continue
                if not s:
                    blank += 1
                    continue
                lead = len(line) - len(s)  # 前导空白宽度：注释起点 ≤ lead 视为行首起
                # 块注释起始（取最早出现的）
                blk_start = None
                for beg, end in block_pairs:
                    i = line.find(beg)
                    if i >= 0 and (blk_start is None or i < blk_start[0]):
                        blk_start = (i, end)
                # 行注释起始
                ln_start = None
                for tok in line_toks:
                    i = line.find(tok)
                    if i >= 0 and (ln_start is None or i < ln_start):
                        ln_start = i
                cand = []
                if blk_start is not None:
                    cand.append(("b", blk_start[0]))
                if ln_start is not None:
                    cand.append(("l", ln_start))
                if not cand:
                    code += 1
                    continue
                kind, i = min(cand, key=lambda x: x[1])
                if i <= lead:
                    # 整行以注释开头（仅前导空白）
                    if kind == "l":
                        comment += 1
                    else:
                        end = blk_start[1]
                        comment += 1
                        # blk_start[0] 为块注释起始位置（int）；同行闭合则 end 在其后。
                        if line.find(end, blk_start[0]) < 0:
                            in_block = end
                        # 同行闭合则不进入块
                    continue
                # 注释前有代码 → 代码行（含行尾注释 / 行首代码后起块）
                code += 1
                if kind == "b":
                    end = blk_start[1]
                    # i = 块注释起始位置；从其后找结束标记（同行闭合则不进入块）。
                    if line.find(end, i) < 0:
                        in_block = end
    except OSError as e:
        print(f"  [跳过] 读取失败 {path}: {e}", file=sys.stderr)
        return 0, 0, 0
    return code, comment, blank


def main():
    ap = argparse.ArgumentParser(description="统计代码/注释/空行")
    ap.add_argument("folder", help="要统计的文件夹")
    ap.add_argument("--ignore-file", default=None,
                    help="忽略规则文件（默认 <folder>/.ignore）")
    ap.add_argument("--show-files", type=int, default=0,
                    help="打印行数最多的前 N 个文件明细")
    args = ap.parse_args()

    root = os.path.abspath(args.folder)
    if not os.path.isdir(root):
        print(f"目录不存在: {root}")
        return 1
    ignore_path = args.ignore_file or os.path.join(root, ".ignore")
    rules = DEFAULT_IGNORE | set(load_ignore_rules(ignore_path))
    if os.path.exists(ignore_path):
        print(f"[忽略规则] {ignore_path} ({len(rules)} 条，含默认)")
    else:
        print(f"[忽略规则] 未找到 {ignore_path}，使用默认 {len(rules)} 条")

    per_ext = {}          # ext -> [files, code, comment, blank]
    file_rows = []        # (代码行数降序用)
    total_files = 0
    for dirpath, dirnames, filenames in os.walk(root):
        rel_dir = os.path.relpath(dirpath, root).replace("\\", "/")
        if rel_dir == ".":
            rel_dir = ""
        # 剪枝忽略目录
        keep = []
        for d in sorted(dirnames):
            rel = f"{rel_dir}/{d}" if rel_dir else d
            if not ignored_rel(rules, rel, True) and d not in rules:
                keep.append(d)
            elif d not in DEFAULT_IGNORE:
                pass
        dirnames[:] = keep
        for fn in sorted(filenames):
            rel = f"{rel_dir}/{fn}" if rel_dir else fn
            if ignored_rel(rules, rel, False):
                continue
            ext = os.path.splitext(fn)[1].lower()
            if ext not in DIALECT:
                continue
            path = os.path.join(dirpath, fn)
            line_toks, block_pairs = DIALECT[ext]
            c, cm, b = count_file(path, line_toks, block_pairs)
            if c == 0 and cm == 0 and b == 0:
                continue  # 空文件不计
            total_files += 1
            e = per_ext.setdefault(ext, [0, 0, 0, 0])
            e[0] += 1
            e[1] += c
            e[2] += cm
            e[3] += b
            file_rows.append((c + cm + b, rel, ext, c, cm, b))

    if not per_ext:
        print("没有找到可统计的代码文件。")
        return 0

    tot_code = sum(e[1] for e in per_ext.values())
    tot_cm = sum(e[2] for e in per_ext.values())
    tot_b = sum(e[3] for e in per_ext.values())
    tot = tot_code + tot_cm + tot_b

    def line(n):
        return f"{n:,}"

    print("\n========== 汇总 ==========")
    print(f"{'文件数':<8}{'总行数':>12}{'代码行':>12}{'注释行':>12}{'空行':>10}   注释占比")
    print(f"{total_files:<8}{line(tot):>12}{line(tot_code):>12}{line(tot_cm):>12}"
          f"{line(tot_b):>10}   {tot_cm / tot * 100:.1f}%" if tot else "")

    print("\n========== 按扩展名 ==========")
    print(f"{'扩展名':<8}{'文件':>6}{'总行':>11}{'代码':>11}{'注释':>11}{'空行':>9}   注释%")
    for ext in sorted(per_ext):
        nf, cc, cm, b = per_ext[ext]
        tl = cc + cm + b
        pct = cm / tl * 100 if tl else 0
        print(f"{ext:<8}{nf:>6}{line(tl):>11}{line(cc):>11}{line(cm):>11}"
              f"{line(b):>9}   {pct:.1f}%")

    if args.show_files > 0:
        print(f"\n========== 行数前 {args.show_files} 个文件 ==========")
        for _, rel, ext, c, cm, b in sorted(file_rows, reverse=True)[:args.show_files]:
            print(f"{c + cm + b:>10,}  {rel}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
