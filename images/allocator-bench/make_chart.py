# -*- coding: utf-8 -*-
"""musl/glibc × mimalloc/系统 分配器高并发压测对比图（阿里云 Debian12, 2核/1.6GB）"""
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm

# 中文字体（Windows: Microsoft YaHei）
for f in ["Microsoft YaHei", "SimHei", "Noto Sans CJK SC"]:
    if any(fm.FontProperties(fname=p).get_name() == f for p in fm.findSystemFonts()) or f in {x.name for x in fm.fontManager.ttflist}:
        plt.rcParams["font.sans-serif"] = [f]
        break
plt.rcParams["axes.unicode_minus"] = False

labels = ["1 线程", "2 线程", "4 线程"]
data = {
    "glibc-system":   [250888, 271070, 270168],
    "glibc-mimalloc": [338170, 349044, 349914],
    "musl-system":    [52651,  69455,  30136],
    "musl-mimalloc":  [247671, 291512, 298501],
}
colors = {
    "glibc-system":   "#9aa5b1",
    "glibc-mimalloc": "#2f6fed",
    "musl-system":    "#e8a33d",
    "musl-mimalloc":  "#e23b3b",
}

fig, ax = plt.subplots(figsize=(10, 6))
x = range(len(labels))
w = 0.19
for i, (k, v) in enumerate(data.items()):
    off = (i - 1.5) * w
    bars = ax.bar([xi + off for xi in x], v, width=w, label=k, color=colors[k])
    for b, val in zip(bars, v):
        ax.text(b.get_x() + b.get_width() / 2, b.get_height() + 4000,
                f"{val/1000:.0f}k", ha="center", fontsize=8)

ax.set_xticks(list(x))
ax.set_xticklabels(labels)
ax.set_ylabel("QPS（次/秒）")
ax.set_title("山水存迹数据库 · 分配器高并发压测（阿里云 Debian12 2核）\nmimalloc vs 系统分配器 × glibc/musl", fontsize=13)
ax.legend()
ax.grid(axis="y", alpha=0.3)
ax.set_ylim(0, 420000)

# 底部注释：加速比
note = (
    "mimalloc 加速比（vs 同 libc 系统分配器）\n"
    "glibc: 1T ×1.35 / 2T ×1.29 / 4T ×1.30\n"
    "musl : 1T ×4.70 / 2T ×4.20 / 4T ×9.90（musl 默认 malloc 全局单锁，4T 反降 57%）"
)
fig.text(0.02, 0.01, note, fontsize=9, va="bottom",
         bbox=dict(boxstyle="round", facecolor="#f2f2f2", alpha=0.9))

plt.tight_layout(rect=[0, 0.12, 1, 1])
import os
out_dir = os.path.dirname(os.path.abspath(__file__))
plt.savefig(os.path.join(out_dir, "chart-musl-vs-mimalloc.png"), dpi=150)
print("chart saved")
