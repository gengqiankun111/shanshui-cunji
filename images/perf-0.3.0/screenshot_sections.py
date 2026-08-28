# -*- coding: utf-8 -*-
"""把 demo 生成的 report.html 按 section 拆分成独立页面并用 Edge headless 截图（模拟逐块截图）。"""
import os
import re
import subprocess
import sys

EDGE = r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"
report_dir = sys.argv[1] if len(sys.argv) > 1 else "."
html_path = os.path.join(report_dir, "report.html")
tmp_dir = os.path.join(report_dir, "_sections")

with open(html_path, encoding="utf-8") as f:
    html = f.read()

# 提取 <head> 里的样式 + body 前缀（.wrap 前）
m = re.search(r"<style>.*?</style>", html, re.S)
css = m.group(0) if m else ""
m2 = re.search(r'<body><div class="wrap">', html)
body_prefix = m2.group(0) if m2 else '<body><div class="wrap">'
title_m = re.search(r"<title>(.*?)</title>", html, re.S)
title = title_m.group(1) if title_m else "report"
sub_m = re.search(r'<div class="sub">(.*?)</div>', html, re.S)
sub = sub_m.group(1) if sub_m else ""

os.makedirs(tmp_dir, exist_ok=True)
sections = re.findall(r'<section.*?</section>', html, re.S)
print(f"sections: {len(sections)}")

for i, sec in enumerate(sections):
    slug_m = re.search(r'id="([^"]+)"', sec)
    slug = slug_m.group(1) if slug_m else f"{i+1:02d}"
    name_m = re.search(r"<h2>(.*?)</h2>", sec, re.S)
    name = name_m.group(1).strip() if name_m else slug
    page = f"""<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="utf-8"><title>{title}</title>
{css}
<style> body {{ padding:28px 32px; }} </style>
</head><body><div class="wrap">
  <h1>{title}</h1>
  <div class="sub">{sub}</div>
  {sec}
</div></body></html>"""
    fpath = os.path.join(tmp_dir, f"{slug}.html")
    with open(fpath, "w", encoding="utf-8") as f:
        f.write(page)
    out_png = os.path.abspath(os.path.join(report_dir, f"{slug}.png"))
    subprocess.run(
        [
            EDGE, "--headless", "--disable-gpu", "--hide-scrollbars",
            "--user-data-dir=" + os.path.abspath(os.path.join(report_dir, "_edge-profile")),
            f"--window-size=900,620",
            f"--screenshot={out_png}",
            "file:///" + os.path.abspath(fpath).replace("\\", "/"),
        ],
        capture_output=True, timeout=60,
    )
    print(f"{slug}.png  {name}")

print("done")
