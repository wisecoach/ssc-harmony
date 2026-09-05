#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
把 docs/tools/hindsight/manifest.json 里的文档摘要+关系写入 hindsight bank。

用法:
    python3 docs/tools/hindsight/ingest.py                 # 默认写 harmony-sscc（async）
    python3 docs/tools/hindsight/ingest.py --dry-run       # 只打印将提交的 items，不写
    python3 docs/tools/hindsight/ingest.py --sync          # 同步（需 bank LLM 可用）
    python3 docs/tools/hindsight/ingest.py --bank papers    # 其它 bank
    BASE 环境变量可覆盖数据面地址（默认 http://localhost:8888）

manifest 字段: id, section, status, path, title, summary, relations[], tags[]
"""
import argparse, json, os, sys, urllib.request, urllib.error

BASE = os.environ.get("BASE", "http://localhost:8888")
DEFAULT_BANK = "harmony-sscc"
HERE = os.path.dirname(os.path.abspath(__file__))
MANIFEST = os.path.join(HERE, "manifest.json")

def build_items(entries):
    items = []
    for e in entries:
        rel = "；".join(e["relations"]) if e.get("relations") else "—"
        content = (
            f"【{e['id']}】{e['title']}\n"
            f"摘要：{e['summary']}\n"
            f"与其他文档的关系：{rel}\n"
            f"仓库路径：{e['path']}  状态：{e['status']}  目录：{e['section']}"
        )
        items.append({
            "content": content,
            "document_id": e["id"],
            "tags": list(dict.fromkeys(e.get("tags", []) + [e["section"], "doc"])),
            "metadata": {"path": e["path"], "status": e["status"], "section": e["section"], "kind": "doc-index"},
            "timestamp": "unset",   # 静态参考材料，无时间属性
        })
    return items

def post(url, payload):
    req = urllib.request.Request(url, data=json.dumps(payload).encode("utf-8"),
                                 headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=120) as r:
        return json.loads(r.read().decode("utf-8"))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--sync", action="store_true")
    ap.add_argument("--bank", default=DEFAULT_BANK)
    ap.add_argument("--manifest", default=MANIFEST)
    a = ap.parse_args()
    entries = json.load(open(a.manifest, encoding="utf-8"))
    items = build_items(entries)
    print(f"manifest entries: {len(entries)}  -> retain items: {len(items)}  (sync={a.sync})")
    if a.dry_run:
        for it in items:
            print("-" * 60)
            print(it["content"])
        return 0
    url = f"{BASE}/v1/default/banks/{a.bank}/memories"
    payload = {"async": not a.sync, "items": items}
    try:
        resp = post(url, payload)
        print(json.dumps(resp, ensure_ascii=False, indent=2))
    except urllib.error.HTTPError as e:
        print("HTTP", e.code, e.read().decode("utf-8", "replace"), file=sys.stderr)
        return 1
    return 0

if __name__ == "__main__":
    sys.exit(main())
