#!/usr/bin/env python3
"""Collect third-party license notices for the rabbitmq-bridge binary.

Usage: scripts/third-party-licenses.py [TARGET_TRIPLE] > THIRD-PARTY-LICENSES.txt

Lists every crate linked into rabbitmq-bridge (normal dependencies for the
given target) with its declared license and the text of its LICENSE*/COPYING*/
NOTICE* files from the Cargo registry checkout. Exits non-zero when a crate
ships no license file and declares no license, so releases cannot silently
omit notices.
"""
import json
import pathlib
import subprocess
import sys

target = sys.argv[1] if len(sys.argv) > 1 else None
tree_cmd = ["cargo", "tree", "--locked", "-p", "rabbitmq-bridge", "-e", "normal",
            "--prefix", "none", "--format", "{p}"]
if target:
    tree_cmd += ["--target", target]
tree = subprocess.run(tree_cmd, check=True, capture_output=True, text=True).stdout

wanted = set()
for line in tree.splitlines():
    parts = line.split()
    if len(parts) >= 2 and parts[1].startswith("v") and "(" not in line.split(" v", 1)[0]:
        if len(parts) > 2 and parts[2].startswith("(/"):
            continue  # workspace path crate, covered by our own licenses
        wanted.add((parts[0], parts[1][1:]))

meta = json.loads(subprocess.run(
    ["cargo", "metadata", "--locked", "--format-version", "1"],
    check=True, capture_output=True, text=True).stdout)

prefixes = ("license", "licence", "copying", "notice", "copyright", "unlicense")
missing = []
out = sys.stdout
out.write("Third-party software linked into rabbitmq-bridge\n")
out.write("=" * 48 + "\n\n")
for pkg in sorted(meta["packages"], key=lambda p: (p["name"], p["version"])):
    if pkg["source"] is None or (pkg["name"], pkg["version"]) not in wanted:
        continue
    root = pathlib.Path(pkg["manifest_path"]).parent
    files = sorted(f for f in root.iterdir()
                   if f.is_file() and f.name.lower().startswith(prefixes))
    out.write("-" * 78 + "\n")
    out.write(f"{pkg['name']} {pkg['version']}\n")
    out.write(f"License: {pkg.get('license') or 'see files'}\n")
    if pkg.get("repository"):
        out.write(f"Source: {pkg['repository']}\n")
    out.write("\n")
    if not files:
        if not pkg.get("license"):
            missing.append(f"{pkg['name']} {pkg['version']}")
        out.write("(No license file shipped in the crate; see declared license above.)\n\n")
    for f in files:
        out.write(f"--- {f.name} ---\n")
        out.write(f.read_text(errors="replace").rstrip() + "\n\n")

sys.stderr.write(f"collected notices for {len(wanted)} crates\n")
if missing:
    sys.stderr.write("crates with no license information: " + ", ".join(missing) + "\n")
    sys.exit(1)
