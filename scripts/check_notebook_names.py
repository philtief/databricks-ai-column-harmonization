#!/usr/bin/env python3
"""Fail on undefined names in notebooks, allowing Databricks globals and whatever `%run ./_shared_utils` defines."""

import ast
import subprocess
import sys

SHARED = "notebooks/_shared_utils.py"
names = {"dbutils", "spark", "display", "displayHTML", "sc"}
source = "\n".join(line for line in open(SHARED).read().splitlines() if not line.startswith(("# MAGIC", "%")))
for node in ast.walk(ast.parse(source)):
    if isinstance(node, ast.Import | ast.ImportFrom):
        names |= {(alias.asname or alias.name).split(".")[0] for alias in node.names}
    elif isinstance(node, ast.FunctionDef | ast.ClassDef):
        names.add(node.name)
    elif isinstance(node, ast.Assign):
        names |= {target.id for target in node.targets if isinstance(target, ast.Name)}

out = subprocess.run(
    [sys.executable, "-m", "ruff", "check", "notebooks", "examples", "--select", "F821", "--isolated", "--output-format", "concise"],
    capture_output=True,
    text=True,
).stdout
undefined = [line for line in out.splitlines() if "F821" in line and line.split("`")[1] not in names]
print("\n".join(undefined) or "notebook names ok")
sys.exit(1 if undefined else 0)
