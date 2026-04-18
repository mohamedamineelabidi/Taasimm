"""Validate 03_zone_mapping.ipynb: JSON structure + AST syntax of every code cell."""
import ast
import json
import sys
from pathlib import Path

NB_PATH = Path(__file__).resolve().parent.parent / "notebooks" / "03_zone_mapping.ipynb"

raw = NB_PATH.read_text(encoding="utf-8")
try:
    nb = json.loads(raw)
except json.JSONDecodeError as e:
    print(f"INVALID JSON: {e}")
    sys.exit(1)

print(f"nbformat      : {nb['nbformat']}.{nb['nbformat_minor']}")
cells = nb["cells"]
print(f"Total cells   : {len(cells)}")

md_count = sum(1 for c in cells if c["cell_type"] == "markdown")
code_count = sum(1 for c in cells if c["cell_type"] == "code")
print(f"Markdown      : {md_count}")
print(f"Code          : {code_count}")
print()

errors = []
for i, cell in enumerate(cells):
    ctype = cell["cell_type"]
    src = "".join(cell["source"]) if isinstance(cell["source"], list) else cell["source"]
    if ctype == "code":
        try:
            ast.parse(src)
            print(f"  Cell {i+1:02d} [code]     OK  ({len(src.splitlines())} lines)")
        except SyntaxError as e:
            msg = f"Cell {i+1}: SyntaxError at line {e.lineno}: {e.msg}"
            errors.append(msg)
            print(f"  Cell {i+1:02d} [code]     ERROR  {e.msg} (line {e.lineno})")
    else:
        print(f"  Cell {i+1:02d} [markdown] OK  ({len(src.splitlines())} lines)")

print()
if errors:
    print(f"FAILED — {len(errors)} syntax error(s):")
    for e in errors:
        print(" ", e)
    sys.exit(1)
else:
    print(f"PASSED — all {code_count} code cells are syntactically valid.")
