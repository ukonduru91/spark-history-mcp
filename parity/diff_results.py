#!/usr/bin/env python3
"""Compare two capture directories produced by run_cases.py.

Compares the semantic JSON of every tool result, prompt render and catalogue
entry, and reports the differences.

Usage: diff_results.py <dir_a> <dir_b> [--json report.json]
"""

import json
import sys
from pathlib import Path

VOLATILE_HEADER_PREFIXES = ("HTTP response headers:",)


def parse_text(text):
    """Return parsed JSON when the text is JSON, else the raw string."""
    stripped = text.strip()
    if stripped[:1] in "[{":
        try:
            return json.loads(stripped)
        except json.JSONDecodeError:
            return text
    return text


def normalise_content(content):
    out = []
    for item in content or []:
        if item.get("type") != "text":
            out.append(item)
            continue
        out.append(parse_text(item.get("text", "")))
    return out


def normalise_error(text):
    """Drop the volatile header line from a generated-client error message."""
    lines = [
        line
        for line in text.splitlines()
        if not line.startswith(VOLATILE_HEADER_PREFIXES)
    ]
    return "\n".join(lines)


def walk_diff(path, left, right, out, limit=40):
    if len(out) >= limit:
        return
    if isinstance(left, dict) and isinstance(right, dict):
        for key in sorted(set(left) | set(right)):
            if key not in left:
                out.append(f"{path}.{key}: missing on the left (right={short(right[key])})")
            elif key not in right:
                out.append(f"{path}.{key}: missing on the right (left={short(left[key])})")
            else:
                walk_diff(f"{path}.{key}", left[key], right[key], out, limit)
        return
    if isinstance(left, list) and isinstance(right, list):
        if len(left) != len(right):
            out.append(f"{path}: list length {len(left)} vs {len(right)}")
            return
        for index, (a, b) in enumerate(zip(left, right)):
            walk_diff(f"{path}[{index}]", a, b, out, limit)
        return
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        if not isinstance(left, bool) and not isinstance(right, bool):
            if float(left) == float(right):
                return
    if left != right:
        out.append(f"{path}: {short(left)} != {short(right)}")


def short(value):
    text = json.dumps(value) if not isinstance(value, str) else value
    text = text.replace("\n", "\\n")
    return text if len(text) <= 160 else text[:157] + "..."


def compare_case(name, left, right):
    diffs = []

    if bool(left.get("isError")) != bool(right.get("isError")):
        diffs.append(
            f"isError: {bool(left.get('isError'))} != {bool(right.get('isError'))}"
        )

    if left.get("isError") and right.get("isError"):
        left_text = normalise_error(
            "".join(item.get("text", "") for item in left.get("content") or [])
        )
        right_text = normalise_error(
            "".join(item.get("text", "") for item in right.get("content") or [])
        )
        if left_text != right_text:
            diffs.append(f"error text: {short(left_text)} != {short(right_text)}")
    else:
        walk_diff(
            "content",
            normalise_content(left.get("content")),
            normalise_content(right.get("content")),
            diffs,
        )

    walk_diff(
        "structuredContent",
        left.get("structuredContent"),
        right.get("structuredContent"),
        diffs,
    )

    if "exception" in left or "exception" in right:
        if left.get("exception") != right.get("exception"):
            diffs.append(
                f"exception: {short(left.get('exception'))} != {short(right.get('exception'))}"
            )

    return diffs


def compare_prompt(name, left, right):
    diffs = []
    if left.get("description") != right.get("description"):
        diffs.append("description differs")
    left_msgs = [
        (m.get("role"), (m.get("content") or {}).get("text"))
        for m in left.get("messages") or []
    ]
    right_msgs = [
        (m.get("role"), (m.get("content") or {}).get("text"))
        for m in right.get("messages") or []
    ]
    if left_msgs != right_msgs:
        if len(left_msgs) != len(right_msgs):
            diffs.append(f"message count {len(left_msgs)} vs {len(right_msgs)}")
        else:
            for index, (a, b) in enumerate(zip(left_msgs, right_msgs)):
                if a[0] != b[0]:
                    diffs.append(f"messages[{index}].role: {a[0]} != {b[0]}")
                if a[1] != b[1]:
                    diffs.append(f"messages[{index}].text differs")
    return diffs


def main() -> None:
    dir_a = Path(sys.argv[1])
    dir_b = Path(sys.argv[2])
    json_out = None
    if "--json" in sys.argv:
        json_out = Path(sys.argv[sys.argv.index("--json") + 1])

    report = {"tools": {}, "prompts": {}, "catalogue": {}}
    total = 0
    failed = 0

    # Catalogue comparison
    for catalogue in ("_tools.json", "_prompts.json"):
        left = json.loads((dir_a / catalogue).read_text())
        right = json.loads((dir_b / catalogue).read_text())
        if catalogue == "_tools.json":
            # The dataflint_* tools are an extension of the TypeScript port with
            # no Python counterpart; parity covers the upstream tools only.
            left = [t for t in left if not t["name"].startswith("dataflint_")]
            right = [t for t in right if not t["name"].startswith("dataflint_")]
        diffs = []
        walk_diff(catalogue, left, right, diffs, limit=60)
        report["catalogue"][catalogue] = diffs
        total += 1
        if diffs:
            failed += 1

    for path in sorted(dir_a.glob("tool__*.json")):
        name = path.name
        other = dir_b / name
        total += 1
        if not other.exists():
            report["tools"][name] = ["missing in second capture"]
            failed += 1
            continue
        diffs = compare_case(
            name, json.loads(path.read_text()), json.loads(other.read_text())
        )
        report["tools"][name] = diffs
        if diffs:
            failed += 1

    for path in sorted(dir_a.glob("prompt__*.json")):
        name = path.name
        other = dir_b / name
        total += 1
        if not other.exists():
            report["prompts"][name] = ["missing in second capture"]
            failed += 1
            continue
        diffs = compare_prompt(
            name, json.loads(path.read_text()), json.loads(other.read_text())
        )
        report["prompts"][name] = diffs
        if diffs:
            failed += 1

    print(f"compared {total} items; {total - failed} identical, {failed} differing\n")
    for section, entries in report.items():
        for name, diffs in entries.items():
            if diffs:
                print(f"### {section}/{name}")
                for diff in diffs[:20]:
                    print(f"    {diff}")
                if len(diffs) > 20:
                    print(f"    ... and {len(diffs) - 20} more")
                print()

    if json_out:
        json_out.write_text(json.dumps(report, indent=2))

    sys.exit(0)


if __name__ == "__main__":
    main()
