"""
Alteryx-to-Python Conversion Planner

Reads an Alteryx workflow (.yxmd), analyzes the data flow graph,
identifies dead ends and redundancies, and produces an optimized
Python (Polars) conversion plan.
"""

import argparse
import xml.etree.ElementTree as ET
from pathlib import Path
from collections import defaultdict
from catalog import parse_workflow, auto_describe, _extract_tool_name, _extract_sql


def _build_graph(wf: dict) -> dict:
    """Build adjacency lists (forward and reverse) from connections."""
    forward = defaultdict(list)   # tool_id -> [downstream tool_ids]
    reverse = defaultdict(list)   # tool_id -> [upstream tool_ids]

    for conn in wf["connections"]:
        forward[conn["from_id"]].append(conn["to_id"])
        reverse[conn["to_id"]].append(conn["from_id"])

    return {"forward": dict(forward), "reverse": dict(reverse)}


def _find_reachable_from_outputs(wf: dict, graph: dict) -> set:
    """Walk backwards from all output nodes to find every tool that
    contributes to a final output. Anything NOT in this set is a dead end."""
    output_ids = {out["tool_id"] for out in wf["outputs"]}
    visited = set()
    stack = list(output_ids)

    while stack:
        node = stack.pop()
        if node in visited:
            continue
        visited.add(node)
        for parent in graph["reverse"].get(node, []):
            stack.append(parent)

    return visited


def _find_dead_ends(wf: dict, graph: dict, reachable: set) -> list:
    """Find tools that don't contribute to any output."""
    dead = []
    for tool_id, detail in wf["node_details"].items():
        if tool_id not in reachable and detail["tool_name"] != "Browse":
            dead.append({
                "tool_id": tool_id,
                "tool_name": detail["tool_name"],
                "annotation": detail["annotation"],
            })
    return dead


def _find_rename_chains(wf: dict) -> list:
    """Detect fields that get renamed multiple times across different tools."""
    # Track all formula/select operations that rename fields
    renames = []
    for f in wf["formulas"]:
        if f["field"] and f["expression"]:
            # A formula like [new_name] = [old_name] is effectively a rename
            expr = f["expression"].strip()
            if expr.startswith("[") and expr.endswith("]") and expr.count("[") == 1:
                renames.append({
                    "from": expr[1:-1],
                    "to": f["field"],
                    "type": "formula rename",
                })

    # Build rename chains: A -> B -> C means just do A -> C
    chains = []
    rename_map = {r["from"]: r["to"] for r in renames}
    visited = set()
    for start_field in rename_map:
        if start_field in visited:
            continue
        chain = [start_field]
        current = start_field
        while current in rename_map:
            current = rename_map[current]
            chain.append(current)
            visited.add(current)
        if len(chain) > 2:
            chains.append({"chain": chain, "suggestion": f"Rename '{chain[0]}' directly to '{chain[-1]}'"})

    return chains


def _find_unused_formulas(wf: dict, graph: dict, reachable: set) -> list:
    """Find formulas in dead-end branches or that produce fields not used downstream."""
    unused = []
    for f in wf["formulas"]:
        # Simple heuristic: if the field name contains "temp", "tmp", "test", or starts with _
        field = f["field"].lower()
        if any(hint in field for hint in ("temp", "tmp", "test", "unused", "delete")):
            unused.append({
                "field": f["field"],
                "expression": f["expression"],
                "reason": "Field name suggests temporary/test field",
            })
    return unused


def _detect_optimization_opportunities(wf: dict, graph: dict) -> list:
    """Detect patterns that can be simplified in Python."""
    opportunities = []

    # 1. Filter after Join — could push filter before join for performance
    for flt in wf["filters"]:
        flt_id = flt["tool_id"]
        for parent_id in graph["reverse"].get(flt_id, []):
            parent = wf["node_details"].get(parent_id, {})
            if parent.get("tool_name") == "Join":
                opportunities.append({
                    "type": "Filter After Join",
                    "detail": f"Filter '{flt['annotation'] or flt['expression']}' runs after a Join. "
                              f"In Python, apply the filter to the input DataFrame before joining for better performance.",
                    "priority": "medium",
                })

    # 2. Multiple sequential Joins — could be a single multi-join
    join_ids = [j["tool_id"] for j in wf["joins"]]
    for jid in join_ids:
        downstream = graph["forward"].get(jid, [])
        for ds in downstream:
            if ds in join_ids:
                opportunities.append({
                    "type": "Sequential Joins",
                    "detail": "Multiple joins chained together. In Python, consider joining all reference tables "
                              "in sequence on a single DataFrame instead of creating intermediate frames.",
                    "priority": "low",
                })
                break

    # 3. Union followed by dedup — might be replaceable with a single concat + unique
    for tool_id, detail in wf["node_details"].items():
        if detail["tool_name"] == "Union":
            downstream = graph["forward"].get(tool_id, [])
            for ds in downstream:
                ds_detail = wf["node_details"].get(ds, {})
                if ds_detail.get("tool_name") in ("Unique", "Sort"):
                    opportunities.append({
                        "type": "Union + Dedup",
                        "detail": "Union followed by Unique/Sort. In Python, use pl.concat() then .unique() in one step.",
                        "priority": "low",
                    })

    # 4. Sort before output — Polars is lazy, sort only if output requires it
    for out in wf["outputs"]:
        out_id = out["tool_id"]
        for parent_id in graph["reverse"].get(out_id, []):
            parent = wf["node_details"].get(parent_id, {})
            if parent.get("tool_name") == "Sort":
                opportunities.append({
                    "type": "Sort Before Output",
                    "detail": "Sort tool right before output. If writing to a database, the sort may be unnecessary. "
                              "If the output order matters, keep it but do it as .sort() at the end.",
                    "priority": "low",
                })

    # 5. Summarize + CrossTab — might be a single group_by + pivot in Polars
    tool_names = [d["tool_name"] for d in wf["node_details"].values()]
    if "Summarize" in tool_names and "Cross Tab" in tool_names:
        opportunities.append({
            "type": "Summarize + CrossTab",
            "detail": "Summarize followed by Cross Tab. In Polars, this is a single "
                      "df.group_by().agg() followed by .pivot() or done in one step.",
            "priority": "medium",
        })

    return opportunities


def _generate_python_plan(wf: dict, graph: dict, reachable: set,
                          dead_ends: list, optimizations: list) -> list:
    """Generate an ordered list of Python conversion steps."""
    steps = []
    step_num = [0]

    def add_step(phase, description, code_hint="", notes=""):
        step_num[0] += 1
        steps.append({
            "step": step_num[0],
            "phase": phase,
            "description": description,
            "code_hint": code_hint,
            "notes": notes,
        })

    # --- Phase 1: Inputs ---
    for inp in wf["inputs"]:
        if inp["tool_id"] not in reachable:
            continue
        label = inp["annotation"] or inp.get("source_short", "data source")
        source = inp["source"]
        sql = inp.get("sql", "")

        if "odbc" in source.lower():
            dsn = inp.get("source_short", "database")
            if sql:
                code = f'df = pl.read_database("""\n{sql}\n""", connection)'
            else:
                code = f'df = pl.read_database("SELECT * FROM table", connection)'
            add_step("Input", f"Connect to {dsn} and run query", code, f"Source: {source}")
        elif source.lower().endswith(".csv"):
            filename = inp.get("source_short", "file.csv")
            code = f'df = pl.read_csv("{filename}")'
            add_step("Input", f"Read {label}", code)
        elif source.lower().endswith((".xlsx", ".xls")):
            filename = inp.get("source_short", "file.xlsx")
            code = f'df = pl.read_excel("{filename}")'
            add_step("Input", f"Read {label}", code)
        elif source.lower().endswith((".parquet", ".pq")):
            filename = inp.get("source_short", "file.parquet")
            code = f'df = pl.scan_parquet("{filename}")'
            add_step("Input", f"Read {label}", code)
        else:
            add_step("Input", f"Read {label}", f"# Load from: {source}")

    # --- Phase 2: Filters (push early) ---
    for flt in wf["filters"]:
        if flt["tool_id"] not in reachable:
            continue
        label = flt["annotation"] or "Apply filter"
        expr = flt["expression"]
        code = f'df = df.filter(pl.col("field") == "value")  # Alteryx: {expr}'
        add_step("Filter", label, code, f"Original expression: {expr}")

    # --- Phase 3: Joins ---
    for j in wf["joins"]:
        if j["tool_id"] not in reachable:
            continue
        fields = ", ".join(j["fields"])
        label = j["annotation"] or f"Join on {fields}"
        code = f'df = df.join(df_right, on=["{fields}"], how="left")'
        add_step("Join", label, code, f"Join key(s): {fields}")

    # --- Phase 4: Formulas / Computed columns ---
    active_formulas = []
    for f in wf["formulas"]:
        # Skip formulas that look temporary
        if f["field"].lower().startswith(("temp", "tmp", "_", "test")):
            continue
        active_formulas.append(f)

    if active_formulas:
        fields = [f["field"] for f in active_formulas]
        exprs = []
        for f in active_formulas:
            exprs.append(f'    # {f["field"]} = {f["expression"]}')
        code = "df = df.with_columns([\n" + "\n".join(
            f'    pl.lit(None).alias("{f["field"]}"),  # TODO: convert {f["expression"]}'
            for f in active_formulas
        ) + "\n])"
        notes = "\n".join(f'  {f["field"]}: {f["expression"]}' for f in active_formulas)
        add_step("Transform", f"Calculate {', '.join(fields[:5])}" + (f" (+{len(fields)-5} more)" if len(fields) > 5 else ""),
                 code, f"Formulas to convert:\n{notes}")

    # --- Phase 5: Aggregations ---
    if wf["summarizes"]:
        agg_exprs = []
        for s in wf["summarizes"]:
            action = s["action"].lower()
            field = s["field"]
            rename = s["rename"] or field
            if action == "sum":
                agg_exprs.append(f'    pl.col("{field}").sum().alias("{rename}"),')
            elif action == "count":
                agg_exprs.append(f'    pl.col("{field}").count().alias("{rename}"),')
            elif action == "min":
                agg_exprs.append(f'    pl.col("{field}").min().alias("{rename}"),')
            elif action == "max":
                agg_exprs.append(f'    pl.col("{field}").max().alias("{rename}"),')
            elif action == "avg" or action == "average":
                agg_exprs.append(f'    pl.col("{field}").mean().alias("{rename}"),')
            elif action == "first":
                agg_exprs.append(f'    pl.col("{field}").first().alias("{rename}"),')
            else:
                agg_exprs.append(f'    pl.col("{field}").{action}().alias("{rename}"),  # TODO: verify')
        code = "df_summary = df.group_by([\"group_col\"]).agg([\n" + "\n".join(agg_exprs) + "\n])"
        add_step("Aggregate", "Summarize data", code, "TODO: identify the group_by columns from the workflow context")

    # --- Phase 6: Outputs ---
    for out in wf["outputs"]:
        if out["tool_id"] not in reachable:
            continue
        label = out["annotation"] or out.get("dest_short", "destination")
        dest = out["destination"]
        out_type = out.get("type", "file")

        if out_type == "database":
            code = f'# Write to database\n# df.write_database("table_name", connection)'
            add_step("Output", f"Write to {label}", code, f"Destination: {dest}")
        elif out_type == "CSV file":
            filename = out.get("dest_short", "output.csv")
            code = f'df.write_csv("{filename}")'
            add_step("Output", f"Export to {label}", code)
        elif out_type == "Excel file":
            filename = out.get("dest_short", "output.xlsx")
            code = f'df.write_excel("{filename}")'
            add_step("Output", f"Export to {label}", code)
        else:
            code = f'df.write_parquet("output.parquet")'
            add_step("Output", f"Write to {label}", code, f"Destination: {dest}")

    return steps


def analyze_workflow(filepath: Path) -> dict:
    """Full analysis of a workflow for conversion planning."""
    wf = parse_workflow(filepath)
    graph = _build_graph(wf)
    reachable = _find_reachable_from_outputs(wf, graph)
    dead_ends = _find_dead_ends(wf, graph, reachable)
    rename_chains = _find_rename_chains(wf)
    unused_formulas = _find_unused_formulas(wf, graph, reachable)
    optimizations = _detect_optimization_opportunities(wf, graph)
    plan = _generate_python_plan(wf, graph, reachable, dead_ends, optimizations)

    return {
        "workflow": wf,
        "reachable_count": len(reachable),
        "total_tools": wf["tool_count"],
        "dead_ends": dead_ends,
        "rename_chains": rename_chains,
        "unused_formulas": unused_formulas,
        "optimizations": optimizations,
        "plan": plan,
    }


def print_conversion_plan(analysis: dict) -> None:
    """Print the conversion plan to the console."""
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich import box

    import sys, io
    if sys.platform == "win32":
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    console = Console()

    wf = analysis["workflow"]

    console.print()
    console.rule("[bold cyan]Alteryx -> Python Conversion Plan[/]", style="cyan")
    console.print()

    # --- Workflow Overview ---
    console.print(Panel(
        f"[bold white]{wf['name']}[/]  [dim]{wf['filename']}[/]",
        border_style="cyan", padding=(0, 2),
    ))

    console.print(f"  [bold]What it does:[/]")
    console.print(f"    {auto_describe(wf)}")
    console.print()
    console.print(f"  [bold]Total tools:[/]     {analysis['total_tools']}")
    console.print(f"  [bold]Tools needed:[/]    {analysis['reachable_count']}  [dim](contribute to an output)[/]")
    if analysis["dead_ends"]:
        console.print(f"  [bold]Dead ends:[/]       [yellow]{len(analysis['dead_ends'])} tools can be skipped[/]")
    else:
        console.print(f"  [bold]Dead ends:[/]       [green]None - all tools contribute to output[/]")
    console.print()

    # --- Dead Ends ---
    if analysis["dead_ends"]:
        console.print(Panel("[bold yellow]Dead Ends - Skip These[/]", border_style="yellow", padding=(0, 2)))
        console.print("  [dim]These tools don't connect to any output. They can be ignored during conversion.[/]")
        console.print()
        t = Table(box=box.SIMPLE, border_style="dim")
        t.add_column("Tool ID", style="dim")
        t.add_column("Tool", style="yellow")
        t.add_column("Annotation", style="white")
        for d in analysis["dead_ends"]:
            t.add_row(d["tool_id"], d["tool_name"], d["annotation"] or "-")
        console.print(t)

    # --- Rename Chains ---
    if analysis["rename_chains"]:
        console.print(Panel("[bold magenta]Rename Chains - Simplify[/]", border_style="magenta", padding=(0, 2)))
        console.print("  [dim]Fields renamed multiple times. Just do the final rename.[/]")
        console.print()
        for chain in analysis["rename_chains"]:
            console.print(f"    [dim]{' -> '.join(chain['chain'])}[/]")
            console.print(f"    [green]{chain['suggestion']}[/]")
            console.print()

    # --- Unused Formulas ---
    if analysis["unused_formulas"]:
        console.print(Panel("[bold yellow]Suspect Formulas - Review[/]", border_style="yellow", padding=(0, 2)))
        console.print("  [dim]These formulas look temporary or unused. Verify before converting.[/]")
        console.print()
        t = Table(box=box.SIMPLE, border_style="dim")
        t.add_column("Field", style="yellow")
        t.add_column("Expression", style="dim")
        t.add_column("Reason", style="white")
        for u in analysis["unused_formulas"]:
            t.add_row(u["field"], u["expression"], u["reason"])
        console.print(t)

    # --- Optimization Opportunities ---
    if analysis["optimizations"]:
        console.print(Panel("[bold blue]Optimization Opportunities[/]", border_style="blue", padding=(0, 2)))
        console.print("  [dim]Patterns that can be simplified or improved in Python.[/]")
        console.print()
        for opt in analysis["optimizations"]:
            priority_color = {"high": "red", "medium": "yellow", "low": "dim"}.get(opt["priority"], "white")
            console.print(f"  [{priority_color}][{opt['priority'].upper()}][/{priority_color}] [bold]{opt['type']}[/]")
            console.print(f"    {opt['detail']}")
            console.print()

    # --- Conversion Plan ---
    console.print(Panel("[bold green]Conversion Plan[/]", border_style="green", padding=(0, 2)))
    console.print("  [dim]Step-by-step Python (Polars) implementation plan.[/]")
    console.print()

    current_phase = ""
    for step in analysis["plan"]:
        if step["phase"] != current_phase:
            current_phase = step["phase"]
            console.print(f"  [bold cyan]--- {current_phase} ---[/]")
            console.print()

        console.print(f"  [bold]Step {step['step']}:[/] {step['description']}")
        if step["code_hint"]:
            console.print()
            for line in step["code_hint"].split("\n"):
                console.print(f"    [green]{line}[/]")
        if step["notes"]:
            console.print()
            for line in step["notes"].split("\n"):
                console.print(f"    [dim]{line}[/]")
        console.print()

    # --- Suggested File Structure ---
    console.print(Panel("[bold white]Suggested Project Structure[/]", border_style="white", padding=(0, 2)))
    name_slug = wf["name"].lower().replace(" ", "_")
    console.print(f"""
    {name_slug}/
    ├── main.py              # CLI entry point
    ├── config.yaml          # Connection strings, file paths
    ├── pipeline.py          # Pipeline logic (fetch, transform, export)
    ├── sql/                 # SQL queries extracted from input tools
    │   └── query.sql
    └── output/              # Generated output files
    """)

    console.print()
    console.rule(style="dim")
    console.print()


def main():
    parser = argparse.ArgumentParser(description="Generate a Python conversion plan from an Alteryx workflow")
    parser.add_argument("file", help="Path to .yxmd file to analyze")
    args = parser.parse_args()

    filepath = Path(args.file)
    if not filepath.exists():
        print(f"Error: File not found: {filepath}")
        return
    if filepath.suffix not in (".yxmd", ".yxwz", ".yxmc"):
        print(f"Error: Not an Alteryx workflow file: {filepath}")
        return

    analysis = analyze_workflow(filepath)
    print_conversion_plan(analysis)


if __name__ == "__main__":
    main()
