"""
Alteryx Workflow Catalog Builder

Scans a directory of .yxmd/.yxwz/.yxmc files, parses the XML,
and builds a catalog of all workflows with their metadata,
inputs, outputs, tools used, and an auto-generated description.
"""

import argparse
import xml.etree.ElementTree as ET
from pathlib import Path
from collections import defaultdict

# Tool plugin names -> friendly category names
TOOL_CATEGORIES = {
    "DbFileInput": "Input",
    "DbFileOutput": "Output",
    "Join": "Join",
    "Filter": "Filter",
    "Formula": "Formula",
    "Summarize": "Summarize",
    "Sort": "Sort",
    "Select": "Select",
    "Union": "Union",
    "CrossTab": "Cross Tab",
    "Transpose": "Transpose",
    "RegEx": "RegEx",
    "DateTime": "DateTime",
    "MultiFieldFormula": "Multi-Field Formula",
    "Sample": "Sample",
    "Unique": "Unique",
    "Imputation": "Imputation",
    "GenerateRows": "Generate Rows",
    "RecordID": "Record ID",
    "MultiRowFormula": "Multi-Row Formula",
    "FindReplace": "Find Replace",
    "TextInput": "Text Input",
    "BrowseV2": "Browse",
    "AlteryxSelect": "Select",
    "Cleanse": "Data Cleansing",
    "Macro": "Macro",
    "RunCommand": "Run Command",
    "EmailOutput": "Email",
    "DownloadTool": "Download (API)",
    "BlockUntilDone": "Block Until Done",
    "Append": "Append Fields",
    "DynamicInput": "Dynamic Input",
    "DynamicReplace": "Dynamic Replace",
    "CharReplace": "Character Replace",
}


def _extract_tool_name(plugin: str) -> str:
    """Extract friendly tool name from plugin string."""
    parts = plugin.rsplit(".", 1)
    short = parts[-1] if parts else plugin
    return TOOL_CATEGORIES.get(short, short)


def _friendly_source(raw: str) -> str:
    """Turn a raw connection string / path into a short readable label."""
    if not raw:
        return "unknown"
    # Extract DSN name from ODBC string
    if "DSN=" in raw:
        for part in raw.split(";"):
            if part.strip().startswith("DSN="):
                return part.strip().split("=", 1)[1]
    # File paths — just take the filename
    if "\\" in raw or "/" in raw:
        return raw.rsplit("\\", 1)[-1].rsplit("/", 1)[-1]
    return raw


def _extract_connection_info(file_elem) -> str:
    """Pull connection string, file path, or table name from a File element."""
    parts = []
    conn = file_elem.findtext("ConnectionString", "").strip()
    if conn:
        parts.append(conn)
    table = file_elem.findtext("Table", "").strip()
    if table:
        if len(table) > 120:
            table = table[:120] + "..."
        parts.append(table)
    if file_elem.text and file_elem.text.strip():
        parts.append(file_elem.text.strip())
    return " | ".join(parts) if parts else "unknown"


def parse_workflow(filepath: Path) -> dict:
    """Parse a single .yxmd file and extract catalog metadata."""
    tree = ET.parse(filepath)
    root = tree.getroot()

    info = {
        "file": str(filepath),
        "filename": filepath.name,
        "name": "",
        "description": "",
        "author": "",
        "created": "",
        "last_saved": "",
        "annotation": "",
        "inputs": [],
        "outputs": [],
        "tools": [],
        "tool_count": 0,
        "formulas": [],
        "filters": [],
        "joins": [],
        "summarizes": [],
        "sorts": [],
        "node_details": {},   # tool_id -> {plugin, tool_name, annotation, ...}
        "connections": [],     # [{from_id, to_id}, ...]
    }

    # -- Metadata --
    meta = root.find(".//MetaInfo")
    if meta is not None:
        info["name"] = meta.findtext("Name", "").strip()
        info["description"] = meta.findtext("Description", "").strip()
        info["author"] = meta.findtext("Author", "").strip()
        info["created"] = meta.findtext("CreationDate", "").strip()
        info["last_saved"] = meta.findtext("LastSavedDate", "").strip()

    if not info["name"]:
        info["name"] = filepath.stem.replace("_", " ").title()

    # -- Workflow-level annotation --
    top_annot = root.find("./Properties/Annotation")
    if top_annot is not None:
        info["annotation"] = top_annot.findtext("DefaultAnnotationText", "").strip()

    # -- Connections (data flow graph) --
    for conn in root.findall(".//Connection"):
        origin = conn.find("Origin")
        dest = conn.find("Destination")
        if origin is not None and dest is not None:
            info["connections"].append({
                "from_id": origin.get("ToolID"),
                "to_id": dest.get("ToolID"),
                "from_conn": origin.get("Connection", ""),
                "to_conn": dest.get("Connection", ""),
            })

    # -- Nodes (tools) --
    nodes = root.findall(".//Node")
    info["tool_count"] = len(nodes)

    for node in nodes:
        gui = node.find("GuiSettings")
        if gui is None:
            continue

        tool_id = node.get("ToolID", "")
        plugin = gui.get("Plugin", "")
        tool_name = _extract_tool_name(plugin)
        if tool_name not in info["tools"]:
            info["tools"].append(tool_name)

        props = node.find("Properties")
        if props is None:
            continue

        config = props.find("Configuration")
        annotation = props.findtext(".//Annotation/Name", "").strip()
        if not annotation:
            annotation = props.findtext(".//Annotation/DefaultAnnotationText", "").strip()

        # Store node detail for graph walk
        info["node_details"][tool_id] = {
            "plugin": plugin,
            "tool_name": tool_name,
            "annotation": annotation,
        }

        # -- Inputs --
        if "DbFileInput" in plugin or "TextInput" in plugin:
            input_info = {"tool_id": tool_id, "tool": tool_name, "annotation": annotation, "source": ""}
            if config is not None:
                file_elem = config.find("File")
                if file_elem is not None:
                    input_info["source"] = _extract_connection_info(file_elem)
                    input_info["source_short"] = _friendly_source(input_info["source"])
            info["inputs"].append(input_info)

        # -- Outputs --
        elif "DbFileOutput" in plugin or "EmailOutput" in plugin:
            output_info = {"tool_id": tool_id, "tool": tool_name, "annotation": annotation, "destination": ""}
            if config is not None:
                file_elem = config.find("File")
                if file_elem is not None:
                    output_info["destination"] = _extract_connection_info(file_elem)
                    output_info["dest_short"] = _friendly_source(output_info["destination"])
                    # Detect output type
                    fmt = file_elem.get("FileFormat", "")
                    if "odbc" in output_info["destination"].lower() or fmt == "19":
                        output_info["type"] = "database"
                    elif output_info["destination"].lower().endswith(".csv"):
                        output_info["type"] = "CSV file"
                    elif output_info["destination"].lower().endswith((".xlsx", ".xls")):
                        output_info["type"] = "Excel file"
                    else:
                        output_info["type"] = "file"
            info["outputs"].append(output_info)

        # -- Filters --
        if "Filter" in plugin and config is not None:
            expr = config.findtext("Expression", "").strip()
            if expr:
                info["filters"].append({"tool_id": tool_id, "expression": expr, "annotation": annotation})

        # -- Joins --
        if "Join" in plugin and config is not None:
            join_fields = []
            for ji in config.findall(".//JoinInfo"):
                field = ji.find("Field")
                if field is not None:
                    join_fields.append(field.get("field", ""))
            if join_fields:
                info["joins"].append({
                    "tool_id": tool_id,
                    "fields": list(dict.fromkeys(join_fields)),  # dedupe, keep order
                    "annotation": annotation,
                })

        # -- Formulas --
        if "Formula" in plugin and config is not None:
            for ff in config.findall(".//FormulaField"):
                expr = ff.get("expression", "")
                field = ff.get("field", "")
                if expr:
                    info["formulas"].append({"field": field, "expression": expr})

        # -- Summarize --
        if "Summarize" in plugin and config is not None:
            for sf in config.findall(".//SummarizeField"):
                info["summarizes"].append({
                    "field": sf.get("field", ""),
                    "action": sf.get("action", ""),
                    "rename": sf.get("rename", ""),
                })

        # -- Sort --
        if "Sort" in plugin and config is not None:
            for sf in config.findall(".//SortInfo"):
                field = sf.get("field", "")
                order = sf.get("order", "Ascending")
                if field:
                    info["sorts"].append({"field": field, "order": order})

    return info


def auto_describe(wf: dict) -> str:
    """Generate a plain-English description of what the workflow does."""
    sentences = []

    # 1. What data it pulls in
    if wf["inputs"]:
        if len(wf["inputs"]) == 1:
            inp = wf["inputs"][0]
            label = inp.get("annotation") or inp.get("source_short", "a data source")
            sentences.append(f"Pulls data from {label}.")
        else:
            labels = []
            for inp in wf["inputs"]:
                labels.append(inp.get("annotation") or inp.get("source_short", "a source"))
            sentences.append(f"Pulls data from {len(labels)} sources: {', '.join(labels)}.")

    # 2. Filtering
    if wf["filters"]:
        for flt in wf["filters"]:
            expr = flt["expression"]
            if flt["annotation"]:
                sentences.append(f"Filters rows where {flt['annotation'].lower()} ({expr}).")
            else:
                sentences.append(f"Filters rows where {expr}.")

    # 3. Joins
    if wf["joins"]:
        for j in wf["joins"]:
            fields = ", ".join(j["fields"])
            if j["annotation"]:
                sentences.append(f"Joins data on [{fields}] ({j['annotation'].lower()}).")
            else:
                sentences.append(f"Joins data on [{fields}].")

    # 4. Formulas / computed columns
    if wf["formulas"]:
        new_fields = [f["field"] for f in wf["formulas"] if f["field"]]
        if len(new_fields) <= 3:
            sentences.append(f"Computes new fields: {', '.join(new_fields)}.")
        else:
            sentences.append(f"Computes {len(new_fields)} new fields including {', '.join(new_fields[:3])}.")

    # 5. Aggregations
    if wf["summarizes"]:
        agg_parts = []
        for s in wf["summarizes"]:
            name = s["rename"] or s["field"]
            agg_parts.append(f"{s['action']}({s['field']}) as {name}")
        if len(agg_parts) <= 3:
            sentences.append(f"Aggregates: {', '.join(agg_parts)}.")
        else:
            sentences.append(f"Aggregates {len(agg_parts)} measures including {', '.join(agg_parts[:3])}.")

    # 6. Other notable tools
    other = []
    for t in wf["tools"]:
        if t in ("Data Cleansing",):
            other.append("cleanses data")
        elif t in ("Cross Tab",):
            other.append("pivots data (cross tab)")
        elif t in ("Transpose",):
            other.append("transposes rows/columns")
        elif t in ("Union",):
            other.append("unions multiple streams")
        elif t in ("Unique",):
            other.append("deduplicates rows")
        elif t in ("RegEx",):
            other.append("applies regex transformations")
        elif t in ("Download (API)",):
            other.append("calls an external API")
        elif t in ("Email",):
            other.append("sends an email notification")
        elif t in ("Run Command",):
            other.append("runs an external command")
        elif t in ("Macro",):
            other.append("calls a macro (sub-workflow)")
    if other:
        sentences.append("Also " + ", ".join(other) + ".")

    # 7. Where it writes
    if wf["outputs"]:
        if len(wf["outputs"]) == 1:
            out = wf["outputs"][0]
            dest = out.get("annotation") or out.get("dest_short", "a destination")
            out_type = out.get("type", "file")
            sentences.append(f"Writes results to {dest} ({out_type}).")
        else:
            parts = []
            for out in wf["outputs"]:
                dest = out.get("annotation") or out.get("dest_short", "a destination")
                out_type = out.get("type", "file")
                parts.append(f"{dest} ({out_type})")
            sentences.append(f"Writes results to {len(parts)} destinations: {', '.join(parts)}.")

    if not sentences:
        return "Could not determine workflow purpose from XML."

    return " ".join(sentences)


def build_summary(wf: dict) -> str:
    """Build a short flow-style summary: Input -> Transform -> Output."""
    parts = []
    if wf["inputs"]:
        sources = [inp.get("annotation") or inp.get("source_short", "?") for inp in wf["inputs"]]
        parts.append(f"Reads from: {', '.join(sources)}")
    transform_tools = [t for t in wf["tools"] if t not in ("Input", "Output", "Browse")]
    if transform_tools:
        parts.append(f"Transforms: {', '.join(transform_tools)}")
    if wf["outputs"]:
        dests = [out.get("annotation") or out.get("dest_short", "?") for out in wf["outputs"]]
        parts.append(f"Outputs to: {', '.join(dests)}")
    return " -> ".join(parts) if parts else "No summary available"


def print_catalog(workflows: list[dict]) -> None:
    """Print the catalog to the console using Rich."""
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich import box

    import sys, io
    if sys.platform == "win32":
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    console = Console()

    console.print()
    console.rule("[bold cyan]Alteryx Workflow Catalog[/]", style="cyan")
    console.print(f"  [dim]Scanned {len(workflows)} workflow(s)[/]")
    console.print()

    for wf in workflows:
        # Header panel
        title_parts = [f"[bold white]{wf['name']}[/]"]
        if wf["author"]:
            title_parts.append(f"[dim]by {wf['author']}[/]")
        console.print(Panel("  ".join(title_parts), border_style="cyan", padding=(0, 2)))

        # Auto-description (always shown, replaces missing description)
        if wf["description"]:
            console.print(f"  [bold]Description:[/]  {wf['description']}")
        console.print(f"  [bold]Auto Summary:[/] {auto_describe(wf)}")

        if wf["annotation"]:
            console.print(f"  [bold]Department:[/]   {wf['annotation']}")
        console.print(f"  [bold]File:[/]         [dim]{wf['filename']}[/]")
        if wf["created"]:
            console.print(f"  [bold]Created:[/]      {wf['created']}")
        if wf["last_saved"]:
            console.print(f"  [bold]Last Saved:[/]   {wf['last_saved']}")
        console.print(f"  [bold]Tools:[/]        {wf['tool_count']} total ({', '.join(wf['tools'])})")

        # Inputs table
        if wf["inputs"]:
            console.print()
            t = Table(title="[bold green]Inputs[/]", box=box.SIMPLE, border_style="dim", show_lines=False)
            t.add_column("#", style="dim", width=3)
            t.add_column("Label", style="white", min_width=25)
            t.add_column("Source", style="cyan")
            for i, inp in enumerate(wf["inputs"], 1):
                t.add_row(str(i), inp["annotation"] or "-", inp["source"] or "-")
            console.print(t)

        # Outputs table
        if wf["outputs"]:
            t = Table(title="[bold red]Outputs[/]", box=box.SIMPLE, border_style="dim", show_lines=False)
            t.add_column("#", style="dim", width=3)
            t.add_column("Label", style="white", min_width=25)
            t.add_column("Destination", style="yellow")
            t.add_column("Type", style="dim")
            for i, out in enumerate(wf["outputs"], 1):
                t.add_row(str(i), out["annotation"] or "-", out["destination"] or "-", out.get("type", "-"))
            console.print(t)

        # Filters
        if wf["filters"]:
            t = Table(title="[bold yellow]Filters[/]", box=box.SIMPLE, border_style="dim", show_lines=False)
            t.add_column("Label", style="white", min_width=20)
            t.add_column("Expression", style="dim")
            for f in wf["filters"]:
                t.add_row(f["annotation"] or "-", f["expression"])
            console.print(t)

        # Joins
        if wf["joins"]:
            t = Table(title="[bold blue]Joins[/]", box=box.SIMPLE, border_style="dim", show_lines=False)
            t.add_column("Label", style="white", min_width=20)
            t.add_column("Join Fields", style="cyan")
            for j in wf["joins"]:
                t.add_row(j["annotation"] or "-", ", ".join(j["fields"]))
            console.print(t)

        # Formulas
        if wf["formulas"]:
            t = Table(title="[bold magenta]Formulas[/]", box=box.SIMPLE, border_style="dim", show_lines=False)
            t.add_column("Field", style="white", min_width=15)
            t.add_column("Expression", style="dim")
            for f in wf["formulas"]:
                t.add_row(f["field"], f["expression"])
            console.print(t)

        # Summarize
        if wf["summarizes"]:
            t = Table(title="[bold cyan]Aggregations[/]", box=box.SIMPLE, border_style="dim", show_lines=False)
            t.add_column("Field", style="white", min_width=15)
            t.add_column("Action", style="yellow")
            t.add_column("Output Name", style="cyan")
            for s in wf["summarizes"]:
                t.add_row(s["field"], s["action"], s["rename"] or s["field"])
            console.print(t)

        # Flow summary
        console.print()
        console.print(f"  [bold]Flow:[/] {build_summary(wf)}")
        console.print()
        console.rule(style="dim")
        console.print()


def export_csv(workflows: list[dict], output_path: Path) -> None:
    """Export catalog to a CSV file."""
    import csv

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "Workflow Name", "File", "Author", "Department",
            "Description", "Auto Description",
            "Created", "Last Saved", "Tool Count", "Tools Used",
            "Inputs", "Outputs", "Filters", "Joins", "Formulas",
            "Aggregations", "Flow",
        ])
        for wf in workflows:
            inputs_str = "; ".join(
                f"{inp['annotation'] or '-'}: {inp['source']}" for inp in wf["inputs"]
            )
            outputs_str = "; ".join(
                f"{out['annotation'] or '-'}: {out['destination']}" for out in wf["outputs"]
            )
            filters_str = "; ".join(f["expression"] for f in wf["filters"])
            joins_str = "; ".join(
                f"{j['annotation'] or '-'}: {', '.join(j['fields'])}" for j in wf["joins"]
            )
            formulas_str = "; ".join(
                f"{f['field']} = {f['expression']}" for f in wf["formulas"]
            )
            agg_str = "; ".join(
                f"{s['action']}({s['field']})" for s in wf["summarizes"]
            )
            writer.writerow([
                wf["name"], wf["filename"], wf["author"], wf["annotation"],
                wf["description"], auto_describe(wf),
                wf["created"], wf["last_saved"],
                wf["tool_count"], ", ".join(wf["tools"]),
                inputs_str, outputs_str, filters_str, joins_str,
                formulas_str, agg_str, build_summary(wf),
            ])

    print(f"Exported catalog to {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Build a catalog of Alteryx workflows")
    parser.add_argument(
        "path", nargs="?", default=".",
        help="Directory to scan for .yxmd files (default: current directory)",
    )
    parser.add_argument(
        "--recursive", "-r", action="store_true",
        help="Scan subdirectories recursively",
    )
    parser.add_argument(
        "--csv", type=str, default=None,
        help="Export catalog to a CSV file",
    )
    args = parser.parse_args()

    scan_dir = Path(args.path)
    if not scan_dir.is_dir():
        print(f"Error: {scan_dir} is not a directory")
        return

    # Find all workflow files
    extensions = ("*.yxmd", "*.yxwz", "*.yxmc")
    files = []
    for ext in extensions:
        if args.recursive:
            files.extend(scan_dir.rglob(ext))
        else:
            files.extend(scan_dir.glob(ext))

    files = sorted(files)

    if not files:
        print(f"No Alteryx workflow files found in {scan_dir}")
        return

    # Parse all workflows
    workflows = []
    for f in files:
        try:
            wf = parse_workflow(f)
            workflows.append(wf)
        except ET.ParseError as e:
            print(f"  Warning: Failed to parse {f.name}: {e}")

    # Output
    print_catalog(workflows)

    if args.csv:
        export_csv(workflows, Path(args.csv))


if __name__ == "__main__":
    main()
