"""
generate_ssis_docs.py

Reads ssis_changelog.md and ssis_processed.json, then for each unprocessed SSIS package:
  - Parses .dtsx XML to extract package info, connections, variables, control flow,
    data flow, SQL tasks, script tasks, tables, procedures, packages called
  - Generates Mermaid control flow diagram
  - Uses OpenRouter AI for package summary and script task summaries
  - Outputs .md to wiki repo
  - Tracks failures in ssis_backlog.json

Run from the root of the main repo:
    python generate_ssis_docs.py \
        --main-repo "." \
        --wiki-repo "../wiki" \
        --ssis-root "SSIS" \
        --openrouter-key "your_key_here"
"""

import argparse
import json
import re
import html
from datetime import datetime, timezone
from pathlib import Path
import xml.etree.ElementTree as ET

try:
    from openai import OpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    print("Warning: openai package not available — AI descriptions will be skipped.")

POINT_OF_CONTACT = "analytics@radiusgs.com"
DTS_NS = "www.microsoft.com/SqlServer/Dts"
DTS = f"{{{DTS_NS}}}"

PRIMARY_MODEL   = "nvidia/nemotron-3-super-120b-a12b:free"
FALLBACK_MODELS = [
    "meta-llama/llama-3.3-70b-instruct:free",
    "google/gemma-3-27b-it:free",
]

PACKAGE_RULES = """
- Explain the given SSIS package in around 200 words
- Do not reply with anything other than the explanation of what the package does
- Explain what data is extracted, transformed, and loaded at a high level
- Do not mention variable names or technical SSIS details
- Always complete the explanation without cutting the response in the middle
- Start with 'Package Analysis:' and then give the explanation
- Do not give any suggestions of your own
"""

SCRIPT_RULES = """
- Explain the given C# script task code in around 200 words
- Do not reply with anything other than the explanation of what the script does
- Explain what the script accomplishes at a high level
- Do not mention variable names specifically
- Always complete the explanation without cutting the response in the middle
- Start with 'Script Analysis:' and then give the explanation
- Do not give any suggestions of your own
"""


# ---------------------------------------------------------------------------
# XML helpers
# ---------------------------------------------------------------------------

def decode_html(text):
    if not text:
        return ""
    return html.unescape(text)


def get_attr(elem, name):
    return elem.get(f"{DTS}{name}", "")


# ---------------------------------------------------------------------------
# Package parser
# ---------------------------------------------------------------------------

def parse_dtsx(dtsx_path: Path):
    """
    Parse a .dtsx file and return a structured dict with all extracted info.
    """
    try:
        tree = ET.parse(dtsx_path)
        root = tree.getroot()
    except Exception as e:
        raise ValueError(f"Failed to parse XML: {e}")

    result = {
        "package_info": {},
        "variables": [],
        "connections": [],
        "file_sources": [],
        "control_flow": [],
        "precedence_constraints": [],
        "data_flows": [],
        "sql_tasks": [],
        "script_tasks": [],
        "event_handlers": [],
        "tables": {},
        "procedures": set(),
        "packages_called": [],
    }

    # -- Package info --
    result["package_info"] = {
        "name":     get_attr(root, "ObjectName"),
        "created":  get_attr(root, "CreationDate"),
        "creator":  get_attr(root, "CreatorName"),
        "version":  get_attr(root, "LastModifiedProductVersion"),
    }

    # -- Variables --
    for var in root.iter(f"{DTS}Variable"):
        name  = get_attr(var, "ObjectName")
        val_elem = var.find(f"{DTS}VariableValue")
        val   = val_elem.text if val_elem is not None else ""
        if name and not name == "Propagate":
            result["variables"].append({"name": name, "value": val or ""})

    # -- Connection managers --
    for cm in root.findall(f".//{DTS}ConnectionManager"):
        name  = get_attr(cm, "ObjectName")
        ctype = get_attr(cm, "CreationName")
        if not name or not ctype:
            continue
        # Get connection string
        conn_str = ""
        inner = cm.find(f"{DTS}ObjectData/{DTS}ConnectionManager")
        if inner is not None:
            conn_str = inner.get(f"{DTS}ConnectionString", "")
        if not conn_str:
            for child in cm.iter():
                cs = child.get(f"{DTS}ConnectionString")
                if cs:
                    conn_str = cs
                    break

        # Simplify type
        if "FLATFILE" in ctype.upper():
            simple_type = "Flat File"
        elif "OLEDB" in ctype.upper() or "OLEDB" in ctype.upper():
            simple_type = "OLE DB"
        elif "ADO.NET" in ctype.upper() or "System.Data.SqlClient" in conn_str:
            simple_type = "ADO.NET"
        elif "SMTP" in ctype.upper():
            simple_type = "SMTP"
        elif "Odbc" in conn_str:
            simple_type = "ADO.NET (ODBC)"
        else:
            simple_type = ctype

        result["connections"].append({
            "name":        name,
            "type":        simple_type,
            "conn_string": conn_str,
        })

        # File sources
        if simple_type == "Flat File" and conn_str:
            result["file_sources"].append({
                "name": name,
                "path": conn_str,
            })

    # -- Executables (control flow) --
    def parse_executables(node, parent_ref="Package"):
        for exe in node.findall(f"{DTS}Executable"):
            ref   = get_attr(exe, "refId")
            name  = get_attr(exe, "ObjectName")
            etype = get_attr(exe, "ExecutableType")

            if not ref:
                continue

            # Simplify type
            if etype == "STOCK:SEQUENCE":
                simple = "Sequence Container"
            elif etype == "Microsoft.Pipeline":
                simple = "Data Flow Task"
            elif etype == "Microsoft.ExecuteSQLTask":
                simple = "Execute SQL Task"
            elif etype == "Microsoft.ScriptTask":
                simple = "Script Task"
            elif etype == "Microsoft.SendMailTask":
                simple = "Send Mail Task"
            elif etype == "STOCK:FOREACH":
                simple = "For Each Loop"
            elif etype == "STOCK:FORLOOP":
                simple = "For Loop"
            elif etype == "Microsoft.ExecutePackageTask":
                simple = "Execute Package Task"
            else:
                simple = etype.split(".")[-1] if "." in etype else etype

            depth = ref.count("\\") - 1  # depth relative to package
            result["control_flow"].append({
                "ref":    ref,
                "name":   name,
                "type":   simple,
                "depth":  depth,
                "parent": parent_ref,
            })

            # Execute Package Task — packages called
            if etype == "Microsoft.ExecutePackageTask":
                for elem in exe.iter():
                    pkg = elem.get("PackageName") or elem.get("PackageNameFromProjectReference")
                    if pkg:
                        result["packages_called"].append(pkg)

            # Execute SQL Task — extract SQL
            if etype == "Microsoft.ExecuteSQLTask":
                for elem in exe.iter():
                    tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
                    if tag == "SqlTaskData":
                        sql = decode_html(elem.get("SqlStatementSource", ""))
                        conn = elem.get("Connection", "")
                        if sql:
                            result["sql_tasks"].append({
                                "name": name,
                                "ref":  ref,
                                "sql":  sql,
                                "conn": conn,
                            })
                            # Extract procedures
                            for proc in re.findall(r"EXEC(?:UTE)?\s+(\w[\w\.]+)", sql, re.IGNORECASE):
                                result["procedures"].add(proc)
                            # Extract tables from SQL
                            _extract_tables_from_sql(sql, result["tables"])

            # Script Task — extract C# code
            if etype == "Microsoft.ScriptTask":
                read_vars  = ""
                write_vars = ""
                code       = ""
                for elem in exe.iter():
                    tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
                    if tag == "ScriptProject":
                        read_vars  = elem.get("ReadOnlyVariables", "")
                        write_vars = elem.get("ReadWriteVariables", "")
                    if tag == "ProjectItem":
                        item_name = elem.get("Name", "")
                        if item_name == "ScriptMain.cs" or item_name == "ScriptMain.vb":
                            cdata = elem.text or ""
                            if cdata:
                                code = cdata.strip()

                result["script_tasks"].append({
                    "name":       name,
                    "ref":        ref,
                    "read_vars":  read_vars,
                    "write_vars": write_vars,
                    "code":       code,
                    "summary":    None,  # filled by AI later
                })

            # Data Flow Task — extract components
            if etype == "Microsoft.Pipeline":
                components = []
                for elem in exe.iter():
                    comp_name  = elem.get("name")
                    comp_class = elem.get("componentClassID", "")
                    if not comp_name or not comp_class:
                        continue
                    short_class = comp_class.split(".")[-1] if "." in comp_class else comp_class

                    table = ""
                    sql   = ""
                    for prop in elem.iter("property"):
                        pname = prop.get("name", "")
                        if pname == "OpenRowset" and prop.text:
                            table = prop.text
                        if pname in ("SqlCommand", "SqlCommandParam") and prop.text:
                            sql = prop.text[:200]

                    components.append({
                        "name":  comp_name,
                        "type":  short_class,
                        "table": table,
                        "sql":   sql,
                    })

                    # Add destination tables
                    if "Destination" in short_class and table:
                        clean = table.replace("[", "").replace("]", "")
                        result["tables"][clean] = result["tables"].get(clean, set())
                        result["tables"][clean].add("INSERT")

                result["data_flows"].append({
                    "ref":        ref,
                    "name":       name,
                    "components": components,
                })

            # Recurse into containers
            execs_node = exe.find(f"{DTS}Executables")
            if execs_node is not None:
                parse_executables(execs_node, ref)

    execs_node = root.find(f"{DTS}Executables")
    if execs_node is not None:
        parse_executables(execs_node)

    # -- Precedence constraints --
    for pc in root.iter(f"{DTS}PrecedenceConstraint"):
        from_ref = get_attr(pc, "From")
        to_ref   = get_attr(pc, "To")
        eval_op  = get_attr(pc, "EvalOp")
        value    = get_attr(pc, "Value")

        # EvalOp: None/0=Success, 1=Failure, 2=Completion, 3=Expression
        if eval_op in ("1",):
            label = "Failure"
        elif eval_op in ("2",):
            label = "Completion"
        else:
            label = "Success"

        result["precedence_constraints"].append({
            "from":  from_ref,
            "to":    to_ref,
            "label": label,
        })

    # -- Event handlers --
    for eh in root.iter(f"{DTS}EventHandler"):
        event_name = get_attr(eh, "EventName")
        tasks = []
        for exe in eh.iter(f"{DTS}Executable"):
            tasks.append({
                "name": get_attr(exe, "ObjectName"),
                "type": get_attr(exe, "ExecutableType").split(".")[-1],
            })
        if event_name:
            result["event_handlers"].append({
                "event": event_name,
                "tasks": tasks,
            })

    result["procedures"] = sorted(result["procedures"])
    return result


def _extract_tables_from_sql(sql: str, tables: dict):
    text = re.sub(r"\s+", " ", sql)
    for tbl in re.findall(r"(?:FROM|JOIN)\s+([^\s\(\);,]+)", text, re.IGNORECASE):
        tbl = tbl.strip().replace("[", "").replace("]", "")
        if "." in tbl and not tbl.startswith("#") and not tbl.startswith("@"):
            tables[tbl] = tables.get(tbl, set())
            tables[tbl].add("READ")
    for tbl in re.findall(r"(?:INSERT\s+INTO|INSERT)\s+([^\s\(\);,]+)", text, re.IGNORECASE):
        tbl = tbl.strip().replace("[", "").replace("]", "")
        if not tbl.startswith("#") and not tbl.startswith("@"):
            tables[tbl] = tables.get(tbl, set())
            tables[tbl].add("INSERT")
    for tbl in re.findall(r"UPDATE\s+([^\s\(\);,]+)", text, re.IGNORECASE):
        tbl = tbl.strip().replace("[", "").replace("]", "")
        if "." in tbl and not tbl.startswith("#") and not tbl.startswith("@"):
            tables[tbl] = tables.get(tbl, set())
            tables[tbl].add("UPDATE")
    for tbl in re.findall(r"TRUNCATE\s+TABLE\s+([^\s\(\);,]+)", text, re.IGNORECASE):
        tbl = tbl.strip().replace("[", "").replace("]", "")
        tables[tbl] = tables.get(tbl, set())
        tables[tbl].add("TRUNCATE")


# ---------------------------------------------------------------------------
# Mermaid control flow builder (simplified — top level only)
# ---------------------------------------------------------------------------

def build_control_flow_mermaid(parsed: dict, package_name: str):
    lines = ["```mermaid", "flowchart TD"]

    # Only top level tasks (depth == 0)
    top_level = [t for t in parsed["control_flow"] if t["depth"] == 0]

    # Build node map
    node_map = {}
    for i, task in enumerate(top_level):
        node_id = f"N{i}"
        node_map[task["ref"]] = node_id
        label = f"{task['name']}\n{task['type']}"
        lines.append(f'    {node_id}["{label}"]')

    # Build edges from precedence constraints (top level only)
    for pc in parsed["precedence_constraints"]:
        from_ref = pc["from"]
        to_ref   = pc["to"]
        label    = pc["label"]
        if from_ref in node_map and to_ref in node_map:
            from_id = node_map[from_ref]
            to_id   = node_map[to_ref]
            lines.append(f'    {from_id} -->|"{label}"| {to_id}')

    lines.append("```")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Markdown builder
# ---------------------------------------------------------------------------

def build_markdown(parsed: dict, project: str, package_name: str, ai_summary: str):
    md = []

    md.append(f"# {package_name}\n")
    md.append("---\n")

    # Package info
    info = parsed["package_info"]
    md.append("## Package Info\n")
    md.append("| Property | Value |")
    md.append("|----------|-------|")
    md.append(f"| Package Name | {info.get('name', '')} |")
    md.append(f"| Project | {project} |")
    md.append(f"| Created By | {info.get('creator', '')} |")
    md.append(f"| Created Date | {info.get('created', '')} |")
    md.append(f"| Last Modified Version | {info.get('version', '')} |")
    md.append("\n---\n")

    # Description
    md.append("## Description\n")
    md.append(ai_summary if ai_summary else "_AI description unavailable — will be retried._")
    md.append("\n---\n")

    # Variables
    md.append("## Variables\n")
    if parsed["variables"]:
        md.append("| Variable | Value |")
        md.append("|----------|-------|")
        seen = set()
        for v in parsed["variables"]:
            if v["name"] not in seen:
                seen.add(v["name"])
                md.append(f"| {v['name']} | {v['value']} |")
    else:
        md.append("_No variables._")
    md.append("\n---\n")

    # Connections
    md.append("## Connections\n")
    if parsed["connections"]:
        md.append("| Name | Type | Connection String |")
        md.append("|------|------|------------------|")
        seen = set()
        for c in parsed["connections"]:
            if c["name"] not in seen:
                seen.add(c["name"])
                md.append(f"| {c['name']} | {c['type']} | {c['conn_string']} |")
    else:
        md.append("_No connections._")
    md.append("\n---\n")

    # File sources
    md.append("## File Sources\n")
    if parsed["file_sources"]:
        md.append("| File | Path |")
        md.append("|------|------|")
        seen = set()
        for f in parsed["file_sources"]:
            if f["name"] not in seen:
                seen.add(f["name"])
                md.append(f"| {f['name']} | {f['path']} |")
    else:
        md.append("_No file sources._")
    md.append("\n---\n")

    # Control flow diagram
    md.append("## Control Flow\n")
    md.append(build_control_flow_mermaid(parsed, package_name))
    md.append("\n---\n")

    # Data flow
    md.append("## Data Flow\n")
    if parsed["data_flows"]:
        for df in parsed["data_flows"]:
            md.append(f"### {df['name']}\n")
            if df["components"]:
                md.append("| Component | Type | Detail |")
                md.append("|-----------|------|--------|")
                for comp in df["components"]:
                    detail = comp["table"] or comp["sql"] or ""
                    md.append(f"| {comp['name']} | {comp['type']} | {detail[:100]} |")
            md.append("")
    else:
        md.append("_No data flow tasks._")
    md.append("\n---\n")

    # Execute SQL tasks
    md.append("## Execute SQL Tasks\n")
    if parsed["sql_tasks"]:
        for task in parsed["sql_tasks"]:
            md.append(f"### {task['name']}\n")
            md.append("```sql")
            md.append(task["sql"])
            md.append("```\n")
    else:
        md.append("_No Execute SQL tasks._")
    md.append("\n---\n")

    # Script tasks
    md.append("## Script Tasks\n")
    if parsed["script_tasks"]:
        for script in parsed["script_tasks"]:
            md.append(f"### {script['name']}\n")
            if script["read_vars"]:
                md.append(f"**Variables Read:** {script['read_vars']}\n")
            if script["write_vars"]:
                md.append(f"**Variables Written:** {script['write_vars']}\n")
            if script["summary"]:
                md.append(f"{script['summary']}\n")
            if script["code"]:
                md.append("```csharp")
                md.append(script["code"][:2000])
                md.append("```\n")
    else:
        md.append("_No Script tasks._")
    md.append("\n---\n")

    # Event handlers
    md.append("## Event Handlers\n")
    if parsed["event_handlers"]:
        md.append("| Event | Task | Type |")
        md.append("|-------|------|------|")
        for eh in parsed["event_handlers"]:
            for task in eh["tasks"]:
                md.append(f"| {eh['event']} | {task['name']} | {task['type']} |")
    else:
        md.append("_No event handlers._")
    md.append("\n---\n")

    # Tables involved
    md.append("## Tables Involved\n")
    if parsed["tables"]:
        md.append("| Table | Operation |")
        md.append("|-------|-----------|")
        for tbl, ops in sorted(parsed["tables"].items()):
            md.append(f"| {tbl} | {', '.join(sorted(ops))} |")
    else:
        md.append("_No tables detected._")
    md.append("\n---\n")

    # Procedures involved
    md.append("## Procedures Involved\n")
    if parsed["procedures"]:
        md.append("| Procedure |")
        md.append("|-----------|")
        for proc in parsed["procedures"]:
            md.append(f"| {proc} |")
    else:
        md.append("_No procedures called._")
    md.append("\n---\n")

    # Packages called
    md.append("## Packages Called\n")
    if parsed["packages_called"]:
        md.append("| Package |")
        md.append("|---------|")
        for pkg in sorted(set(parsed["packages_called"])):
            md.append(f"| {pkg} |")
    else:
        md.append("_No packages called._")
    md.append("\n---\n")

    # Point of contact
    md.append("## Point of Contact\n")
    md.append(POINT_OF_CONTACT)
    md.append("")

    return "\n".join(md)


# ---------------------------------------------------------------------------
# AI
# ---------------------------------------------------------------------------

def get_ai_description(prompt: str, rules: str, api_key: str):
    if not OPENAI_AVAILABLE:
        return None

    client = OpenAI(
        base_url="https://openrouter.ai/api/v1",
        api_key=api_key,
    )

    models = [PRIMARY_MODEL] + FALLBACK_MODELS

    for model in models:
        try:
            print(f"  Trying model: {model}")
            completion = client.chat.completions.create(
                model=model,
                messages=[
                    {
                        "role": "system",
                        "content": f"You are an SSIS documentation assistant.\nFormatting Rules:\n{rules}"
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ]
            )
            raw     = completion.choices[0].message.content
            cleaned = re.sub(r"^(Package|Script) Analysis:\s*", "", raw.strip(), flags=re.IGNORECASE)
            print(f"  AI description generated using {model}")
            return cleaned
        except Exception as e:
            print(f"  Model {model} failed: {e}")
            continue

    return None


def build_package_prompt(parsed: dict, package_name: str):
    """Build condensed package representation for AI summary."""
    lines = []
    lines.append(f"Package: {package_name}")
    lines.append(f"Created by: {parsed['package_info'].get('creator', '')}")

    lines.append(f"\nConnections ({len(parsed['connections'])}):")
    for c in parsed["connections"][:10]:
        lines.append(f"  - {c['name']} ({c['type']}): {c['conn_string'][:100]}")

    lines.append(f"\nFile Sources ({len(parsed['file_sources'])}):")
    for f in parsed["file_sources"][:10]:
        lines.append(f"  - {f['name']}: {f['path']}")

    lines.append(f"\nControl Flow ({len(parsed['control_flow'])} tasks):")
    for t in parsed["control_flow"]:
        if t["depth"] == 0:
            lines.append(f"  - {t['name']} ({t['type']})")

    lines.append(f"\nData Flows ({len(parsed['data_flows'])}):")
    for df in parsed["data_flows"]:
        lines.append(f"  - {df['name']}")
        for comp in df["components"]:
            if comp["table"]:
                lines.append(f"    {comp['type']}: {comp['table']}")

    lines.append(f"\nSQL Tasks ({len(parsed['sql_tasks'])}):")
    for task in parsed["sql_tasks"]:
        lines.append(f"  - {task['name']}: {task['sql'][:150]}")

    lines.append(f"\nScript Tasks ({len(parsed['script_tasks'])}):")
    for s in parsed["script_tasks"]:
        lines.append(f"  - {s['name']}")

    lines.append(f"\nTables ({len(parsed['tables'])}):")
    for tbl, ops in list(parsed["tables"].items())[:15]:
        lines.append(f"  - {tbl}: {', '.join(ops)}")

    lines.append(f"\nProcedures: {', '.join(parsed['procedures'][:10])}")
    lines.append(f"Packages Called: {', '.join(parsed['packages_called'][:5]) or 'None'}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Backlog tracker
# ---------------------------------------------------------------------------

def read_backlog(backlog_path: Path):
    if not backlog_path.exists():
        return {}
    try:
        return json.loads(backlog_path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def add_to_backlog(backlog_path: Path, key: str, reason: str):
    data      = read_backlog(backlog_path)
    data[key] = {
        "reason":    reason,
        "failed_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
    }
    backlog_path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def remove_from_backlog(backlog_path: Path, key: str):
    data = read_backlog(backlog_path)
    if key in data:
        del data[key]
        backlog_path.write_text(json.dumps(data, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# Processed tracker
# ---------------------------------------------------------------------------

def read_processed(processed_path: Path):
    if not processed_path.exists():
        return {}
    try:
        return json.loads(processed_path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def mark_processed(processed_path: Path, key: str):
    data      = read_processed(processed_path)
    data[key] = "completed"
    processed_path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def is_processed(processed_path: Path, key: str, status: str):
    return read_processed(processed_path).get(key) == "completed" and status != "MODIFIED"


# ---------------------------------------------------------------------------
# Changelog reader
# ---------------------------------------------------------------------------

def read_changelog(changelog_path: Path):
    if not changelog_path.exists():
        return {}

    content  = changelog_path.read_text(encoding="utf-8")
    sections = re.split(r'(?=^## \d{4}-\d{2}-\d{2})', content, flags=re.MULTILINE)

    result = {}
    for section in sections:
        date_match = re.match(r"## (\d{4}-\d{2}-\d{2})", section)
        if not date_match:
            continue
        date    = date_match.group(1)
        entries = {}
        for line in section.split("\n"):
            line = line.strip()
            if ":" in line and not line.startswith("#") and not line.startswith("-"):
                parts = line.split(":", 1)
                if len(parts) != 2:
                    continue
                status = parts[0].strip()
                path   = parts[1].strip()
                if status in ("CREATED", "MODIFIED", "DELETED") and path.endswith(".dtsx"):
                    entries[path] = status
        if entries:
            result[date] = entries

    return result


# ---------------------------------------------------------------------------
# Wiki Home updater
# ---------------------------------------------------------------------------

def update_home_md(wiki_dir: Path, processed_path: Path, ssis_root: Path, main_repo: Path):
    processed = read_processed(processed_path)

    # Group by project group (first folder under SSIS/)
    groups = {}
    for key in processed:
        if processed[key] != "completed":
            continue
        parts = Path(key).parts
        # key format: SSIS/<project>/.../package.dtsx
        if len(parts) < 2:
            continue
        project  = parts[1]
        pkg_name = Path(parts[-1]).stem
        if project not in groups:
            groups[project] = []
        groups[project].append(pkg_name)

    # Build SSIS section
    lines = []
    lines.append("## SSIS Packages\n")
    if groups:
        for project in sorted(groups):
            lines.append(f"### {project}\n")
            for pkg in sorted(groups[project]):
                encoded = f"SSIS_{pkg}".replace(" ", "%20")
                lines.append(f"- [{pkg}]({encoded})")
            lines.append("")
    else:
        lines.append("_No SSIS packages documented yet._\n")

    new_section = "\n".join(lines)

    home_path = wiki_dir / "Home.md"
    if home_path.exists():
        content = home_path.read_text(encoding="utf-8")
    else:
        content = "# Home\n\n---\n"

    if "## SSIS Packages" in content:
        content = re.sub(
            r"## SSIS Packages.*?(?=\n---|\n## |\Z)",
            new_section,
            content,
            flags=re.DOTALL
        )
    else:
        content = content.rstrip() + f"\n\n{new_section}\n\n---\n"

    home_path.write_text(content, encoding="utf-8")
    print(f"  Updated Home.md — SSIS section")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--main-repo",      required=True)
    parser.add_argument("--wiki-repo",      required=True)
    parser.add_argument("--ssis-root",      required=True)
    parser.add_argument("--openrouter-key", required=True)
    args = parser.parse_args()

    main_repo = Path(args.main_repo).resolve()
    wiki_dir  = Path(args.wiki_repo).resolve()
    ssis_root = main_repo / args.ssis_root

    changelog_path = main_repo / "ssis_changelog.md"
    processed_path = main_repo / "ssis_processed.json"
    backlog_path   = main_repo / "ssis_backlog.json"

    changelog = read_changelog(changelog_path)

    if not changelog:
        print("No SSIS changelog entries found.")
        return

    any_processed = False

    for date, entries in sorted(changelog.items()):
        for path_key, status in entries.items():
            dtsx_file = main_repo / path_key
            parts     = Path(path_key).parts

            # Extract project group and package name
            if len(parts) < 2:
                print(f"  Skipping malformed path: {path_key}")
                continue

            project      = parts[1]
            package_name = Path(parts[-1]).stem
            doc_key      = f"{project}/{package_name}"

            if is_processed(processed_path, doc_key, status):
                print(f"  Already processed {doc_key} — skipping.")
                continue

            print(f"\n[{date}] {status}: {doc_key}")

            try:
                if status in ("CREATED", "MODIFIED"):
                    if not dtsx_file.exists():
                        print(f"  File not found: {dtsx_file}")
                        add_to_backlog(backlog_path, doc_key, "File not found")
                        continue

                    # Parse package
                    parsed = parse_dtsx(dtsx_file)

                    # AI package summary
                    prompt     = build_package_prompt(parsed, package_name)
                    ai_summary = get_ai_description(prompt, PACKAGE_RULES, args.openrouter_key)

                    if ai_summary is None:
                        print(f"  AI failed for {doc_key} — adding to backlog.")
                        add_to_backlog(backlog_path, doc_key, "AI call failed — all models exhausted")
                        continue

                    # AI script task summaries
                    for script in parsed["script_tasks"]:
                        if script["code"]:
                            summary = get_ai_description(script["code"], SCRIPT_RULES, args.openrouter_key)
                            script["summary"] = summary or "_AI summary unavailable._"
                        else:
                            script["summary"] = "_No code extracted._"

                    # Build markdown
                    markdown = build_markdown(parsed, project, package_name, ai_summary)

                    output_path = wiki_dir / f"SSIS_{package_name}.md"
                    output_path.write_text(markdown, encoding="utf-8")
                    print(f"  Saved: {output_path}")

                elif status == "DELETED":
                    md_path = wiki_dir / f"SSIS_{package_name}.md"
                    if md_path.exists():
                        md_path.unlink()
                        print(f"  Deleted: {md_path}")
                    else:
                        print(f"  Skipping delete (not found): {md_path}")

                mark_processed(processed_path, doc_key)
                remove_from_backlog(backlog_path, doc_key)
                any_processed = True

            except Exception as e:
                print(f"  ERROR processing {doc_key}: {e}")
                add_to_backlog(backlog_path, doc_key, str(e))
                continue

    if any_processed:
        update_home_md(wiki_dir, processed_path, ssis_root, main_repo)
        print("\nHome.md updated.")
    else:
        print("\nNothing new to process.")


if __name__ == "__main__":
    main()