"""
generate_ssis_docs.py

Processes SSIS package changes passed directly from the workflow.
Also retries any entries in ssis_backlog.json.
  - CREATED / MODIFIED : generate .md doc
  - DELETED            : remove .md from wiki

Run from the root of the main repo:
    python generate_ssis_docs.py \
        --main-repo "." \
        --wiki-repo "../wiki" \
        --ssis-root "SSIS" \
        --openrouter-key "your_key_here" \
        --changes "CREATED|SSIS/CallMiner/GetTextData_CALMNR/CallMiner_Export.dtsx,..."
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
DTS    = f"{{{DTS_NS}}}"

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

def get_attr(elem, name):
    return elem.get(f"{DTS}{name}", "")


# ---------------------------------------------------------------------------
# Package parser
# ---------------------------------------------------------------------------

def parse_dtsx(dtsx_path: Path):
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

    result["package_info"] = {
        "name":    get_attr(root, "ObjectName"),
        "created": get_attr(root, "CreationDate"),
        "creator": get_attr(root, "CreatorName"),
        "version": get_attr(root, "LastModifiedProductVersion"),
    }

    # Variables
    seen_vars = set()
    for var in root.iter(f"{DTS}Variable"):
        name = get_attr(var, "ObjectName")
        val_elem = var.find(f"{DTS}VariableValue")
        val = val_elem.text if val_elem is not None else ""
        if name and name != "Propagate" and name not in seen_vars:
            seen_vars.add(name)
            result["variables"].append({"name": name, "value": val or ""})

    # Connection managers
    seen_conns = set()
    for cm in root.findall(f".//{DTS}ConnectionManager"):
        name  = get_attr(cm, "ObjectName")
        ctype = get_attr(cm, "CreationName")
        if not name or not ctype or name in seen_conns:
            continue
        seen_conns.add(name)

        conn_str = ""
        for child in cm.iter():
            cs = child.get(f"{DTS}ConnectionString")
            if cs:
                conn_str = cs
                break

        if "FLATFILE" in ctype.upper():
            simple_type = "Flat File"
        elif "OLEDB" in ctype.upper():
            simple_type = "OLE DB"
        elif "ADO.NET" in ctype.upper() or "System.Data.SqlClient" in conn_str:
            simple_type = "ADO.NET"
        elif "SMTP" in ctype.upper():
            simple_type = "SMTP"
        elif "Odbc" in conn_str:
            simple_type = "ADO.NET (ODBC)"
        else:
            simple_type = ctype

        result["connections"].append({"name": name, "type": simple_type, "conn_string": conn_str})
        if simple_type == "Flat File" and conn_str:
            result["file_sources"].append({"name": name, "path": conn_str})

    # Executables
    def parse_executables(node, depth=0):
        for exe in node.findall(f"{DTS}Executable"):
            ref   = get_attr(exe, "refId")
            name  = get_attr(exe, "ObjectName")
            etype = get_attr(exe, "ExecutableType")
            if not ref:
                continue

            type_map = {
                "STOCK:SEQUENCE": "Sequence Container",
                "Microsoft.Pipeline": "Data Flow Task",
                "Microsoft.ExecuteSQLTask": "Execute SQL Task",
                "Microsoft.ScriptTask": "Script Task",
                "Microsoft.SendMailTask": "Send Mail Task",
                "STOCK:FOREACH": "For Each Loop",
                "STOCK:FORLOOP": "For Loop",
                "Microsoft.ExecutePackageTask": "Execute Package Task",
            }
            simple = type_map.get(etype, etype.split(".")[-1] if "." in etype else etype)

            result["control_flow"].append({"ref": ref, "name": name, "type": simple, "depth": depth})

            if etype == "Microsoft.ExecutePackageTask":
                for elem in exe.iter():
                    pkg = elem.get("PackageName") or elem.get("PackageNameFromProjectReference")
                    if pkg:
                        result["packages_called"].append(pkg)

            if etype == "Microsoft.ExecuteSQLTask":
                with open(str(dtsx_path), 'r', encoding='utf-8-sig') as f:
                    raw = f.read()
                sql_matches = re.findall(r'SqlStatementSource="([^"]{10,})"', raw)
                for sql in sql_matches:
                    decoded = html.unescape(sql)
                    result["sql_tasks"].append({"name": name, "ref": ref, "sql": decoded})
                    for proc in re.findall(r"EXEC(?:UTE)?\s+(\w[\w\.]+)", decoded, re.IGNORECASE):
                        result["procedures"].add(proc)
                    _extract_tables_from_sql(decoded, result["tables"])

            if etype == "Microsoft.ScriptTask":
                read_vars = write_vars = code = ""
                for elem in exe.iter():
                    tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
                    if tag == "ScriptProject":
                        read_vars  = elem.get("ReadOnlyVariables", "")
                        write_vars = elem.get("ReadWriteVariables", "")
                    if tag == "ProjectItem" and elem.get("Name", "") == "ScriptMain.cs":
                        code = elem.text.strip() if elem.text else ""
                result["script_tasks"].append({
                    "name": name, "ref": ref,
                    "read_vars": read_vars, "write_vars": write_vars,
                    "code": code, "summary": None,
                })

            if etype == "Microsoft.Pipeline":
                components = []
                for elem in exe.iter():
                    comp_name  = elem.get("name")
                    comp_class = elem.get("componentClassID", "")
                    if not comp_name or not comp_class:
                        continue
                    short_class = comp_class.split(".")[-1] if "." in comp_class else comp_class
                    table = sql = ""
                    for prop in elem.iter("property"):
                        pname = prop.get("name", "")
                        if pname == "OpenRowset" and prop.text:
                            table = prop.text
                        if pname in ("SqlCommand", "SqlCommandParam") and prop.text:
                            sql = prop.text[:200]
                    components.append({"name": comp_name, "type": short_class, "table": table, "sql": sql})
                    if "Destination" in short_class and table:
                        clean = table.replace("[", "").replace("]", "")
                        result["tables"][clean] = result["tables"].get(clean, set())
                        result["tables"][clean].add("INSERT")
                result["data_flows"].append({"ref": ref, "name": name, "components": components})

            execs_node = exe.find(f"{DTS}Executables")
            if execs_node is not None:
                parse_executables(execs_node, depth + 1)

    execs_node = root.find(f"{DTS}Executables")
    if execs_node is not None:
        parse_executables(execs_node)

    # Precedence constraints
    for pc in root.iter(f"{DTS}PrecedenceConstraint"):
        eval_op = get_attr(pc, "EvalOp")
        label = "Failure" if eval_op == "1" else "Completion" if eval_op == "2" else "Success"
        result["precedence_constraints"].append({
            "from": get_attr(pc, "From"), "to": get_attr(pc, "To"), "label": label
        })

    # Event handlers
    for eh in root.iter(f"{DTS}EventHandler"):
        event_name = get_attr(eh, "EventName")
        tasks = [{"name": get_attr(e, "ObjectName"), "type": get_attr(e, "ExecutableType").split(".")[-1]}
                 for e in eh.iter(f"{DTS}Executable")]
        if event_name:
            result["event_handlers"].append({"event": event_name, "tasks": tasks})

    result["procedures"] = sorted(result["procedures"])
    return result


def _extract_tables_from_sql(sql: str, tables: dict):
    text = re.sub(r"\s+", " ", sql)
    for tbl in re.findall(r"(?:FROM|JOIN)\s+([^\s\(\);,]+)", text, re.IGNORECASE):
        tbl = tbl.strip().replace("[", "").replace("]", "")
        if "." in tbl and not tbl.startswith(("#", "@")):
            tables[tbl] = tables.get(tbl, set())
            tables[tbl].add("READ")
    for tbl in re.findall(r"TRUNCATE\s+TABLE\s+([^\s\(\);,]+)", text, re.IGNORECASE):
        tbl = tbl.strip().replace("[", "").replace("]", "")
        tables[tbl] = tables.get(tbl, set())
        tables[tbl].add("TRUNCATE")


# ---------------------------------------------------------------------------
# Mermaid control flow builder
# ---------------------------------------------------------------------------

def build_control_flow_mermaid(parsed: dict):
    lines = ["```mermaid", "flowchart TD"]
    top_level = [t for t in parsed["control_flow"] if t["depth"] == 0]
    node_map  = {}
    for i, task in enumerate(top_level):
        node_id = f"N{i}"
        node_map[task["ref"]] = node_id
        lines.append(f'    {node_id}["{task["name"]}\n{task["type"]}"]')
    for pc in parsed["precedence_constraints"]:
        if pc["from"] in node_map and pc["to"] in node_map:
            lines.append(f'    {node_map[pc["from"]]} -->|"{pc["label"]}"| {node_map[pc["to"]]}')
    lines.append("```")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Markdown builder
# ---------------------------------------------------------------------------

def build_markdown(parsed: dict, project: str, package_name: str, ai_summary: str):
    md = []
    info = parsed["package_info"]

    md.append(f"# {package_name}\n")
    md.append("---\n")

    md.append("## Package Info\n")
    md.append("| Property | Value |")
    md.append("|----------|-------|")
    md.append(f"| Package Name | {info.get('name', '')} |")
    md.append(f"| Project | {project} |")
    md.append(f"| Created By | {info.get('creator', '')} |")
    md.append(f"| Created Date | {info.get('created', '')} |")
    md.append(f"| Last Modified Version | {info.get('version', '')} |")
    md.append("\n---\n")

    md.append("## Description\n")
    md.append(ai_summary if ai_summary else "_AI description unavailable — will be retried._")
    md.append("\n---\n")

    md.append("## Variables\n")
    if parsed["variables"]:
        md.append("| Variable | Value |")
        md.append("|----------|-------|")
        for v in parsed["variables"]:
            md.append(f"| {v['name']} | {v['value']} |")
    else:
        md.append("_No variables._")
    md.append("\n---\n")

    md.append("## Connections\n")
    if parsed["connections"]:
        md.append("| Name | Type | Connection String |")
        md.append("|------|------|------------------|")
        for c in parsed["connections"]:
            md.append(f"| {c['name']} | {c['type']} | {c['conn_string']} |")
    else:
        md.append("_No connections._")
    md.append("\n---\n")

    md.append("## File Sources\n")
    if parsed["file_sources"]:
        md.append("| File | Path |")
        md.append("|------|------|")
        for f in parsed["file_sources"]:
            md.append(f"| {f['name']} | {f['path']} |")
    else:
        md.append("_No file sources._")
    md.append("\n---\n")

    md.append("## Control Flow\n")
    md.append(build_control_flow_mermaid(parsed))
    md.append("\n---\n")

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

    md.append("## Execute SQL Tasks\n")
    if parsed["sql_tasks"]:
        seen = set()
        for task in parsed["sql_tasks"]:
            if task["ref"] not in seen:
                seen.add(task["ref"])
                md.append(f"### {task['name']}\n")
                md.append("```sql")
                md.append(task["sql"])
                md.append("```\n")
    else:
        md.append("_No Execute SQL tasks._")
    md.append("\n---\n")

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

    md.append("## Tables Involved\n")
    if parsed["tables"]:
        md.append("| Table | Operation |")
        md.append("|-------|-----------|")
        for tbl, ops in sorted(parsed["tables"].items()):
            md.append(f"| {tbl} | {', '.join(sorted(ops))} |")
    else:
        md.append("_No tables detected._")
    md.append("\n---\n")

    md.append("## Procedures Involved\n")
    if parsed["procedures"]:
        md.append("| Procedure |")
        md.append("|-----------|")
        for proc in parsed["procedures"]:
            md.append(f"| {proc} |")
    else:
        md.append("_No procedures called._")
    md.append("\n---\n")

    md.append("## Packages Called\n")
    if parsed["packages_called"]:
        md.append("| Package |")
        md.append("|---------|")
        for pkg in sorted(set(parsed["packages_called"])):
            md.append(f"| {pkg} |")
    else:
        md.append("_No packages called._")
    md.append("\n---\n")

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

    client = OpenAI(base_url="https://openrouter.ai/api/v1", api_key=api_key)

    for model in [PRIMARY_MODEL] + FALLBACK_MODELS:
        try:
            print(f"  Trying model: {model}")
            completion = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": f"You are an SSIS documentation assistant.\nFormatting Rules:\n{rules}"},
                    {"role": "user", "content": prompt}
                ]
            )
            raw = completion.choices[0].message.content
            cleaned = re.sub(r"^(Package|Script) Analysis:\s*", "", raw.strip(), flags=re.IGNORECASE)
            print(f"  AI description generated using {model}")
            return cleaned
        except Exception as e:
            print(f"  Model {model} failed: {e}")
            continue

    return None


def build_package_prompt(parsed: dict, package_name: str):
    lines = [f"Package: {package_name}"]
    lines.append(f"Created by: {parsed['package_info'].get('creator', '')}")
    lines.append(f"\nConnections ({len(parsed['connections'])}):")
    for c in parsed["connections"][:10]:
        lines.append(f"  - {c['name']} ({c['type']}): {c['conn_string'][:80]}")
    lines.append(f"\nFile Sources: {', '.join(f['name'] for f in parsed['file_sources'][:8])}")
    lines.append(f"\nControl Flow ({len(parsed['control_flow'])} tasks):")
    for t in parsed["control_flow"]:
        if t["depth"] == 0:
            lines.append(f"  - {t['name']} ({t['type']})")
    lines.append(f"\nData Flows: {', '.join(df['name'] for df in parsed['data_flows'])}")
    lines.append(f"\nSQL Tasks ({len(parsed['sql_tasks'])}):")
    for task in parsed["sql_tasks"][:5]:
        lines.append(f"  - {task['name']}: {task['sql'][:100]}")
    lines.append(f"\nScript Tasks: {', '.join(s['name'] for s in parsed['script_tasks'])}")
    lines.append(f"\nTables ({len(parsed['tables'])}):")
    for tbl, ops in list(parsed["tables"].items())[:10]:
        lines.append(f"  - {tbl}: {', '.join(ops)}")
    lines.append(f"\nProcedures: {', '.join(parsed['procedures'][:8])}")
    lines.append(f"\nPackages Called: {', '.join(parsed['packages_called'][:5]) or 'None'}")
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
    data = read_backlog(backlog_path)
    data[key] = {"reason": reason, "failed_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")}
    backlog_path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def remove_from_backlog(backlog_path: Path, key: str):
    data = read_backlog(backlog_path)
    if key in data:
        del data[key]
        backlog_path.write_text(json.dumps(data, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# Wiki Home updater
# ---------------------------------------------------------------------------

def update_home_md(wiki_dir: Path):
    ssis_files = sorted([f.stem for f in wiki_dir.glob("SSIS_*.md")])

    groups = {}
    for stem in ssis_files:
        pkg_name = stem[5:]  # strip SSIS_
        md_path  = wiki_dir / f"{stem}.md"
        project  = "Unknown"
        if md_path.exists():
            content = md_path.read_text(encoding="utf-8")
            m = re.search(r"\| Project \| ([^\|]+) \|", content)
            if m:
                project = m.group(1).strip()
        if project not in groups:
            groups[project] = []
        groups[project].append(pkg_name)

    lines = ["## SSIS Packages\n"]
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
    content   = home_path.read_text(encoding="utf-8") if home_path.exists() else "# Home\n\n---\n"

    if "## SSIS Packages" in content:
        content = re.sub(r"## SSIS Packages.*?(?=\n---|\n## |\Z)", "", content, flags=re.DOTALL)
    content = re.sub(r"\n---\s*\n---", "\n---", content)
    content = content.rstrip() + f"\n\n{new_section}\n\n---\n"

    home_path.write_text(content, encoding="utf-8")
    print(f"  Updated Home.md — SSIS section")


# ---------------------------------------------------------------------------
# Process a single package
# ---------------------------------------------------------------------------

def process_package(path_key: str, status: str, main_repo: Path, wiki_dir: Path, api_key: str, backlog_path: Path):
    dtsx_file    = main_repo / path_key
    parts        = Path(path_key).parts
    project      = parts[1] if len(parts) > 1 else "Unknown"
    package_name = Path(parts[-1]).stem
    doc_key      = f"{project}/{package_name}"

    print(f"\n{status}: {doc_key}")

    try:
        if status in ("CREATED", "MODIFIED"):
            if not dtsx_file.exists():
                print(f"  File not found: {dtsx_file}")
                add_to_backlog(backlog_path, doc_key, "File not found")
                return False

            parsed     = parse_dtsx(dtsx_file)
            prompt     = build_package_prompt(parsed, package_name)
            ai_summary = get_ai_description(prompt, PACKAGE_RULES, api_key)

            if ai_summary is None:
                print(f"  AI failed for {doc_key} — adding to backlog.")
                add_to_backlog(backlog_path, doc_key, "AI call failed — all models exhausted")
                return False

            for script in parsed["script_tasks"]:
                if script["code"]:
                    script["summary"] = get_ai_description(script["code"], SCRIPT_RULES, api_key) or "_AI summary unavailable._"
                else:
                    script["summary"] = "_No code extracted._"

            markdown    = build_markdown(parsed, project, package_name, ai_summary)
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

        remove_from_backlog(backlog_path, doc_key)
        return True

    except Exception as e:
        print(f"  ERROR processing {doc_key}: {e}")
        add_to_backlog(backlog_path, doc_key, str(e))
        return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--main-repo",      required=True)
    parser.add_argument("--wiki-repo",      required=True)
    parser.add_argument("--ssis-root",      required=True)
    parser.add_argument("--openrouter-key", required=True)
    parser.add_argument("--changes",        required=True)
    args = parser.parse_args()

    main_repo    = Path(args.main_repo).resolve()
    wiki_dir     = Path(args.wiki_repo).resolve()
    backlog_path = main_repo / "ssis_backlog.json"

    # Parse current push changes
    changes = {}
    for item in args.changes.split(","):
        item = item.strip()
        if "|" not in item:
            continue
        status, path = item.split("|", 1)
        changes[path.strip()] = status.strip()

    # Load backlog for retry
    backlog = read_backlog(backlog_path)
    for doc_key in backlog:
        parts = doc_key.split("/", 1)
        if len(parts) == 2:
            project, package_name = parts
            # Try to find the file in SSIS folder
            ssis_root = main_repo / args.ssis_root
            matches = list(ssis_root.rglob(f"{package_name}.dtsx"))
            if matches:
                rel_path = str(matches[0].relative_to(main_repo))
                if rel_path not in changes:
                    print(f"  Retrying from backlog: {doc_key}")
                    changes[rel_path] = "MODIFIED"

    if not changes:
        print("No changes to process.")
        return

    any_processed = False
    for path_key, status in changes.items():
        success = process_package(path_key, status, main_repo, wiki_dir, args.openrouter_key, backlog_path)
        if success:
            any_processed = True

    if any_processed:
        update_home_md(wiki_dir)
        print("\nHome.md updated.")
    else:
        print("\nNothing new to process.")


if __name__ == "__main__":
    main()