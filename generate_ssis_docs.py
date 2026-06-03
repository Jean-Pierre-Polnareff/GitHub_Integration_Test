"""
generate_ssis_docs.py

Processes SSIS package changes passed directly from the workflow.
Supports both old format (SQL Server 2005-2012) and new format (SQL Server 2012+).
Also retries any entries in ssis_backlog.json.

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

# Old format executable type mappings (assembly name fragments -> simple name)
OLD_TYPE_MAP = {
    "ExecuteSQLTask":      "Execute SQL Task",
    "ScriptTask":          "Script Task",
    "SendMailTask":        "Send Mail Task",
    "ExecutePackageTask":  "Execute Package Task",
    "FileSystemTask":      "File System Task",
    "FtpTask":             "FTP Task",
    "BulkInsertTask":      "Bulk Insert Task",
    "DataFlowTask":        "Data Flow Task",
    "FileWatcherTask":     "File Watcher Task",
    "TimerTask":           "Timer Task",
    "FileOperationTask":   "File Operation Task",
    "RestApiTask":         "REST API Task",
    "SFTPTask":            "SFTP Task",
    "WebServiceTask":      "Web Service Task",
    "HttpTask":            "HTTP Task",
    "SSIS.Pipeline":       "Data Flow Task",
    "STOCK:SEQUENCE":      "Sequence Container",
    "STOCK:FOREACH":       "For Each Loop",
    "STOCK:FORLOOP":       "For Loop",
}

# New format executable type mappings
NEW_TYPE_MAP = {
    "STOCK:SEQUENCE":                    "Sequence Container",
    "STOCK:FOREACH":                     "For Each Loop",
    "STOCK:FORLOOP":                     "For Loop",
    "Microsoft.Pipeline":                "Data Flow Task",
    "Microsoft.ExecuteSQLTask":          "Execute SQL Task",
    "Microsoft.ScriptTask":              "Script Task",
    "Microsoft.SendMailTask":            "Send Mail Task",
    "Microsoft.ExecutePackageTask":      "Execute Package Task",
    "Microsoft.FileSystemTask":          "File System Task",
    "Microsoft.FtpTask":                 "FTP Task",
    "Microsoft.BulkInsertTask":          "Bulk Insert Task",
}


# ---------------------------------------------------------------------------
# Dual-format XML helpers
# ---------------------------------------------------------------------------

def get_prop(elem, name):
    """
    Get a property value from either:
    - New format: DTS:Name="value" attribute
    - Old format: <DTS:Property DTS:Name="Name">value</DTS:Property> child element
    """
    # New format — attribute
    val = elem.get(f"{DTS}{name}", "")
    if val:
        return val
    # Old format — property element
    for prop in elem.findall(f"{DTS}Property"):
        if prop.get(f"{DTS}Name") == name:
            return prop.text or ""
    return ""


def simplify_exec_type(etype: str):
    """Convert full assembly name or short type to a readable label."""
    if not etype:
        return "Task"

    # New format — exact match
    if etype in NEW_TYPE_MAP:
        return NEW_TYPE_MAP[etype]

    # Old format — match fragment in assembly name
    for fragment, label in OLD_TYPE_MAP.items():
        if fragment in etype:
            return label

    # Fallback — last segment
    return etype.split(".")[-1] if "." in etype else etype


def is_pipeline(etype: str):
    return "Pipeline" in etype or etype == "Microsoft.Pipeline"


def is_execute_sql(etype: str):
    return "ExecuteSQLTask" in etype or etype == "Microsoft.ExecuteSQLTask"


def is_script_task(etype: str):
    return "ScriptTask" in etype or etype == "Microsoft.ScriptTask"


def is_execute_package(etype: str):
    return "ExecutePackageTask" in etype or etype == "Microsoft.ExecutePackageTask"


def get_connection_string(cm_elem):
    """Extract connection string from either format connection manager."""
    # New format — attribute on inner ConnectionManager
    inner = cm_elem.find(f"{DTS}ObjectData/{DTS}ConnectionManager")
    if inner is not None:
        cs = get_prop(inner, "ConnectionString")
        if cs:
            return cs

    # Old format — inside ObjectData as various child elements
    obj_data = cm_elem.find(f"{DTS}ObjectData")
    if obj_data is not None:
        # Try inner DTS:ConnectionManager
        inner_cm = obj_data.find(f"{DTS}ConnectionManager")
        if inner_cm is not None:
            cs = get_prop(inner_cm, "ConnectionString")
            if cs:
                return cs
        # Try direct child attributes (SMTP, HTTP etc.)
        for child in obj_data:
            cs = child.get("ConnectionString", "")
            if cs:
                return cs

    # Try iterating all descendants
    for child in cm_elem.iter():
        cs = child.get(f"{DTS}ConnectionString") or child.get("ConnectionString", "")
        if cs:
            return cs

    return ""


# ---------------------------------------------------------------------------
# Package parser
# ---------------------------------------------------------------------------

def parse_dtsx(dtsx_path: Path):
    try:
        tree = ET.parse(dtsx_path)
        root = tree.getroot()
    except Exception as e:
        raise ValueError(f"Failed to parse XML: {e}")

    # Read raw content for regex-based extraction
    with open(dtsx_path, 'r', encoding='utf-8-sig') as f:
        raw_content = f.read()

    result = {
        "package_info":           {},
        "variables":              [],
        "connections":            [],
        "file_sources":           [],
        "control_flow":           [],
        "precedence_constraints": [],
        "data_flows":             [],
        "sql_tasks":              [],
        "script_tasks":           [],
        "event_handlers":         [],
        "tables":                 {},
        "procedures":             set(),
        "packages_called":        [],
    }

    # -- Package info --
    result["package_info"] = {
        "name":    get_prop(root, "ObjectName") or get_prop(root, "PackageName") or dtsx_path.stem,
        "created": get_prop(root, "CreationDate"),
        "creator": get_prop(root, "CreatorName"),
        "version": get_prop(root, "LastModifiedProductVersion"),
    }

    # -- Variables --
    seen_vars = set()
    for var in root.iter(f"{DTS}Variable"):
        name = get_prop(var, "ObjectName")
        val_elem = var.find(f"{DTS}VariableValue")
        val = val_elem.text if val_elem is not None else ""
        if name and name != "Propagate" and name not in seen_vars:
            seen_vars.add(name)
            result["variables"].append({"name": name, "value": val or ""})

    # -- Connection managers --
    seen_conns = set()
    for cm in root.iter(f"{DTS}ConnectionManager"):
        name  = get_prop(cm, "ObjectName")
        ctype = get_prop(cm, "CreationName")
        if not name or name in seen_conns:
            continue
        seen_conns.add(name)

        conn_str = get_connection_string(cm)

        # Simplify type
        ctype_upper = ctype.upper()
        if "FLATFILE" in ctype_upper or "FLAT" in ctype_upper:
            simple_type = "Flat File"
        elif "OLEDB" in ctype_upper or "OLE DB" in ctype_upper:
            simple_type = "OLE DB"
        elif "ADO.NET" in ctype_upper or "System.Data.SqlClient" in conn_str:
            simple_type = "ADO.NET"
        elif "SMTP" in ctype_upper:
            simple_type = "SMTP"
        elif "HTTP" in ctype_upper:
            simple_type = "HTTP"
        elif "FTP" in ctype_upper:
            simple_type = "FTP"
        elif "Odbc" in conn_str or "ODBC" in ctype_upper:
            simple_type = "ODBC"
        elif "FILE" in ctype_upper:
            simple_type = "File"
        else:
            simple_type = ctype or "Unknown"

        result["connections"].append({
            "name":        name,
            "type":        simple_type,
            "conn_string": conn_str,
        })

        if simple_type in ("Flat File", "File") and conn_str:
            result["file_sources"].append({"name": name, "path": conn_str})

    # -- Executables --
    def parse_executables(node, depth=0):
        for exe in node.findall(f"{DTS}Executable"):
            name  = get_prop(exe, "ObjectName")
            etype = get_prop(exe, "ExecutableType")
            ref   = get_prop(exe, "refId") or get_prop(exe, "DTSID") or name

            if not name:
                continue

            simple = simplify_exec_type(etype)

            result["control_flow"].append({
                "ref":   ref,
                "name":  name,
                "type":  simple,
                "depth": depth,
            })

            # Execute Package Task
            if is_execute_package(etype):
                for elem in exe.iter():
                    pkg = elem.get("PackageName") or elem.get("PackageNameFromProjectReference")
                    if not pkg:
                        pkg = get_prop(elem, "PackageName")
                    if pkg:
                        result["packages_called"].append(pkg)

            # Execute SQL Task
            if is_execute_sql(etype):
                sql_found = ""
                # New format — SqlStatementSource as attribute on SqlTaskData
                for elem in exe.iter():
                    # Try both namespace prefixes: new format and old SQLTask: namespace
                    sql_attr = (elem.get("SqlStatementSource", "") or
                                elem.get("{www.microsoft.com/sqlserver/dts/tasks/sqltask}SqlStatementSource", ""))
                    if sql_attr:
                        sql_found = html.unescape(sql_attr)
                        break
                    # Also try any attribute containing SqlStatementSource
                    for attr_key, attr_val in elem.attrib.items():
                        if "SqlStatementSource" in attr_key and attr_val:
                            sql_found = html.unescape(attr_val)
                            break
                    if sql_found:
                        break
                # Old format — SqlStatementSource as DTS:Property element
                if not sql_found:
                    for prop in exe.iter(f"{DTS}Property"):
                        if prop.get(f"{DTS}Name") == "SqlStatementSource" and prop.text:
                            sql_found = prop.text
                            break
                if sql_found:
                    result["sql_tasks"].append({"name": name, "ref": ref, "sql": sql_found})
                    for proc in re.findall(r"EXEC(?:UTE)?\s+(\w[\w\.]+)", sql_found, re.IGNORECASE):
                        result["procedures"].add(proc)
                    _extract_tables_from_sql(sql_found, result["tables"])

            # Script Task — new format CDATA
            if is_script_task(etype):
                read_vars = write_vars = code = ""
                for elem in exe.iter():
                    tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
                    if tag == "ScriptProject":
                        read_vars  = elem.get("ReadOnlyVariables", "")
                        write_vars = elem.get("ReadWriteVariables", "")
                    if tag == "ProjectItem" and elem.get("Name", "") == "ScriptMain.cs":
                        code = elem.text.strip() if elem.text else ""
                result["script_tasks"].append({
                    "name":       name,
                    "ref":        ref,
                    "read_vars":  read_vars,
                    "write_vars": write_vars,
                    "code":       code if code else "_BINARY_",
                    "summary":    None,
                })

            # Data Flow / Pipeline
            if is_pipeline(etype):
                components = []
                for elem in exe.iter():
                    comp_name  = elem.get("name")
                    comp_class = elem.get("componentClassID", "")
                    if not comp_name or not comp_class:
                        continue
                    # Simplify component class
                    short_class = comp_class.split(".")[-1] if "." in comp_class else comp_class
                    # For GUIDs use generic labels
                    if re.match(r'^\{[0-9A-F-]+\}$', comp_class, re.IGNORECASE):
                        short_class = "Component"

                    table = sql_cmd = ""
                    for prop in elem.iter("property"):
                        pname = prop.get("name", "")
                        if pname == "OpenRowset" and prop.text:
                            table = prop.text
                        if pname in ("SqlCommand", "SqlCommandParam") and prop.text:
                            sql_cmd = prop.text[:200]

                    # For Flat File Destination — get path from connection manager by GUID
                    if not table and ("FlatFile" in short_class or "Flat File" in comp_name or
                                      "Destination" in comp_name):
                        for conn_ref in elem.iter("connection"):
                            cm_guid = conn_ref.get("connectionManagerID", "")
                            if cm_guid:
                                # Match GUID against connection manager DTSIDs
                                for cm in root.iter(f"{DTS}ConnectionManager"):
                                    cm_dtsid = get_prop(cm, "DTSID")
                                    if cm_dtsid == cm_guid:
                                        conn_str = get_connection_string(cm)
                                        if conn_str:
                                            table = conn_str
                                        break

                    components.append({
                        "name":  comp_name,
                        "type":  short_class,
                        "table": table,
                        "sql":   sql_cmd,
                    })

                    # Detect destination:
                    # - Named class contains Destination (new format)
                    # - OR has a table but no SQL (old format with GUID class IDs)
                    is_destination = ("Destination" in short_class or
                                      "Destination" in comp_name or
                                      (table and not sql_cmd))
                    if is_destination and table:
                        clean = table.replace("[", "").replace("]", "")
                        result["tables"][clean] = result["tables"].get(clean, set())
                        result["tables"][clean].add("INSERT")

                result["data_flows"].append({
                    "ref":        ref,
                    "name":       name,
                    "components": components,
                })

            # Recurse into containers
            # New format: has DTS:Executables wrapper
            # Old format: direct DTS:Executable children with no wrapper
            inner = exe.find(f"{DTS}Executables")
            if inner is not None:
                parse_executables(inner, depth + 1)
            elif exe.findall(f"{DTS}Executable"):
                # Old format — recurse directly into the container element
                parse_executables(exe, depth + 1)

    execs_node = root.find(f"{DTS}Executables")
    if execs_node is not None:
        # New format — executables under DTS:Executables wrapper
        parse_executables(execs_node)
    else:
        # Old format — executables are direct children of root
        parse_executables(root)

    # -- Precedence constraints --
    # Build DTSID -> name map for old format PC resolution
    dtsid_to_name = {get_prop(exe, "DTSID"): get_prop(exe, "ObjectName")
                     for exe in root.iter(f"{DTS}Executable")
                     if get_prop(exe, "DTSID") and get_prop(exe, "ObjectName")}

    for pc in root.iter(f"{DTS}PrecedenceConstraint"):
        from_ref = get_prop(pc, "From")
        to_ref   = get_prop(pc, "To")
        eval_op  = get_prop(pc, "EvalOp")
        label    = "Failure" if eval_op == "1" else "Completion" if eval_op == "2" else "Success"

        # Old format — From/To are DTS:Executable children with IDREF + DTS:IsFrom attributes
        if not from_ref and not to_ref:
            for pc_exe in pc.findall(f"{DTS}Executable"):
                idref   = pc_exe.get("IDREF", "")
                is_from = pc_exe.get(f"{DTS}IsFrom", "0")
                name    = dtsid_to_name.get(idref, idref)
                if is_from == "-1":
                    from_ref = name
                else:
                    to_ref = name

        if from_ref or to_ref:
            result["precedence_constraints"].append({
                "from": from_ref, "to": to_ref, "label": label
            })

    # -- Event handlers --
    for eh in root.iter(f"{DTS}EventHandler"):
        event_name = get_prop(eh, "EventName")
        tasks = []
        for exe in eh.iter(f"{DTS}Executable"):
            t_name = get_prop(exe, "ObjectName")
            t_type = simplify_exec_type(get_prop(exe, "ExecutableType"))
            if t_name:
                tasks.append({"name": t_name, "type": t_type})
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
    for tbl in re.findall(r"INSERT\s+(?:INTO\s+)?([^\s\(\);,]+)", text, re.IGNORECASE):
        tbl = tbl.strip().replace("[", "").replace("]", "")
        if not tbl.startswith(("#", "@")):
            tables[tbl] = tables.get(tbl, set())
            tables[tbl].add("INSERT")
    for tbl in re.findall(r"UPDATE\s+([^\s\(\);,]+)", text, re.IGNORECASE):
        tbl = tbl.strip().replace("[", "").replace("]", "")
        if "." in tbl and not tbl.startswith(("#", "@")):
            tables[tbl] = tables.get(tbl, set())
            tables[tbl].add("UPDATE")


# ---------------------------------------------------------------------------
# Mermaid control flow (simplified top-level only)
# ---------------------------------------------------------------------------

def build_control_flow_mermaid(parsed: dict):
    lines = ["```mermaid", "flowchart TD"]
    top_level = [t for t in parsed["control_flow"] if t["depth"] == 0]
    node_map  = {}
    for i, task in enumerate(top_level):
        node_id = f"N{i}"
        node_map[task["ref"]] = node_id
        node_map[task["name"]] = node_id  # also map by name for old format
        label = f"{task['name']}\n{task['type']}"
        lines.append(f'    {node_id}["{label}"]')

    for pc in parsed["precedence_constraints"]:
        from_id = node_map.get(pc["from"])
        to_id   = node_map.get(pc["to"])
        if from_id and to_id:
            lines.append(f'    {from_id} -->|"{pc["label"]}"| {to_id}')

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
    md.append(f"| Package Name | {info.get('name', package_name)} |")
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
            md.append(f"| {c['name']} | {c['type']} | {c['conn_string'][:120]} |")
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
                    # Sanitize newlines so they don't break markdown table rows
                    detail = detail.replace("\r\n", " ").replace("\n", " ").replace("\r", " ")
                    md.append(f"| {comp['name']} | {comp['type']} | {detail[:100]} |")
            md.append("")
    else:
        md.append("_No data flow tasks._")
    md.append("\n---\n")

    md.append("## Execute SQL Tasks\n")
    if parsed["sql_tasks"]:
        seen = set()
        for task in parsed["sql_tasks"]:
            key = task["sql"][:50]
            if key not in seen:
                seen.add(key)
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
            if script["code"] == "_BINARY_":
                md.append("_Source code stored in compiled binary format — upgrade package to SQL Server 2012+ format to extract._\n")
            elif script["code"]:
                md.append("<details>")
                md.append("<summary>View Source Code</summary>")
                md.append("")
                md.append("```csharp")
                md.append(script["code"])
                md.append("```")
                md.append("")
                md.append("</details>\n")
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
                    {"role": "user",   "content": prompt}
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
    lines = [f"Package: {package_name}"]
    lines.append(f"Created by: {parsed['package_info'].get('creator', '')}")
    lines.append(f"\nConnections ({len(parsed['connections'])}):")
    for c in parsed["connections"][:8]:
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
    data      = read_backlog(backlog_path)
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

            # AI summaries for script tasks
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
    for doc_key in list(backlog.keys()):
        parts = doc_key.split("/", 1)
        if len(parts) == 2:
            project, package_name = parts
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