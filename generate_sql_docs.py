"""
generate_sql_docs.py

Processes SQL stored procedure changes passed directly from the workflow.
Also retries any entries in backlog.json.
  - CREATED / MODIFIED : generate .md doc
  - DELETED            : remove .md from wiki

Run from the root of the main repo:
    python generate_sql_docs.py \
        --main-repo "." \
        --wiki-repo "../wiki" \
        --sql-root "SQL" \
        --openrouter-key "your_key_here" \
        --changes "CREATED|SQL/BISQL/DB/DB/dbo/Stored Procedures/usp.sql,..."
"""

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path

try:
    from openai import OpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    print("Warning: openai package not available — AI descriptions will be skipped.")

POINT_OF_CONTACT = "analytics@radiusgs.com"

PRIMARY_MODEL   = "nvidia/nemotron-3-super-120b-a12b:free"
FALLBACK_MODELS = [
    "meta-llama/llama-3.3-70b-instruct:free",
    "google/gemma-3-27b-it:free",
]

AI_RULES = """
- Explain the given SQL stored procedure in around 200 words
- Do not reply with anything other than the explanation of what the code does
- Do not include what the variables or tables contain, explain what the SQL code does in a broader aspect
- Do not tell the user to read and understand it themselves
- Always complete the explanation without cutting the response in the middle
- Start with 'Code Analysis:' and then give the explanation without giving any suggestion of your own
- Do not mention parameter names or variable names specifically
"""


# ---------------------------------------------------------------------------
# Parameters extractor
# ---------------------------------------------------------------------------

def extract_parameters(sql_text: str):
    proc_match = re.search(
        r"CREATE\s+PROCEDURE\s+[^\(]+\((.+?)\)\s*AS",
        sql_text, re.IGNORECASE | re.DOTALL
    )
    if not proc_match:
        return []

    params = []
    for part in re.split(r",(?![^\(]*\))", proc_match.group(1)):
        part = part.strip()
        if not part:
            continue
        direction = "OUTPUT" if re.search(r"\bOUTPUT\b", part, re.IGNORECASE) else "INPUT"
        m = re.match(r"(@[\w]+)\s+([A-Z]+(?:\([^\)]*\))?)", part, re.IGNORECASE)
        if m:
            params.append({
                "parameter": m.group(1),
                "data_type": m.group(2).upper(),
                "direction": direction,
            })
    return params


# ---------------------------------------------------------------------------
# Variables extractor
# ---------------------------------------------------------------------------

def extract_variables(sql_text: str):
    declare_blocks = re.findall(
        r"DECLARE\s+(.*?)(?=\n\s*(BEGIN|SET|SELECT|WITH|INSERT|UPDATE|DELETE|TRUNCATE|;|--))",
        sql_text, re.IGNORECASE | re.DOTALL
    )
    result = {}
    for block, _ in declare_blocks:
        for part in re.split(r",(?![^\(]*\))", block.replace("\n", " ")):
            m = re.search(r"(@[\w]+)\s+([A-Z]+(?:\([^\)]*\))?)", part, re.IGNORECASE)
            if m:
                result[m.group(1)] = m.group(2).upper()
    return result


# ---------------------------------------------------------------------------
# Tables extractor
# ---------------------------------------------------------------------------

def clean_table_name(tbl: str):
    return tbl.strip().rstrip(",").split()[0].replace("[", "").replace("]", "")


def extract_tables(sql_text: str):
    text = re.sub(r"\s+", " ", sql_text)
    read_tables  = set()
    write_tables = {}

    def add_write(tbl, op):
        if tbl not in write_tables:
            write_tables[tbl] = set()
        write_tables[tbl].add(op)

    for tbl in re.findall(r"(?:FROM|JOIN)\s+([^\s\(\);,]+)", text, re.IGNORECASE):
        tbl = clean_table_name(tbl)
        if not tbl.startswith(("#", "##", "@")) and "." in tbl:
            read_tables.add(tbl)

    for pattern, op in [
        (r"INSERT\s+(?:INTO\s+)?([^\s\(\);,]+)", "INSERT"),
        (r"UPDATE\s+([^\s\(\);,]+)", "UPDATE"),
        (r"DELETE\s+(?:FROM\s+)?([^\s\(\);,]+)", "DELETE"),
        (r"TRUNCATE\s+TABLE\s+([^\s\(\);,]+)", "TRUNCATE"),
        (r"MERGE\s+(?:INTO\s+)?([^\s\(\);,]+)", "MERGE"),
        (r"BULK\s+INSERT\s+([^\s\(\);,]+)", "BULK INSERT"),
    ]:
        for tbl in re.findall(pattern, text, re.IGNORECASE):
            tbl = clean_table_name(tbl)
            if not tbl.startswith(("#", "##", "@")) and "." in tbl:
                add_write(tbl, op)

    pure_read = read_tables - set(write_tables.keys())
    return sorted(pure_read), {k: sorted(v) for k, v in write_tables.items()}


# ---------------------------------------------------------------------------
# Called procedures extractor
# ---------------------------------------------------------------------------

def extract_called_procedures(sql_text: str, wiki_dir: Path):
    text   = re.sub(r"\s+", " ", sql_text)
    called = set()
    for match in re.findall(r"EXEC\s+([^\s\(\);@][^\s\(\);]*)", text, re.IGNORECASE):
        match = match.strip().rstrip(",").replace("[", "").replace("]", "")
        if match and "." in match:
            called.add(match)

    # Build with hyperlinks where wiki page exists
    result = []
    for p in sorted(called):
        proc_name = p.split(".")[-1]
        wiki_page = wiki_dir / f"SP_{proc_name}.md"
        if wiki_page.exists():
            result.append(f"[{p}](SP_{proc_name})")
        else:
            result.append(p)
    return result


# ---------------------------------------------------------------------------
# Mermaid diagram builder
# ---------------------------------------------------------------------------

def build_mermaid(procedure: str, read_tables: list, write_tables: dict, called_procs: list):
    lines = ["```mermaid", "flowchart LR", f'    PROC["{procedure}"]']
    for idx, tbl in enumerate(read_tables):
        lines.append(f'    SRC{idx+1}[("{tbl}")] --> PROC')
    for idx, tbl in enumerate(write_tables.keys()):
        lines.append(f'    PROC --> TGT{idx+1}[("{tbl}")]')
    for idx, proc in enumerate(called_procs):
        # Strip markdown link for node label
        label = re.sub(r'\[([^\]]+)\]\([^\)]+\)', r'\1', proc)
        lines.append(f'    PROC --> EXT{idx+1}[["{label}"]]')
    lines.append("```")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Markdown builder
# ---------------------------------------------------------------------------

def build_markdown(meta, ai_description, parameters, variables, read_tables, write_tables, called_procs):
    procedure = meta["procedure"]
    md = []

    md.append(f"# {procedure}\n")
    md.append("---\n")

    md.append("## Metadata\n")
    md.append("| Server | Database | Schema | Procedure |")
    md.append("|--------|----------|--------|-----------|")
    md.append(f"| {meta['server']} | {meta['database']} | {meta['schema']} | {procedure} |\n")
    md.append("---\n")

    md.append("## Description\n")
    md.append(ai_description if ai_description else "_AI description unavailable — will be retried._")
    md.append("\n---\n")

    md.append("## Parameters\n")
    if parameters:
        md.append("| Parameter | Data Type | Direction |")
        md.append("|-----------|-----------|-----------|")
        for p in parameters:
            md.append(f"| {p['parameter']} | {p['data_type']} | {p['direction']} |")
    else:
        md.append("_No parameters._")
    md.append("\n---\n")

    md.append("## Declared Variables\n")
    if variables:
        md.append("| Variable | Data Type |")
        md.append("|----------|-----------|")
        for v, t in variables.items():
            md.append(f"| {v} | {t} |")
    else:
        md.append("_No declared variables._")
    md.append("\n---\n")

    md.append("## Data Lineage\n")
    md.append(build_mermaid(procedure, read_tables, write_tables, called_procs))
    md.append("\n---\n")

    md.append("## Read Tables\n")
    if read_tables:
        md.append("| Table |")
        md.append("|-------|")
        for t in read_tables:
            md.append(f"| {t} |")
    else:
        md.append("_No read tables._")
    md.append("\n---\n")

    md.append("## Write Tables\n")
    if write_tables:
        md.append("| Table | Operation |")
        md.append("|-------|-----------|")
        for t, ops in write_tables.items():
            md.append(f"| {t} | {', '.join(ops)} |")
    else:
        md.append("_No write tables._")
    md.append("\n---\n")

    md.append("## Called Procedures\n")
    if called_procs:
        md.append("| Procedure |")
        md.append("|-----------|")
        for p in called_procs:
            md.append(f"| {p} |")
    else:
        md.append("_No external procedures called._")
    md.append("\n---\n")

    md.append("## Point of Contact\n")
    md.append(POINT_OF_CONTACT)
    md.append("")

    return "\n".join(md)


# ---------------------------------------------------------------------------
# AI description
# ---------------------------------------------------------------------------

def get_ai_description(sql_text: str, api_key: str):
    if not OPENAI_AVAILABLE:
        return None

    client = OpenAI(base_url="https://openrouter.ai/api/v1", api_key=api_key)

    for model in [PRIMARY_MODEL] + FALLBACK_MODELS:
        try:
            print(f"  Trying model: {model}")
            completion = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": f"You are a SQL documentation assistant.\nFormatting Rules:\n{AI_RULES}"},
                    {"role": "user", "content": sql_text}
                ]
            )
            raw = completion.choices[0].message.content
            cleaned = re.sub(r"^Code Analysis:\s*", "", raw.strip(), flags=re.IGNORECASE)
            print(f"  AI description generated using {model}")
            return cleaned
        except Exception as e:
            print(f"  Model {model} failed: {e}")
            continue

    return None


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
# Wiki SQL index page updater
# ---------------------------------------------------------------------------

def update_home_md(wiki_dir: Path):
    """Rebuild SQL Stored Procedures section of Home.md grouped by server/database/schema."""
    sp_files = sorted([f.stem for f in wiki_dir.glob("SP_*.md")])

    # Parse server/database/schema from each SP file's metadata
    groups = {}
    for stem in sp_files:
        proc_name = stem[3:]  # strip SP_
        md_path = wiki_dir / f"{stem}.md"
        server = database = schema = "Unknown"
        if md_path.exists():
            content = md_path.read_text(encoding="utf-8")
            m = re.search(r"\|\s*([^|]+?)\s*\|\s*([^|]+?)\s*\|\s*([^|]+?)\s*\|\s*" + re.escape(proc_name), content)
            if m:
                server   = m.group(1).strip()
                database = m.group(2).strip()
                schema   = m.group(3).strip()

        key = (server, database, schema)
        if key not in groups:
            groups[key] = []
        groups[key].append(proc_name)

    lines = []
    lines.append("## SQL Stored Procedures\n")
    if groups:
        for (server, database, schema) in sorted(groups):
            lines.append(f"### {server}\n")
            lines.append(f"#### {database}\n")
            lines.append(f"##### {schema}\n")
            for proc in sorted(groups[(server, database, schema)]):
                encoded = f"SP_{proc}".replace(" ", "%20")
                lines.append(f"- [{proc}]({encoded})")
            lines.append("")
    else:
        lines.append("_No stored procedures documented yet._\n")

    new_section = "\n".join(lines)

    home_path = wiki_dir / "Home.md"
    content = home_path.read_text(encoding="utf-8") if home_path.exists() else "# Home\n\n---\n"

    if "## SQL Stored Procedures" in content:
        content = re.sub(r"## SQL Stored Procedures.*?(?=\n---|\n## |\Z)", "", content, flags=re.DOTALL)
    content = re.sub(r"\n---\s*\n---", "\n---", content)
    content = content.rstrip() + f"\n\n{new_section}\n\n---\n"

    home_path.write_text(content, encoding="utf-8")
    print(f"  Updated Home.md — SQL section")


# ---------------------------------------------------------------------------
# Process a single procedure
# ---------------------------------------------------------------------------

def process_procedure(path_key: str, status: str, main_repo: Path, wiki_dir: Path, api_key: str, backlog_path: Path):
    sql_file = main_repo / path_key
    parts    = Path(path_key).parts

    if len(parts) < 7:
        print(f"  Skipping malformed path: {path_key}")
        return False

    server    = parts[1]
    database  = parts[2]
    schema    = parts[4]
    procedure = Path(parts[6]).stem
    doc_key   = f"{server}/{database}/{schema}/{procedure}"

    print(f"\n{status}: {doc_key}")

    try:
        if status in ("CREATED", "MODIFIED"):
            if not sql_file.exists():
                print(f"  File not found: {sql_file}")
                add_to_backlog(backlog_path, doc_key, "File not found")
                return False

            sql_text     = sql_file.read_text(encoding="utf-8-sig")
            meta         = {"server": server, "database": database, "schema": schema, "procedure": procedure}
            parameters   = extract_parameters(sql_text)
            variables    = extract_variables(sql_text)
            read_tables, write_tables = extract_tables(sql_text)
            called_procs = extract_called_procedures(sql_text, wiki_dir)

            ai_description = get_ai_description(sql_text, api_key)
            if ai_description is None:
                print(f"  AI failed for {doc_key} — adding to backlog.")
                add_to_backlog(backlog_path, doc_key, "AI call failed — all models exhausted")
                return False

            markdown = build_markdown(meta, ai_description, parameters, variables, read_tables, write_tables, called_procs)
            output_path = wiki_dir / f"SP_{procedure}.md"
            output_path.write_text(markdown, encoding="utf-8")
            print(f"  Saved: {output_path}")

        elif status == "DELETED":
            md_path = wiki_dir / f"SP_{procedure}.md"
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
    parser.add_argument("--sql-root",       required=True)
    parser.add_argument("--openrouter-key", required=True)
    parser.add_argument("--changes",        required=True, help="Pipe-separated: STATUS|path,...")
    args = parser.parse_args()

    main_repo = Path(args.main_repo).resolve()
    wiki_dir  = Path(args.wiki_repo).resolve()
    backlog_path = main_repo / "backlog.json"

    # Parse current push changes
    changes = {}
    for item in args.changes.split(","):
        item = item.strip()
        if "|" not in item:
            continue
        status, path = item.split("|", 1)
        changes[path.strip()] = status.strip()

    # Also load backlog entries for retry
    backlog = read_backlog(backlog_path)
    for doc_key, info in backlog.items():
        # Convert doc_key back to path: server/database/schema/procedure
        parts = doc_key.split("/")
        if len(parts) == 4:
            server, database, schema, procedure = parts
            path = f"SQL/{server}/{database}/{database}/{schema}/Stored Procedures/{procedure}.sql"
            if path not in changes:
                print(f"  Retrying from backlog: {doc_key}")
                changes[path] = "MODIFIED"

    if not changes:
        print("No changes to process.")
        return

    any_processed = False
    for path_key, status in changes.items():
        success = process_procedure(path_key, status, main_repo, wiki_dir, args.openrouter_key, backlog_path)
        if success:
            any_processed = True

    if any_processed:
        update_home_md(wiki_dir)
        print("\nHome.md updated.")
    else:
        print("\nNothing new to process.")


if __name__ == "__main__":
    main()