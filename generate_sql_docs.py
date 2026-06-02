"""
generate_sql_docs.py

Reads sql_changelog.md and sql_processed.json, then for each unprocessed SQL stored procedure:
  - Extracts metadata from folder structure
  - Extracts parameters, variables, read/write tables, called procedures
  - Generates Mermaid data lineage diagram
  - Uses OpenRouter AI for natural language description
  - Outputs .md to wiki repo
  - Tracks failures in backlog.json

Run from the root of the main repo:
    python generate_sql_docs.py \
        --main-repo "." \
        --wiki-repo "../wiki" \
        --sql-root "SQL" \
        --openrouter-key "your_key_here"
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
        sql_text,
        re.IGNORECASE | re.DOTALL
    )
    if not proc_match:
        return []

    params_block = proc_match.group(1)
    params = []

    parts = re.split(r",(?![^\(]*\))", params_block)
    for part in parts:
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
        sql_text,
        re.IGNORECASE | re.DOTALL
    )

    result = {}
    for block, _ in declare_blocks:
        block = block.replace("\n", " ")
        parts = re.split(r",(?![^\(]*\))", block)
        for part in parts:
            m = re.search(r"(@[\w]+)\s+([A-Z]+(?:\([^\)]*\))?)", part, re.IGNORECASE)
            if m:
                result[m.group(1)] = m.group(2).upper()

    return result


# ---------------------------------------------------------------------------
# Tables extractor
# ---------------------------------------------------------------------------

def clean_table_name(tbl: str):
    tbl = tbl.strip().rstrip(",").split()[0]
    tbl = tbl.replace("[", "").replace("]", "")
    return tbl


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
        if tbl.startswith(("#", "##", "@")) or not tbl:
            continue
        if "." in tbl:
            read_tables.add(tbl)

    for tbl in re.findall(r"INSERT\s+(?:INTO\s+)?([^\s\(\);,]+)", text, re.IGNORECASE):
        tbl = clean_table_name(tbl)
        if tbl.startswith(("#", "##", "@")) or not tbl:
            continue
        if "." in tbl:
            add_write(tbl, "INSERT")

    for tbl in re.findall(r"UPDATE\s+([^\s\(\);,]+)", text, re.IGNORECASE):
        tbl = clean_table_name(tbl)
        if tbl.startswith(("#", "##", "@")) or not tbl:
            continue
        if "." in tbl:
            add_write(tbl, "UPDATE")

    for tbl in re.findall(r"DELETE\s+(?:FROM\s+)?([^\s\(\);,]+)", text, re.IGNORECASE):
        tbl = clean_table_name(tbl)
        if tbl.startswith(("#", "##", "@")) or not tbl:
            continue
        if "." in tbl:
            add_write(tbl, "DELETE")

    for tbl in re.findall(r"TRUNCATE\s+TABLE\s+([^\s\(\);,]+)", text, re.IGNORECASE):
        tbl = clean_table_name(tbl)
        if tbl.startswith(("#", "##", "@")) or not tbl:
            continue
        if "." in tbl:
            add_write(tbl, "TRUNCATE")

    for tbl in re.findall(r"MERGE\s+(?:INTO\s+)?([^\s\(\);,]+)", text, re.IGNORECASE):
        tbl = clean_table_name(tbl)
        if tbl.startswith(("#", "##", "@")) or not tbl:
            continue
        if "." in tbl:
            add_write(tbl, "MERGE")

    for tbl in re.findall(r"BULK\s+INSERT\s+([^\s\(\);,]+)", text, re.IGNORECASE):
        tbl = clean_table_name(tbl)
        if tbl.startswith(("#", "##", "@")) or not tbl:
            continue
        if "." in tbl:
            add_write(tbl, "BULK INSERT")

    pure_read = read_tables - set(write_tables.keys())
    return sorted(pure_read), {k: sorted(v) for k, v in write_tables.items()}


# ---------------------------------------------------------------------------
# Called procedures extractor
# ---------------------------------------------------------------------------

def extract_called_procedures(sql_text: str):
    text   = re.sub(r"\s+", " ", sql_text)
    called = set()

    for match in re.findall(r"EXEC\s+([^\s\(\);@][^\s\(\);]*)", text, re.IGNORECASE):
        match = match.strip().rstrip(",").replace("[", "").replace("]", "")
        if match and "." in match:
            called.add(match)

    return sorted(called)


# ---------------------------------------------------------------------------
# Mermaid diagram builder
# ---------------------------------------------------------------------------

def build_mermaid(procedure: str, read_tables: list, write_tables: dict, called_procs: list):
    lines = []
    lines.append("```mermaid")
    lines.append("flowchart LR")
    lines.append(f'    PROC["{procedure}"]')

    for idx, tbl in enumerate(read_tables):
        lines.append(f'    SRC{idx + 1}[("{tbl}")] --> PROC')

    for idx, tbl in enumerate(write_tables.keys()):
        lines.append(f'    PROC --> TGT{idx + 1}[("{tbl}")]')

    for idx, proc in enumerate(called_procs):
        lines.append(f'    PROC --> EXT{idx + 1}[["{proc}"]]')

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
                        "content": f"You are a SQL documentation assistant.\nFormatting Rules:\n{AI_RULES}"
                    },
                    {
                        "role": "user",
                        "content": sql_text
                    }
                ]
            )
            raw     = completion.choices[0].message.content
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


def is_processed(processed_path: Path, key: str):
    return read_processed(processed_path).get(key) == "completed"


# ---------------------------------------------------------------------------
# sql_changelog.md reader
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
                if status in ("CREATED", "MODIFIED", "DELETED") and "Stored Procedures" in path:
                    entries[path] = status
        if entries:
            result[date] = entries

    return result


# ---------------------------------------------------------------------------
# Wiki SQL index page updater
# ---------------------------------------------------------------------------

def update_sql_home(wiki_dir: Path):
    md_files = sorted([f.stem for f in wiki_dir.glob("SP_*.md")])

    lines = []
    lines.append("# SQL Stored Procedures\n")
    lines.append("---\n")
    if md_files:
        for name in md_files:
            encoded = name.replace(" ", "%20")
            lines.append(f"- [{name}]({encoded})")
    else:
        lines.append("_No stored procedures documented yet._")
    lines.append("")

    home_path = wiki_dir / "SQL-Stored-Procedures.md"
    home_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"  Updated SQL-Stored-Procedures.md")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--main-repo",      required=True)
    parser.add_argument("--wiki-repo",      required=True)
    parser.add_argument("--sql-root",       required=True)
    parser.add_argument("--openrouter-key", required=True)
    args = parser.parse_args()

    main_repo = Path(args.main_repo).resolve()
    wiki_dir  = Path(args.wiki_repo).resolve()

    # SQL specific files
    changelog_path = main_repo / "sql_changelog.md"
    processed_path = main_repo / "sql_processed.json"
    backlog_path   = main_repo / "backlog.json"

    changelog = read_changelog(changelog_path)

    if not changelog:
        print("No SQL stored procedure changelog entries found.")
        return

    any_processed = False

    for date, entries in sorted(changelog.items()):
        for path_key, status in entries.items():
            sql_file = main_repo / path_key

            parts = Path(path_key).parts
            if len(parts) < 7:
                print(f"  Skipping malformed path: {path_key}")
                continue

            server    = parts[1]
            database  = parts[2]
            schema    = parts[4]
            procedure = Path(parts[6]).stem
            doc_key   = f"{server}/{database}/{schema}/{procedure}"

            # Always reprocess MODIFIED, skip only CREATED that's already done
            if is_processed(processed_path, doc_key) and status != "MODIFIED":
                print(f"  Already processed {doc_key} — skipping.")
                continue

            print(f"\n[{date}] {status}: {doc_key}")

            try:
                if status in ("CREATED", "MODIFIED"):
                    if not sql_file.exists():
                        print(f"  File not found: {sql_file}")
                        add_to_backlog(backlog_path, doc_key, "File not found")
                        continue

                    sql_text     = sql_file.read_text(encoding="utf-8-sig")
                    meta         = {
                        "server":    server,
                        "database":  database,
                        "schema":    schema,
                        "procedure": procedure,
                    }
                    parameters   = extract_parameters(sql_text)
                    variables    = extract_variables(sql_text)
                    read_tables, write_tables = extract_tables(sql_text)
                    called_procs = extract_called_procedures(sql_text)

                    ai_description = get_ai_description(sql_text, args.openrouter_key)
                    if ai_description is None:
                        print(f"  AI failed for {doc_key} — adding to backlog.")
                        add_to_backlog(backlog_path, doc_key, "AI call failed — all models exhausted")
                        continue

                    markdown = build_markdown(
                        meta=meta,
                        ai_description=ai_description,
                        parameters=parameters,
                        variables=variables,
                        read_tables=read_tables,
                        write_tables=write_tables,
                        called_procs=called_procs,
                    )

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

                mark_processed(processed_path, doc_key)
                remove_from_backlog(backlog_path, doc_key)
                any_processed = True

            except Exception as e:
                print(f"  ERROR processing {doc_key}: {e}")
                add_to_backlog(backlog_path, doc_key, str(e))
                continue

    if any_processed:
        update_sql_home(wiki_dir)
        print("\nSQL-Stored-Procedures.md updated.")
    else:
        print("\nNothing new to process.")


if __name__ == "__main__":
    main()