# MetaZcode – PL/SQL Extension & Traceability Fixes

_Authors: Houssam Ibnchakroune & Abdelhafid Kbiri-Alaoui_

---

## 🎯 Goal

- Enable **PL/SQL ingestion** at the same level as existing **SSIS ingestion**.  
- Fix traceability so that the `technology` field reflects the correct framework (SSIS, ORACLE/PLSQL, etc.).  
- Expose **dedicated CLI commands** for PL/SQL analysis.

---

## 🧭 Summary of Changes

### 1) New PL/SQL Ingestion Module
- **New folder**: `metazcode/sdk/ingestion/plsql/`
- **Content**: new Python files inspired by the SSIS ingestion, adapted for PL/SQL parsing.
- **Main class**: `PlsqlLoader` (returns `(nodes, edges)` batches for the graph).
- **Parser**: `sqlglot`

### 2) Traceability Fix
File: `metazcode/sdk/models/traceability.py`

- Added a `technology` parameter in:
  - `create_node_traceability(...)`
  - `create_edge_traceability(...)`
- Removed the hardcoded `"SSIS"`.
- Default value remains `"SSIS"` for backward compatibility.

**Result**:  
- SSIS continues to use `"SSIS"` with no changes.  
- PL/SQL calls the method with `technology="ORACLE"` → nodes/edges now reflect the correct technology.  
- Ready for future frameworks (e.g., `"SPARK"`, `"PYTHON"`, etc.).

### 3) CLI – New PL/SQL Commands
File: `metazcode/cli/commands.py`

- **Import added**:
  ```python
  from metazcode.sdk.ingestion.plsql import PlsqlLoader
  
- **New commands**:

  `plsql-ingest → PL/SQL-only ingestion`
  
  `plsql-full → PL/SQL ingestion + analysis + graph export`
  
  Existing commands (ingest, analyze, dump, visualize, full, etc.) remain unchanged for SSIS.

## 🧪 Usage Examples

### A) PL/SQL Ingestion Only
```bash
metazcode plsql-ingest --path ./examples/plsql
```
# Expected output
[PLSQL] Ingestion complete: added <N> nodes, <M> edges

### B) Complete PL/SQL Workflow (ingest + analysis)
```bash
metazcode plsql-full --path ./examples/plsql --output analysis_plsql.json
```
# Generated files
enhanced_graph_plsql.json → exported graph

analysis_plsql.json → analysis results (if --output is provided)

### C) SSIS Example (unchanged)
```bash
metazcode full --path ./examples/ssis --output analysis_ssis.json
```

## 🗂️ Project Structure (excerpt)
```bash
metazcode/
  cli/
    commands.py                # + plsql-ingest / plsql-full commands
  sdk/
    ingestion/
      ssis/                    # existing
      plsql/                   # ✅ new PL/SQL ingestion module
    models/
      traceability.py          # ✅ added technology parameter
```

## 🚀 How to Test

### 1) Install project dependencies
- Created new virtual environment named tmp_venv
- Activated the environment
- Installed the metazensecode package in development mode with all dependencies from **pyproject.toml**
```bash
pip install -e .
```
### 2) PL/SQL Ingestion
```bash
metazcode plsql-ingest --path /path/to/plsql
```
### 3) PL/SQL Analysis
```bash
metazcode plsql-full --path /path/to/plsql --output analysis_plsql.json
```
### 4) Verify in enhanced_graph_plsql.json
"technology": "ORACLE"






