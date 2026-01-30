# Code Review Findings — `feature/filtered-ops`

**Branch:** `feature/filtered-ops` vs `main`
**Commits:** `02aca4b` (main feature) + `08d6e8e` (test fixes)
**Files:** 11 changed, +2040 -26 lines
**Date:** 2026-01-29
**Reviewers:** Claude Code (Opus 4.5) + Codex (GPT-5.2)

---

## Verdicts

| Reviewer | Verdict | Confidence |
|----------|---------|------------|
| Claude Code | CHANGES REQUESTED | High |
| Codex | CHANGES REQUESTED | Medium |

---

## Blocking Issues

### 1. Global Mutable State for Simple File Names
- **Severity:** High
- **Source:** Claude Code
- **File:** `src/preset_cli/cli/superset/export.py:25-26, 451-452`
- **Problem:** `SIMPLE_FILE_NAMES_ENABLED` and `SIMPLE_FILE_NAME_REGISTRY` are module-level globals mutated via `global`. Creates thread-safety risk, action-at-a-distance coupling, and fragile cleanup (the `finally` block resets globals, but `export_resource` called directly would bypass it).
- **Fix:** Pass `simple_file_names` and a local `registry` dict as parameters through the call chain. Remove all globals.

```python
# BEFORE
SIMPLE_FILE_NAMES_ENABLED = False
SIMPLE_FILE_NAME_REGISTRY: Dict[Path, Set[str]] = {}
# ... mutated via global keyword in export_assets()

# AFTER — pass through params, create registry locally in export_assets:
registry: Dict[Path, Set[str]] = {}
for resource_name in ["database", "dataset", "chart", "dashboard"]:
    export_resource(
        ...,
        simple_file_names=simple_file_names,
        simple_name_registry=registry,
    )
```

### 2. Delete Missing Error Handling for Unsupported Filters
- **Severity:** High
- **Source:** Both reviewers
- **File:** `src/preset_cli/cli/superset/delete.py:263`
- **Problem:** `export-assets` wraps `get_dashboards()` in try/except for older Superset versions that reject certain filter keys. `delete-assets` does not — surfaces raw stack trace instead of user-friendly error.
- **Fix:** Add the same try/except pattern used in export:

```python
# BEFORE
parsed_filters = parse_filters(filters, DASHBOARD_FILTER_KEYS)
dashboards = client.get_dashboards(**parsed_filters)

# AFTER
parsed_filters = parse_filters(filters, DASHBOARD_FILTER_KEYS)
try:
    dashboards = client.get_dashboards(**parsed_filters)
except Exception as exc:
    filter_keys = ", ".join(parsed_filters.keys())
    raise click.ClickException(
        f"Filter key(s) {filter_keys} may not be supported by this "
        "Superset version. Supported fields vary by version."
    ) from exc
```

---

## Recommended Improvements

### 3. `dashboard_title` Filter Uses Exact Match (PRD Says Partial)
- **Severity:** Medium
- **Source:** Codex
- **File:** `src/preset_cli/cli/superset/lib.py:117`
- **Problem:** `parse_filters` coerces all string filters to `Equal()`. PRD specifies `dashboard_title` should support partial/substring matching.
- **Fix:** Add a `Contains` operator (`operator = "ct"`) to `operators.py` and use it for `dashboard_title`:

```python
# operators.py
class Contains(Operator):
    operator = "ct"

# lib.py — in parse_filters:
if key == "dashboard_title":
    parsed[key] = Contains(value)
else:
    parsed[key] = _coerce_filter_value(value, value_type, key)
```

> **Note:** Verify Superset API supports the `ct` operator for dashboard_title.

### 4. Dry-Run Summary Missing Individual Cascade Items
- **Severity:** Medium
- **Source:** Both reviewers
- **File:** `src/preset_cli/cli/superset/delete.py:132-163`
- **Problem:** PRD shows itemized cascade items (chart names, IDs, parent dashboard). Implementation only prints counts. Reduces operator confidence during dry-run.
- **Fix:** Retain resource dicts (not just IDs) during resolution so names/details can be displayed. Or accept as v1 limitation with a comment.

### 5. Duplicated UUID Extraction Logic
- **Severity:** Medium
- **Source:** Claude Code
- **File:** `src/preset_cli/cli/superset/delete.py:20-79`
- **Problem:** `_extract_uuids_from_export` and `_extract_dependency_maps` both iterate the same ZIP and parse the same YAML. `_extract_dependency_maps` is a strict superset.
- **Fix:** Remove `_extract_uuids_from_export`, reuse `_extract_dependency_maps` and discard the maps when not needed:

```python
# BEFORE
protected = _extract_uuids_from_export(other_buf)

# AFTER
p_charts, p_datasets, p_databases, _, _ = _extract_dependency_maps(other_buf)
protected = {"charts": p_charts, "datasets": p_datasets, "databases": p_databases}
```

---

## Debatable / Low Priority

### 6. No Transaction Rollback on Delete Failures
- **Severity:** High (Codex) / Low (PRD Audit)
- **Source:** Codex
- **File:** `src/preset_cli/cli/superset/delete.py:371`
- **Problem:** PRD mentions "transaction rollback on error" but implementation uses continue-on-failure with error reporting. No pre-export backup or re-import.
- **My assessment:** Superset API is per-resource with no server-side transactions. A best-effort rollback (pre-export + re-import on failure) adds complexity and may not fully restore state. Current partial-failure reporting is a reasonable v1 tradeoff. **Decide if this is worth the complexity.**

### 7. Duplicated Dashboard UUID Extraction (Cross-File)
- **Severity:** Low
- **Source:** Claude Code
- **File:** `src/preset_cli/cli/superset/export.py:95-114`
- **Problem:** `_get_dashboard_chart_uuids` / `_get_dashboard_dataset_filter_uuids` duplicate logic from `command.py`. Export versions use defensive `.get()` which is correct for YAML that may have missing keys. Acceptable intentional duplication — add a comment.

### 8. Dict Default for List Field
- **Severity:** Low
- **Source:** Claude Code
- **File:** `src/preset_cli/cli/superset/export.py:111`
- **Problem:** `filter_config.get("targets", {})` — `targets` is a list but default is `{}`. Works but misleading.
- **Fix:** Change to `filter_config.get("targets", [])`

### 9. Glob During Directory Mutation
- **Severity:** Low
- **Source:** Claude Code
- **File:** `src/preset_cli/cli/superset/export.py:147`
- **Problem:** `dashboards_dir.glob("*.yaml")` while renaming files inside the loop. CPython materializes results but behavior isn't guaranteed.
- **Fix:** `list(dashboards_dir.glob("*.yaml"))`

### 10. `.yaml` vs `.yml` Inconsistency
- **Severity:** Low
- **Source:** Claude Code
- **File:** `src/preset_cli/cli/superset/export.py:131,137,143,147`
- **Problem:** Export uses `*.yaml` globs but delete accepts both `.yaml` and `.yml`. Superset uses `.yaml` exclusively so unlikely to cause issues.

### 11. Cascade Import Dependency Overwrite Concern
- **Severity:** Low (Design)
- **Source:** Claude Code
- **File:** `src/preset_cli/cli/superset/sync/native/command.py:393-403`
- **Problem:** With `--no-cascade`, dependencies import with `overwrite=False`, then dashboard imports with its full bundle using `overwrite=True`. May re-overwrite deps depending on Superset API behavior. Add a comment documenting the assumption.

### 12. Line Length Violation
- **Severity:** Low
- **Source:** Claude Code
- **File:** `src/preset_cli/cli/superset/delete.py:44`
- **Fix:** Break the tuple return type annotation across multiple lines.

---

## Positive Highlights

Both reviewers noted strong points:
- ✅ Clean architecture — filter parsing in `lib.py`, delete in `delete.py`, export extensions in `export.py`
- ✅ Safety-first delete — dry-run default, `--confirm=DELETE`, cascade hierarchy enforcement, shared dependency protection
- ✅ Backward compatible — existing `export-assets` invocations work unchanged
- ✅ Comprehensive tests — 13 delete + 12 export + 10 filter + 3 cascade = 38 new tests
- ✅ Defensive coding — `.get()` for YAML access, `managed_externally` → `is_managed_externally` alias
- ✅ Follows established patterns throughout

---

## Action Summary

| # | Issue | Severity | Action |
|---|-------|----------|--------|
| 1 | Global mutable state | 🔴 High | **Fix before merge** |
| 2 | Delete filter error handling | 🔴 High | **Fix before merge** |
| 3 | Partial match for dashboard_title | 🟡 Medium | Recommended |
| 4 | Dry-run itemized output | 🟡 Medium | Recommended |
| 5 | Duplicated UUID extraction | 🟡 Medium | Recommended |
| 6 | Delete rollback | 🟠 Debatable | Decide if worth complexity |
| 7-12 | Various low-severity | 🟢 Low | Optional polish |
