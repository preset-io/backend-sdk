# Code Review: 5 Fix Commits on `feature/filtered-ops`

**Diff:** `git diff 08d6e8e..HEAD` (547 lines, 7 files)
**Test suite:** 514 passed, 0 failed (99% coverage)

---

## Fix 1: Error handling for delete filter API calls

**Commit:** `6238122`
**Files:** `src/preset_cli/cli/superset/delete.py:256-264`, `src/preset_cli/cli/superset/export.py:431-439`

**Summary:** Wraps `client.get_dashboards()` in a try/except, converting API errors into a `click.ClickException` with a message about unsupported filter keys.

### Issues

- **Severity:** Medium
  **What:** The `except Exception` clause catches every possible exception (network timeout, auth failure, JSON decode error, etc.) and always blames filter key incompatibility. A network error would mislead the user into thinking their filter key is unsupported.
  **Where:** `delete.py:258`, `export.py:433`
  **Suggestion:** Catch a narrower exception type (e.g., `requests.HTTPError`) or at minimum include the original exception message in the output so the user can distinguish a version mismatch from a connectivity failure. Example: `f"... {filter_keys} may not be supported by this Superset version ({exc})."`

- **Severity:** Low
  **What:** Identical error-handling block is duplicated between `delete.py:256-264` and `export.py:431-439` (same message template, same pattern).
  **Where:** `delete.py:256-264`, `export.py:431-439`
  **Suggestion:** Extract a shared helper (e.g., `_fetch_filtered_dashboards(client, parsed_filters)`) in `lib.py` to keep the error message and handling logic in one place. Not urgent since only two call sites exist today.

### Tests

- `test_delete_assets_filter_api_error` — verifies non-zero exit and message substring. Adequate.
- Missing: no equivalent test for the same error path in `export_assets`. The export command has the same try/except but no test exercises it.

---

## Fix 2: Deduplicated UUID extraction via thin wrapper

**Commit:** `0d7175a`
**Files:** `src/preset_cli/cli/superset/delete.py:20-27, 30-65`

**Summary:** `_extract_uuids_from_export` now delegates to `_extract_dependency_maps` instead of duplicating the ZIP-walking logic. Clean DRY improvement.

### Issues

- **Severity:** Low
  **What:** `_extract_dependency_maps` signature line is 96 characters (line 30), exceeding typical 88-char project conventions. Not a linter failure, but inconsistent with the rest of the file.
  **Where:** `delete.py:30`
  **Suggestion:** Break the return type annotation across multiple lines, matching the style used elsewhere in this file (e.g., `_resolve_ids` on line 92).

- **Severity:** Low
  **What:** `_extract_dependency_maps` lacks a docstring, unlike every other private function in the file (`_extract_uuids_from_export`, `_build_uuid_map`, `_resolve_ids`, etc.).
  **Where:** `delete.py:30`
  **Suggestion:** Add a one-line docstring for consistency.

### Tests

No new tests were added for `_extract_dependency_maps` directly, but it is exercised indirectly through multiple existing integration tests (`test_delete_assets_dry_run_shows_cascade_ids`, all cascade/shared-check tests). Coverage is adequate.

---

## Fix 3: Contains operator for partial match filters

**Commit:** `5639bcf`
**Files:** `src/preset_cli/api/operators.py:45-50`, `src/preset_cli/cli/superset/lib.py:14,18,144-148`

**Summary:** Adds a `Contains` operator (`"ct"`) and applies it automatically when the filter key is `dashboard_title`. Downstream, `get_resources()` already polymorphically handles any `Operator` subclass via `isinstance(v, Operator)` at `superset.py:446`, so this works without client changes.

### Issues

None. Clean, minimal implementation. The `CONTAINS_FILTER_KEYS` set makes it easy to add more partial-match fields in the future without modifying the parsing logic.

### Tests

- `test_parse_filters_dashboard_title_uses_contains` — confirms wrapping.
- `test_parse_filters_slug_uses_exact_match` — confirms non-title fields remain raw values.
- `test_parse_filters_value_with_equals` — updated to expect `Contains` wrapper.

Good coverage. One potential edge case not tested: passing `Contains` through to the actual API serialization (i.e., that `prison.dumps` produces `opr:ct`). This is arguably an integration concern and not a unit test gap since `get_resources` already has operator serialization tests.

---

## Fix 4: Itemized cascade targets in dry-run summary

**Commit:** `4b1d5ee`
**Files:** `src/preset_cli/cli/superset/delete.py:136-164`

**Summary:** The dry-run summary now prints each cascade target ID on its own line (e.g., `  - [ID: 5]`) and lists shared-skipped UUIDs inline. Improves auditability before destructive operations.

### Issues

- **Severity:** Low
  **What:** Shared resources are printed as UUIDs (`sorted(shared['charts'])`) while cascade targets are printed as integer IDs (`sorted(chart_ids)`). This is technically correct (shared detection works on UUIDs, deletion works on IDs), but it may confuse users who see UUIDs in one section and integer IDs in another with no way to correlate them.
  **Where:** `delete.py:138-153` vs `delete.py:159-164`
  **Suggestion:** Consider a follow-up to annotate shared entries with their resolved ID (if available) or vice versa. Not blocking.

### Tests

- `test_delete_assets_dry_run_shows_cascade_ids` — confirms `[ID: 1]` appears in output. Adequate for the change.

---

## Fix 5: Removed global mutable state for simple file names

**Commit:** `f8edbbc`
**Files:** `src/preset_cli/cli/superset/export.py:54-91, 451-486, 489-529`

**Summary:** Replaces module-level `SIMPLE_FILE_NAMES_ENABLED` bool and `SIMPLE_FILE_NAME_REGISTRY` dict with a local `simple_name_registry` dict passed explicitly through the call chain. Eliminates global mutable state that could leak across tests or concurrent invocations.

### Issues

- **Severity:** Medium
  **What:** `export_resource` has `simple_name_registry: Optional[Dict[Path, Set[str]]] = None` as a default, and the guard on line 527 is `if simple_file_names and simple_name_registry is not None`. If a caller passes `simple_file_names=True` but forgets to pass a registry, simple filename processing is silently skipped with no warning. The two parameters are coupled but independently optional.
  **Where:** `export.py:498-499, 527`
  **Suggestion:** Either (a) make `simple_name_registry` required when `simple_file_names=True` by raising a `ValueError`, or (b) auto-create a new `{}` registry inside `export_resource` when `simple_file_names=True` and registry is `None`. Option (b) preserves backwards compatibility and is the safer choice.

- **Severity:** Low
  **What:** A blank line was removed between the `assert` statement and the next function definition (line 23-25), breaking PEP 8's two-blank-line convention between top-level definitions.
  **Where:** `export.py:23-25`
  **Suggestion:** Add the blank line back.

### Tests

- Existing tests for `test_export_assets_simple_file_names` and `test_export_assets_simple_file_names_collision` exercise the happy path with the new parameter threading.
- `test_export_assets_output_zip` updated its fake `_fake_export_resource` to accept the new parameters.
- All `mock.call()` assertions updated with `simple_file_names=False, simple_name_registry={}` (or `=None` for the zip path).

Thorough test updates. Missing: no test for the edge case where `simple_file_names=True` but `simple_name_registry=None` (the silent skip scenario from the Medium issue above).

---

## Cross-Cutting Observations

1. **Test coverage is strong.** 514 tests pass, 99% line coverage. Each fix has at least one dedicated test.
2. **Conventions are followed.** Click decorators, docstrings, type annotations, and `pylint: disable` comments are consistent with the existing codebase.
3. **No security issues introduced.** The changes are CLI/API plumbing with no user-input injection vectors.

---

## Summary Table

| # | Fix | Issues | Severities |
|---|-----|--------|------------|
| 1 | Error handling for delete filter API calls | 2 | Medium, Low |
| 2 | Deduplicated UUID extraction | 2 | Low, Low |
| 3 | Contains operator | 0 | — |
| 4 | Itemized cascade targets in dry-run | 1 | Low |
| 5 | Removed global mutable state | 2 | Medium, Low |

---

## Final Verdict

### ✅ **APPROVE**

No Critical or High severity issues. The two Medium issues (broad exception catch obscuring real errors; silent skip when `simple_name_registry=None`) are worth addressing in a follow-up but do not block merge. All tests pass, coverage is excellent, and each fix is a clear improvement over the prior state.
