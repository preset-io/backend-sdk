# Fix Proposals for `feature/filtered-ops` Code Review Findings

**Branch:** `feature/filtered-ops`
**Date:** 2026-01-30
**Reviewer:** Claude Opus 4.5

---

## ISSUE 1 (BLOCKING): Global Mutable State for Simple File Names

### Analysis

**File:** `src/preset_cli/cli/superset/export.py`

Two module-level globals control "simple file names" behavior:

```python
# Lines 25-26
SIMPLE_FILE_NAMES_ENABLED = False
SIMPLE_FILE_NAME_REGISTRY: Dict[Path, Set[str]] = {}
```

These are mutated/read in four places:

| Location | Line(s) | Action |
|----------|---------|--------|
| `export_assets()` | 450-452 | Clears registry, sets `global SIMPLE_FILE_NAMES_ENABLED = simple_file_names` |
| `export_assets()` finally block | 481-482 | Resets `SIMPLE_FILE_NAMES_ENABLED = False`, clears registry |
| `_unique_simple_name()` | 63 | Reads/writes `SIMPLE_FILE_NAME_REGISTRY` via `.setdefault()` |
| `export_resource()` | 522-523 | Reads `SIMPLE_FILE_NAMES_ENABLED` to decide branch |

No other module imports these globals (confirmed by grep).

**Why it's a problem:**
- Thread-unsafe: concurrent CLI invocations (e.g., from a test runner or programmatic usage) would race on shared mutable state.
- Action-at-a-distance: `export_resource()` and `_unique_simple_name()` silently depend on state set elsewhere, making them hard to reason about and test in isolation.
- The `global` keyword is a Python anti-pattern — it obscures data flow.

### Proposed Fix

Pass `simple_file_names: bool` and a local `registry: Dict[Path, Set[str]]` as parameters through the call chain. Remove both globals.

#### Step 1: Remove globals, update `_unique_simple_name` signature

**Before** (lines 25-26, 59-76):
```python
SIMPLE_FILE_NAMES_ENABLED = False
SIMPLE_FILE_NAME_REGISTRY: Dict[Path, Set[str]] = {}

# ...

def _unique_simple_name(parent: Path, file_name: str) -> str:
    used = SIMPLE_FILE_NAME_REGISTRY.setdefault(parent, set())
    if file_name not in used:
        used.add(file_name)
        return file_name
    # ... collision handling
```

**After:**
```python
# Lines 25-26: DELETED (both globals removed)

def _unique_simple_name(
    parent: Path,
    file_name: str,
    registry: Dict[Path, Set[str]],
) -> str:
    used = registry.setdefault(parent, set())
    if file_name not in used:
        used.add(file_name)
        return file_name
    # ... collision handling (unchanged)
```

#### Step 2: Update `apply_simple_filename` signature

**Before** (lines 79-87):
```python
def apply_simple_filename(file_name: str, root: Path) -> str:
    path = Path(file_name)
    simple_name = simplify_filename(path.name)
    parent = root / path.parent
    unique_name = _unique_simple_name(parent, simple_name)
    return str(path.parent / unique_name) if path.parent != Path(".") else unique_name
```

**After:**
```python
def apply_simple_filename(
    file_name: str,
    root: Path,
    registry: Dict[Path, Set[str]],
) -> str:
    path = Path(file_name)
    simple_name = simplify_filename(path.name)
    parent = root / path.parent
    unique_name = _unique_simple_name(parent, simple_name, registry)
    return str(path.parent / unique_name) if path.parent != Path(".") else unique_name
```

#### Step 3: Update `export_resource` to accept new parameters

**Before** (lines 487-524):
```python
def export_resource(  # pylint: disable=too-many-arguments, too-many-locals
    resource_name: str,
    requested_ids: Set[int],
    root: Path,
    client: SupersetClient,
    overwrite: bool,
    disable_jinja_escaping: bool,
    skip_related: bool = True,
    force_unix_eol: bool = False,
) -> None:
    # ...
        output_name = (
            apply_simple_filename(file_name, root)
            if SIMPLE_FILE_NAMES_ENABLED
            else file_name
        )
```

**After:**
```python
def export_resource(  # pylint: disable=too-many-arguments, too-many-locals
    resource_name: str,
    requested_ids: Set[int],
    root: Path,
    client: SupersetClient,
    overwrite: bool,
    disable_jinja_escaping: bool,
    skip_related: bool = True,
    force_unix_eol: bool = False,
    simple_file_names: bool = False,
    simple_name_registry: Optional[Dict[Path, Set[str]]] = None,
) -> None:
    # ...
        output_name = (
            apply_simple_filename(file_name, root, simple_name_registry)
            if simple_file_names and simple_name_registry is not None
            else file_name
        )
```

> **Design note:** Using `Optional` with defaults keeps backward compatibility — all existing direct callers of `export_resource()` continue to work without changes. Only `export_assets()` passes the new params.

#### Step 4: Update `export_assets` — create local registry, pass it down, remove global manipulation

**Before** (lines 450-484):
```python
    SIMPLE_FILE_NAME_REGISTRY.clear()
    global SIMPLE_FILE_NAMES_ENABLED
    SIMPLE_FILE_NAMES_ENABLED = simple_file_names

    temp_root = None
    if output_zip:
        temp_root = tempfile.TemporaryDirectory()
        root = Path(temp_root.name)

    try:
        for resource_name in ["database", "dataset", "chart", "dashboard"]:
            if ...:
                export_resource(
                    resource_name,
                    ids[resource_name],
                    root,
                    client,
                    overwrite,
                    disable_jinja_escaping,
                    skip_related=not ids_requested,
                    force_unix_eol=force_unix_eol,
                )
        # ...
    finally:
        SIMPLE_FILE_NAMES_ENABLED = False
        SIMPLE_FILE_NAME_REGISTRY.clear()
        if temp_root:
            temp_root.cleanup()
```

**After:**
```python
    simple_name_registry: Dict[Path, Set[str]] = {}

    temp_root = None
    if output_zip:
        temp_root = tempfile.TemporaryDirectory()
        root = Path(temp_root.name)

    try:
        for resource_name in ["database", "dataset", "chart", "dashboard"]:
            if ...:
                export_resource(
                    resource_name,
                    ids[resource_name],
                    root,
                    client,
                    overwrite,
                    disable_jinja_escaping,
                    skip_related=not ids_requested,
                    force_unix_eol=force_unix_eol,
                    simple_file_names=simple_file_names,
                    simple_name_registry=simple_name_registry,
                )
        # ...
    finally:
        if temp_root:
            temp_root.cleanup()
```

### Affected Tests

**`tests/cli/superset/export_test.py`:**

Tests fall into two categories:

**A) Tests that call `export_resource()` directly** — these use default kwargs, so the new optional params don't break them. No changes required:
- `test_export_resource` (line 165)
- `test_export_resource_overwrite` (line 280)
- `test_export_resource_with_ids` (line 329)
- `test_export_resource_jinja_escaping_disabled` (line 1055)
- `test_export_resource_force_unix_eol_enabled` (line 1172)
- `test_export_resource_uuid_validation` (line 1342)
- `test_export_resource_uuid_with_overwrite` (line 1391)
- `test_export_resource_uuid_with_overrides` (line 1447)
- `test_export_resource_backward_compatibility` (line 1505)
- `test_export_resource_deletion_failure_with_unlink` (line 1806)
- `test_export_resource_mixed_uuid_conflicts` (line 1868)

**B) Tests that mock `export_resource` and assert call args** — these need the mock assertions updated to include the two new kwargs. Affected tests:
- `test_export_assets` (line 357) — 4 `mock.call()` assertions need `simple_file_names=False, simple_name_registry={}` added
- `test_export_assets_by_id` (line 423) — same
- `test_export_assets_by_type` (line 465) — same
- `test_export_with_custom_auth` (line 519) — same
- `test_export_resource_jinja_escaping_disabled_command` (line 1098) — same
- `test_export_resource_force_unix_eol_command` (line 1202) — same
- `test_export_assets_with_filter` (line 2082) — same
- `test_export_assets_with_multiple_filters` (line 2123) — same

**C) Tests that go through CLI runner with simple-file-names** — these exercise the full flow and should continue working with no changes:
- `test_export_assets_simple_file_names` (line 2344)
- `test_export_assets_simple_file_names_collision` (line 2381)
- `test_export_assets_per_asset_folder_with_simple_names` (line 2469)

### Confidence: **HIGH**

Mechanical refactoring. Linear data flow. All consumers are within `export.py`. No external module imports these globals.

---

## ISSUE 2 (BLOCKING): Delete Missing Error Handling for Unsupported Filters

### Analysis

**File:** `src/preset_cli/cli/superset/delete.py`

In `export.py` (lines 427-435), the filter-based `get_dashboards()` call is wrapped:

```python
# export.py lines 427-435
try:
    dashboards = client.get_dashboards(**parsed_filters)
except Exception as exc:
    filter_keys = ", ".join(parsed_filters.keys())
    raise click.ClickException(
        "Filter key(s) "
        f"{filter_keys} may not be supported by this Superset version. "
        "Supported fields vary by version.",
    ) from exc
```

In `delete.py` (lines 263-264), the equivalent call is **unprotected**:

```python
# delete.py lines 263-264
parsed_filters = parse_filters(filters, DASHBOARD_FILTER_KEYS)
dashboards = client.get_dashboards(**parsed_filters)
```

On older Superset versions that don't support certain filter keys, this produces a raw stack trace.

### Audit of ALL `get_*()` Calls in delete.py

| Line | Call | Uses parsed filters? | Needs wrapping? |
|------|------|---------------------|-----------------|
| 264 | `client.get_dashboards(**parsed_filters)` | **Yes** | **Yes** |
| 277 | `client.export_zip("dashboard", list(dashboard_ids))` | No (IDs) | No |
| 294 | `client.get_resources("dashboard")` | No (no filters) | No |
| 297 | `client.export_zip("dashboard", list(other_ids))` | No (IDs) | No |
| 335-349 | `_resolve_ids()` → `client.get_resources(...)` | No (no filters) | No |
| 400 | `client.delete_resource(...)` | No (IDs) | No |

**Only the `get_dashboards()` call at line 264 uses parsed filters.**

### Proposed Fix

**Before** (`delete.py` lines 263-267):
```python
    parsed_filters = parse_filters(filters, DASHBOARD_FILTER_KEYS)
    dashboards = client.get_dashboards(**parsed_filters)
    if not dashboards:
        click.echo("No dashboards match the specified filters.")
        return
```

**After:**
```python
    parsed_filters = parse_filters(filters, DASHBOARD_FILTER_KEYS)
    try:
        dashboards = client.get_dashboards(**parsed_filters)
    except Exception as exc:  # pylint: disable=broad-except
        filter_keys = ", ".join(parsed_filters.keys())
        raise click.ClickException(
            "Filter key(s) "
            f"{filter_keys} may not be supported by this Superset version. "
            "Supported fields vary by version.",
        ) from exc
    if not dashboards:
        click.echo("No dashboards match the specified filters.")
        return
```

### Affected Tests

**`tests/cli/superset/delete_test.py`:**

Existing tests are unaffected — they all mock `client.get_dashboards` to return data successfully.

Add one new test:

```python
def test_delete_assets_filter_api_error(mocker: MockerFixture) -> None:
    """
    Test delete assets handles filter API error gracefully.
    """
    SupersetClient = mocker.patch("preset_cli.cli.superset.delete.SupersetClient")
    client = SupersetClient()
    client.get_dashboards.side_effect = Exception("400 Bad Request")
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "delete-assets",
            "--asset-type",
            "dashboard",
            "--filter",
            "dashboard_title=test",
        ],
    )
    assert result.exit_code != 0
    assert "may not be supported" in result.output
```

### Confidence: **HIGH**

Direct parity fix with `export.py`. Single call site. Clear behavior.

---

## ISSUE 3 (RECOMMENDED): `dashboard_title` Filter Uses Exact Match (PRD Says Partial)

### Analysis

**Files:** `src/preset_cli/cli/superset/lib.py`, `src/preset_cli/api/operators.py`

`parse_filters()` (lib.py:117-144) returns raw values for all filter keys. In `SupersetClient.get_resources()` (superset.py:445-446):

```python
operations = {
    k: v if isinstance(v, Operator) else Equal(v) for k, v in kwargs.items()
}
```

Non-Operator values are wrapped in `Equal(v)`, sending `opr: "eq"` to the Superset API. So `--filter dashboard_title=Sales` becomes exact match.

**The Superset FAB API supports these filter operators** (relevant subset):
- `eq` — equals (exact match)
- `ct` — contains (substring match)
- `sw` — starts with
- `ew` — ends with

The existing operator hierarchy in `operators.py`:

```python
class Operator:        # operator = "invalid"
class Equal(Operator): # operator = "eq"
class OneToMany(Operator): # operator = "rel_o_m"
class In(Operator):    # operator = "in"
```

A `Contains` operator with `operator = "ct"` fits cleanly.

### Proposed Fix

#### Step 1: Add `Contains` operator

**File:** `src/preset_cli/api/operators.py`

**Before** (lines 37-43):
```python
class In(Operator):
    """
    Operator for In filters.
    """

    operator = "in"
```

**After** (append after `In`):
```python
class In(Operator):
    """
    Operator for In filters.
    """

    operator = "in"


class Contains(Operator):
    """
    Operator for substring/contains filters.
    """

    operator = "ct"
```

#### Step 2: Use `Contains` for `dashboard_title` in `parse_filters`

**Design choice:** Apply `Contains` only to `dashboard_title`. Rationale:
- `slug` is an identifier — exact match expected
- `certified_by` is a name field — typically matched exactly
- `id` is numeric — exact match
- `is_managed_externally` is boolean — exact match
- `dashboard_title` is the only field where "find dashboards containing X" is the natural user intent

**File:** `src/preset_cli/cli/superset/lib.py`

**Before** (lines 1-2 imports, then lines 141-143):
```python
from preset_cli.lib import dict_merge

# ... (in parse_filters)
        value_type = allowed_keys[key]
        parsed[key] = _coerce_filter_value(value, value_type, key)

    return parsed
```

**After:**
```python
from preset_cli.api.operators import Contains
from preset_cli.lib import dict_merge

# Add module-level constant
CONTAINS_FILTER_KEYS = {"dashboard_title"}

# ... (in parse_filters)
        value_type = allowed_keys[key]
        coerced = _coerce_filter_value(value, value_type, key)
        if key in CONTAINS_FILTER_KEYS:
            parsed[key] = Contains(coerced)
        else:
            parsed[key] = coerced

    return parsed
```

**Note:** Non-Contains values remain raw (not wrapped in `Equal`). `get_resources()` already wraps non-Operator values in `Equal()` at line 446 of superset.py, so we only explicitly wrap values where we want a *different* operator.

### Affected Tests

**`tests/cli/superset/lib_test.py`:**

Update `test_parse_filters_value_with_equals` (line 312):

**Before:**
```python
def test_parse_filters_value_with_equals() -> None:
    assert parse_filters(("dashboard_title=A=B",), DASHBOARD_FILTER_KEYS) == {
        "dashboard_title": "A=B",
    }
```

**After:**
```python
from preset_cli.api.operators import Contains

def test_parse_filters_value_with_equals() -> None:
    result = parse_filters(("dashboard_title=A=B",), DASHBOARD_FILTER_KEYS)
    assert isinstance(result["dashboard_title"], Contains)
    assert result["dashboard_title"].value == "A=B"
```

Add new tests:
```python
def test_parse_filters_dashboard_title_uses_contains() -> None:
    result = parse_filters(("dashboard_title=Sales",), DASHBOARD_FILTER_KEYS)
    assert isinstance(result["dashboard_title"], Contains)
    assert result["dashboard_title"].value == "Sales"

def test_parse_filters_slug_uses_exact_match() -> None:
    result = parse_filters(("slug=test",), DASHBOARD_FILTER_KEYS)
    assert result["slug"] == "test"  # raw value, not an Operator
```

**`tests/cli/superset/export_test.py`** and **`tests/cli/superset/delete_test.py`:**

Tests in these files use `slug=test` as their filter, not `dashboard_title`. The mock assertions like `client.get_dashboards.assert_called_once_with(slug="test")` remain correct because `slug` is still passed as a raw value. **No changes needed.**

**`tests/api/operators_test.py`** (if exists, add):
```python
def test_contains_operator() -> None:
    op = Contains("hello")
    assert op.operator == "ct"
    assert op.value == "hello"
```

### Confidence: **HIGH**

The Superset FAB API `ct` operator is well-documented. The `Operator` class hierarchy makes this trivial. The scope of change is well-contained. If an older Superset version doesn't support `ct`, the error handling from Issue 2 will produce a friendly message.

---

## ISSUE 4 (RECOMMENDED): Dry-Run Summary Missing Individual Cascade Items

### Analysis

**File:** `src/preset_cli/cli/superset/delete.py`

`_echo_summary()` (lines 132-178) currently outputs:

```
Dashboards (2):
  - [ID: 1] Sales Dashboard (slug: sales)
  - [ID: 2] Marketing Dashboard (slug: marketing)

Charts (5):

Datasets (3):

Databases (1):
```

Dashboards are itemized via `_format_dashboard_summary()`, but charts/datasets/databases only show counts. For a destructive cascade operation, operators need to see exactly which items will be deleted.

**What data is available at summary time?**

At the point `_echo_summary` is called (line 367 for dry-run, line 385 for execution):
- `dashboards`: `List[Dict[str, Any]]` — full dashboard objects with titles/slugs (**rich data**)
- `chart_ids`: `Set[int]` — resolved numeric IDs only
- `dataset_ids`: `Set[int]` — resolved numeric IDs only
- `database_ids`: `Set[int]` — resolved numeric IDs only

We do **not** have names/titles for cascade items because `_resolve_ids()` discards them during UUID→ID resolution. The names *are* available in the export ZIP (parsed by `_extract_dependency_maps`), but aren't currently captured.

**I disagree with the review's direction to add full name lookups at this stage.** The simplest high-value change is listing IDs, which are already available and sufficient for operator verification. Adding name extraction from the ZIP (modifying `_extract_dependency_maps` return type) is a larger change that should be separate.

### Proposed Fix (Pragmatic — list IDs)

The `_echo_summary` signature is unchanged. We just add iteration over the existing ID sets.

**Before** (`delete.py` lines 150-163):
```python
    if cascade_flags["charts"]:
        click.echo(f"\nCharts ({len(chart_ids)}):")
    else:
        click.echo("\nCharts (0): (not cascading)")

    if cascade_flags["datasets"]:
        click.echo(f"\nDatasets ({len(dataset_ids)}):")
    else:
        click.echo("\nDatasets (0): (not cascading)")

    if cascade_flags["databases"]:
        click.echo(f"\nDatabases ({len(database_ids)}):")
    else:
        click.echo("\nDatabases (0): (not cascading)")
```

**After:**
```python
    if cascade_flags["charts"]:
        click.echo(f"\nCharts ({len(chart_ids)}):")
        for cid in sorted(chart_ids):
            click.echo(f"  - [ID: {cid}]")
    else:
        click.echo("\nCharts (0): (not cascading)")

    if cascade_flags["datasets"]:
        click.echo(f"\nDatasets ({len(dataset_ids)}):")
        for did in sorted(dataset_ids):
            click.echo(f"  - [ID: {did}]")
    else:
        click.echo("\nDatasets (0): (not cascading)")

    if cascade_flags["databases"]:
        click.echo(f"\nDatabases ({len(database_ids)}):")
        for bid in sorted(database_ids):
            click.echo(f"  - [ID: {bid}]")
    else:
        click.echo("\nDatabases (0): (not cascading)")
```

Also improve the shared section (lines 166-172):

**Before:**
```python
    if any(shared.values()):
        click.echo("\nShared (skipped):")
        if shared["charts"]:
            click.echo(f"  Charts: {len(shared['charts'])}")
        if shared["datasets"]:
            click.echo(f"  Datasets: {len(shared['datasets'])}")
        if shared["databases"]:
            click.echo(f"  Databases: {len(shared['databases'])}")
```

**After:**
```python
    if any(shared.values()):
        click.echo("\nShared (skipped):")
        if shared["charts"]:
            click.echo(f"  Charts ({len(shared['charts'])}): {', '.join(sorted(shared['charts']))}")
        if shared["datasets"]:
            click.echo(f"  Datasets ({len(shared['datasets'])}): {', '.join(sorted(shared['datasets']))}")
        if shared["databases"]:
            click.echo(f"  Databases ({len(shared['databases'])}): {', '.join(sorted(shared['databases']))}")
```

This shows UUIDs for shared items (which is what's available — the shared sets contain UUID strings).

### Affected Tests

**`tests/cli/superset/delete_test.py`:**

Existing tests use `in` containment checks on output, so they're resilient:
- `test_delete_assets_dry_run` checks `"no changes will be made"` — still passes
- `test_delete_assets_shared_dep_skipped` checks `"shared (skipped)"` — still passes

Add one new test:
```python
def test_delete_assets_dry_run_shows_cascade_ids(mocker: MockerFixture) -> None:
    SupersetClient = mocker.patch("preset_cli.cli.superset.delete.SupersetClient")
    client = SupersetClient()
    client.get_dashboards.return_value = [{"id": 1}]
    client.export_zip.return_value = make_export_zip()
    client.get_resources.side_effect = [
        [{"id": 1, "uuid": "chart-uuid"}],
    ]
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "delete-assets",
            "--asset-type", "dashboard",
            "--filter", "slug=test",
            "--cascade-charts",
            "--skip-shared-check",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert "[ID: 1]" in result.output
```

### Confidence: **HIGH**

Output-only change. No behavioral change to deletion logic. Function signature unchanged. All call sites unaffected.

---

## ISSUE 5 (RECOMMENDED): Duplicated UUID Extraction Logic

### Analysis

**File:** `src/preset_cli/cli/superset/delete.py`

Two functions iterate the same ZIP:

**`_extract_uuids_from_export`** (lines 20-41): Returns `Dict[str, Set[str]]` — just UUID sets.

**`_extract_dependency_maps`** (lines 44-79): Returns UUID sets **plus** dependency maps (`chart_dataset_map`, `dataset_database_map`).

They are called in different contexts:
- `_extract_dependency_maps` on line 278: for **target** dashboards (builds cascade dependency info)
- `_extract_uuids_from_export` on line 298: for **other** dashboards (identifies shared/protected assets)

Since `_extract_dependency_maps` collects a superset of the data, `_extract_uuids_from_export` is redundant.

### Behavioral Difference Analysis

`_extract_dependency_maps` returns slightly **more** UUIDs than `_extract_uuids_from_export`:
- `dataset_uuids` includes UUIDs referenced by `dataset_uuid` fields in chart YAMLs (line 62)
- `database_uuids` includes UUIDs referenced by `database_uuid` fields in dataset YAMLs (line 68)

For the "protected assets" use case, this means we'd protect *more* items (transitive dependencies). This is the **safer direction** for a destructive operation — slightly more conservative is better than accidentally deleting shared resources.

### Proposed Fix (Thin Wrapper — Recommended)

Replace the body of `_extract_uuids_from_export` with a delegation to `_extract_dependency_maps`, keeping the function name and call site unchanged.

**Before** (`delete.py` lines 20-41):
```python
def _extract_uuids_from_export(buf: BytesIO) -> Dict[str, Set[str]]:
    uuids = {"charts": set(), "datasets": set(), "databases": set()}
    with ZipFile(buf) as bundle:
        for file_name in bundle.namelist():
            relative = remove_root(file_name)
            if not relative.endswith((".yaml", ".yml")):
                continue
            if not (
                relative.startswith("charts/")
                or relative.startswith("datasets/")
                or relative.startswith("databases/")
            ):
                continue
            config = yaml.load(bundle.read(file_name), Loader=yaml.SafeLoader) or {}
            if uuid := config.get("uuid"):
                if relative.startswith("charts/"):
                    uuids["charts"].add(uuid)
                elif relative.startswith("datasets/"):
                    uuids["datasets"].add(uuid)
                elif relative.startswith("databases/"):
                    uuids["databases"].add(uuid)
    return uuids
```

**After:**
```python
def _extract_uuids_from_export(buf: BytesIO) -> Dict[str, Set[str]]:
    """Extract UUID sets from a dashboard export ZIP."""
    chart_uuids, dataset_uuids, database_uuids, _, _ = _extract_dependency_maps(buf)
    return {
        "charts": chart_uuids,
        "datasets": dataset_uuids,
        "databases": database_uuids,
    }
```

This:
- Eliminates ~20 lines of duplicated ZIP iteration logic
- Keeps the call site on line 298 unchanged: `protected = _extract_uuids_from_export(other_buf)`
- Preserves the function's semantic name
- Is slightly more protective (includes transitive UUIDs) — safer for destructive operations

### Alternative: Full Removal

If you prefer to remove `_extract_uuids_from_export` entirely, the call site changes:

**Before** (line 298):
```python
            protected = _extract_uuids_from_export(other_buf)
```

**After:**
```python
            (
                protected_chart_uuids,
                protected_dataset_uuids,
                protected_database_uuids,
                _,
                _,
            ) = _extract_dependency_maps(other_buf)
            protected = {
                "charts": protected_chart_uuids,
                "datasets": protected_dataset_uuids,
                "databases": protected_database_uuids,
            }
```

I recommend the **thin wrapper** approach for cleanliness, but either works.

### Affected Tests

**`tests/cli/superset/delete_test.py`:**

- `test_delete_assets_shared_dep_skipped` (line 408) — exercises the shared dependency path. With the wrapper approach, behavior is preserved. No test changes needed.

No other tests directly reference `_extract_uuids_from_export`.

### Confidence: **HIGH** (wrapper) / **MEDIUM** (full removal due to slight behavioral change)

---

## Summary Table

| Issue | Severity | Files Changed | Functions Changed | Test Impact | Confidence |
|-------|----------|---------------|-------------------|-------------|------------|
| 1 — Global mutable state | BLOCKING | `export.py` | `_unique_simple_name`, `apply_simple_filename`, `export_resource`, `export_assets` | ~8 mock assertion updates; direct callers unaffected (default params) | HIGH |
| 2 — Delete error handling | BLOCKING | `delete.py` | `delete_assets` | 1 new test | HIGH |
| 3 — Contains operator | RECOMMENDED | `operators.py`, `lib.py` | New `Contains` class, `parse_filters` | 1 test updated, 2-3 new tests | HIGH |
| 4 — Itemized dry-run | RECOMMENDED | `delete.py` | `_echo_summary` | 1 new test | HIGH |
| 5 — Duplicated UUID logic | RECOMMENDED | `delete.py` | `_extract_uuids_from_export` (body replaced) | None | HIGH |

### Recommended Implementation Order

1. **Issue 2** — Smallest fix, highest urgency, zero side effects. Add one try/except.
2. **Issue 5** — Small refactor, eliminates duplication. No API surface changes.
3. **Issue 3** — New operator + parse_filters change. Independent of others.
4. **Issue 4** — Output-only enhancement. Independent of others.
5. **Issue 1** — Largest change with most test updates. Do last to minimize merge conflict risk with other changes.

Issues 2-5 can be done in any order. Issue 1 should be last because it touches the most files and test assertions.
