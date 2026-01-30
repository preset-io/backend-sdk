# Medium Issues Analysis

## ISSUE A: Broad `except Exception` in filter error handler
- **Verdict:** MINIMAL FIX
- **Reasoning:** `parse_filters` already validates keys, so the only expected failure is the server rejecting otherwise valid filter columns for a specific Superset version. However, `client.get_dashboards()` can also raise `SupersetError` for auth/permission problems or `requests` exceptions for network issues; the current `except Exception` masks these as “unsupported filter keys,” which is misleading. Other CLI flows typically let `SupersetError` surface or show its message, so narrowing this path to filter-related errors is consistent and safer.
- **If fixing:** Replace the broad catch with a `SupersetError`-only branch and only convert to the filter-specific ClickException when the error looks filter-related; otherwise re-raise. Apply the same change in both `export.py` and `delete.py`.
  ```python
  # add import
  from preset_cli.exceptions import SupersetError

  try:
      dashboards = client.get_dashboards(**parsed_filters)
  except SupersetError as exc:
      # only rewrite errors that look filter-related
      messages = " ".join(error.get("message", "") for error in exc.errors)
      if "filter" in messages.lower() or "column" in messages.lower():
          filter_keys = ", ".join(parsed_filters.keys())
          raise click.ClickException(
              "Filter key(s) "
              f"{filter_keys} may not be supported by this Superset version. "
              "Supported fields vary by version.",
          ) from exc
      raise
  ```

## ISSUE B: Silent skip when `simple_name_registry=None`
- **Verdict:** DON'T FIX
- **Reasoning:** `export_resource` is only called within `export_assets`, and that call always passes `simple_file_names` and `simple_name_registry` together. There are no other callers in the codebase, and the optional registry parameter exists to preserve backwards compatibility for older internal call sites. Adding a guard or error here would add friction without a real-world caller to trigger it.
- **If fixing:** N/A
