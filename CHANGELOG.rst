=========
Changelog
=========

Version 0.2.0 - 2023-05-23
==========================

- Column descriptions and labels are now synced from dbt models (`PR #72 <https://github.com/preset-io/backend-sdk/pull/72>`_, `PR #111 <https://github.com/preset-io/backend-sdk/pull/111>`_, `PR #195 <https://github.com/preset-io/backend-sdk/pull/195>`_ and `PR #197 <https://github.com/preset-io/backend-sdk/pull/197>`_).
- CLI can now provision users directly to the team (doesn't require accepting the invitation) with the ``import-users`` command. The ``export-users`` command can now also be used with Preset Workspaces.  (`PR #74 <https://github.com/preset-io/backend-sdk/pull/74>`_, `PR #100 <https://github.com/preset-io/backend-sdk/pull/100>`_ and `PR #148 <https://github.com/preset-io/backend-sdk/pull/148>`_).
- It's possible to export roles information using the ``export-roles`` command (`PR #75 <https://github.com/preset-io/backend-sdk/pull/75>`_ and `PR #161 <https://github.com/preset-io/backend-sdk/pull/161>`_). 
- Exported roles information can be imported via the ``import-roles`` command (`PR #76 <https://github.com/preset-io/backend-sdk/pull/76>`_, `PR #167 <https://github.com/preset-io/backend-sdk/pull/167>`_ and `PR #179 <https://github.com/preset-io/backend-sdk/pull/179>`_).
- Improved session object logic (`PR #77 <https://github.com/preset-io/backend-sdk/pull/77>`_). 
- Improved export/import logic for owernship and role information (`PR #79 <https://github.com/preset-io/backend-sdk/pull/79>`_).
- CLI can now add users to imported roles (`PR #81 <https://github.com/preset-io/backend-sdk/pull/81>`_).
- A JWT token can now be passed for authentication (`PR #82 <https://github.com/preset-io/backend-sdk/pull/82>`_).
- Added debug logging to API requests (`PR #83 <https://github.com/preset-io/backend-sdk/pull/83>_`).
- CLI can now export specific asset types, using the ``--asset-type`` flag (`PR #84 <https://github.com/preset-io/backend-sdk/pull/84>`_).
- CLI can now export specific assets only, using the ``--$asset_type-ids`` (for example ``--dashboard-ids``) flag (`PR #85 <https://github.com/preset-io/backend-sdk/pull/85>`_ and `PR #88 <https://github.com/preset-io/backend-sdk/pull/88>`_).
- CLI can now authenticate to Superset (On Premises) without CSRF token (`PR #87 <https://github.com/preset-io/backend-sdk/pull/87>`_).
- Workspace/Team prompt no longer happens in case ``--help`` was pased (`PR #89 <https://github.com/preset-io/backend-sdk/pull/89>`_).
- Team Roles, Workspace Roles and DARs can now be synced to a Preset team based on a YAML file (`PR #90 <https://github.com/preset-io/backend-sdk/pull/90>`_).
- Added ``--version`` command to display the installed version (`PR #91 <https://github.com/preset-io/backend-sdk/pull/91>`_).
- Fixed parent/child node selection in dbt Core for proper graph selection (`PR #92 <https://github.com/preset-io/backend-sdk/pull/92>`_).
- Improved logging for the dbt Client (`PR #94 <https://github.com/preset-io/backend-sdk/pull/94>`_).
- CLI now can create datasets for different databases (for DB Engines that supports multiple databases like Snowflake, BigQuery, etc) (`PR #95 <https://github.com/preset-io/backend-sdk/pull/95>`_).
- BQ connection can now successfully be created/updated from the ``profiles.yml`` information (`PR #96 <https://github.com/preset-io/backend-sdk/pull/96>`_).
- Redshift connectons now get created with the ``redshift+psycopg2`` driver (`PR #97 <https://github.com/preset-io/backend-sdk/pull/97>`_).
- YAML files outside of asset folders aren't imported in the native sync (`PR #99 <https://github.com/preset-io/backend-sdk/pull/99>`_).
- Improved BQ DB detection (`PR #102 <https://github.com/preset-io/backend-sdk/pull/102>`_).
- Reduced the maximum amount of files included in an export file (`PR #105 <https://github.com/preset-io/backend-sdk/pull/105>`_).
- Workspaces can now be defined as environment variables (`PR #106 <https://github.com/preset-io/backend-sdk/pull/106>`_).
- CLI can now create Snowflake connections authenticated via private key pair (`PR #108 <https://github.com/preset-io/backend-sdk/pull/108>`_).
- Improved the ``--exclude`` filter for the dbt sync (`PR #109 <https://github.com/preset-io/backend-sdk/pull/109>`_).
- Improved database connection logic (`PR #111 <https://github.com/preset-io/backend-sdk/pull/111>`_).
- CLI can now create Snowflake connections authenticated with DUO MFA (`PR #112 <https://github.com/preset-io/backend-sdk/pull/112>`_).
- dbt target definition now defaults to the ``profile.yml`` if not specified (`PR #114 <https://github.com/preset-io/backend-sdk/pull/114>`_).
- The dbt sync can now be triggered using the ``dbt_project.yml`` file rather than the ``manifest.json`` (`PR #115 <https://github.com/preset-io/backend-sdk/pull/115>`_).
- CLI now supports `None` as column type (`PR #116 <https://github.com/preset-io/backend-sdk/pull/116>`_).
- Database connection is now tested before triggering the import (`PR #118 <https://github.com/preset-io/backend-sdk/pull/118>`_).
- Added support for companion YAML templates (`PR #120 <https://github.com/preset-io/backend-sdk/pull/120>`_).
- YAML rendering logic is now improved (`PR #121 <https://github.com/preset-io/backend-sdk/pull/121>`_ and `PR #205 <https://github.com/preset-io/backend-sdk/pull/205>`_).
- DB connection password is no longer logged in case the connection fails (`PR #122 <https://github.com/preset-io/backend-sdk/pull/122>`_).
- Import assets is now performed through the ``assets`` endpoint (`PR #124 <https://github.com/preset-io/backend-sdk/pull/124>`_).
- Large imports can be performed with the ``--split`` flag to prevent timeouts (`PR #124 <https://github.com/preset-io/backend-sdk/pull/124>`_). It also creates a ``checkpoint`` in case it fails so the retry would ignore already imported assets (`PR #137 <https://github.com/preset-io/backend-sdk/pull/137>`_ and `PR #139 <https://github.com/preset-io/backend-sdk/pull/139>`_).
- Preset Manager requests updated to use ``api.app.preset.io`` (`PR #127 <https://github.com/preset-io/backend-sdk/pull/127>`_).
- CLI now prompts user for job information if not specified when triggering a sync from dbt Cloud (`PR #128 <https://github.com/preset-io/backend-sdk/pull/128>`_).
- dbt exposures now includes assets that were created by manual datasets, based on the schema and table name (`PR #132 <https://github.com/preset-io/backend-sdk/pull/132>`_).
- Added support for Python 3.11 (`PR #133 <https://github.com/preset-io/backend-sdk/pull/133>`_).
- CLI now refreshes JWT token if needed (`PR #134 <https://github.com/preset-io/backend-sdk/pull/134>`_).
- Import failures due to connection errors are automatically retried (`PR #135 <https://github.com/preset-io/backend-sdk/pull/135>`_).
- Improved Get Resources logic (`PR #136 <https://github.com/preset-io/backend-sdk/pull/136>`_).
- CLI no longer prompts user to enter the DB password in case the connection already exists (`PR #140 <https://github.com/preset-io/backend-sdk/pull/140>`_).
- It's now possible to trigger a sync only for exposures back to dbt, using the ``--exposures-only`` flag (`PR #142 <https://github.com/preset-io/backend-sdk/pull/142>`_).
- CLI can be used to list SCIM groups and membership with the ``list-group-membership`` command (`PR #143 <https://github.com/preset-io/backend-sdk/pull/143>`_).
- The dbt profile name is now used to look for an existing DB connection in the Workspace, instead of the project name (`PR #151 <https://github.com/preset-io/backend-sdk/pull/151>`_).
- Added support for dbt derived metrics (`PR #154 <https://github.com/preset-io/backend-sdk/pull/154>`_, `PR #160 <https://github.com/preset-io/backend-sdk/pull/160>`_, `PR #196 <https://github.com/preset-io/backend-sdk/pull/196>`_, `PR #198 <https://github.com/preset-io/backend-sdk/pull/198>`_ and `PR #199 <https://github.com/preset-io/backend-sdk/pull/199>`_).
- Fixed column configuration issues after a dbt sync (`PR #156 <https://github.com/preset-io/backend-sdk/pull/156>`_ and `PR #165 <https://github.com/preset-io/backend-sdk/pull/165>`_).
- Added support for dbt 1.3 (`PR #159 <https://github.com/preset-io/backend-sdk/pull/159>`_).
- Improved the ``MetricSchema`` loading (`PR #159 <https://github.com/preset-io/backend-sdk/pull/159>`_).
- Added support for Secondary Contributor Workspace Role (`PR #186 <https://github.com/preset-io/backend-sdk/pull/186>`_).
- Use model table alias for dataset creation (`PR #192 <https://github.com/preset-io/backend-sdk/pull/192>`_).
- The dbt sync now only updates the DB connection in case ``--import-db`` is passed. It's also possible to trigger a sync without this flag (`PR #193 <https://github.com/preset-io/backend-sdk/pull/193>`_ and `PR #200 <https://github.com/preset-io/backend-sdk/pull/200>`_).
- Added support for specifying a certification payload for dbt syncs (`PR #203 <https://github.com/preset-io/backend-sdk/pull/203>`_).
- dbt models can now be filtered using ``config`` options (`PR #204 <https://github.com/preset-io/backend-sdk/pull/204>`_).
- It's now possible to disable Jinja syntax escaping during export, and Jinja syntax rendering during import (`PR #205 <https://github.com/preset-io/backend-sdk/pull/205>`_).

Version 0.1.1 - 2022-09-13
==========================

- File path is now passed to template as ``filepath`` in the ``sync native`` command.
- CLI can now invite users to Preset from a YAML file created by ``export-users``.
- Fix database update in the dbt sync.

Version 0.1.0 - 2022-09-09
==========================

- Initial release.
