=========
Changelog
=========

Next
====

Version 0.2.18 - 2024-06-21
===========================

- It's now possible to sync a dbt Cloud project that doesn't have a semantic layer (`#301 <https://github.com/preset-io/backend-sdk/pull/301>`_).
- The dbt Core sync now supports syncing legacy metrics from dialects that are not supported by MetricFlow (`#302 <https://github.com/preset-io/backend-sdk/pull/302>`_).
- The dbt Cloud sync API endpoints were updated to support custom base domains (`#303 <https://github.com/preset-io/backend-sdk/pull/303>`_).

Version 0.2.17 - 2024-06-03
===========================

- The dbt sync now supports creating physical datasets in other catalogs (requires catalog Support in Superset)  (`#295 <https://github.com/preset-io/backend-sdk/pull/295>`_ and `#297 <https://github.com/preset-io/backend-sdk/pull/297>`_).
- The dbt ModelSchema now supports models with ``columns`` set to ``None`` (`#298 <https://github.com/preset-io/backend-sdk/pull/298>`_).
- It's now deprecated to trigger a dbt Core sync passing a ``dbt_project.yml`` file (support for passing the `manifest.json` file is no longer deprecated) (`#299 <https://github.com/preset-io/backend-sdk/pull/299>`_).

Version 0.2.16 - 2024-05-10
===========================

- Changed the dbt sync logic so that metrics that are not associated with the models being synced are ignored (`#289 <https://github.com/preset-io/backend-sdk/pull/289>`_).
- The dbt sync now indicates if the snowflake SQLAlchemy package is missing in the environment (`#290 <https://github.com/preset-io/backend-sdk/pull/290>`_).
- The dbt sync now removes Redshift-specific metatada from columns when refreshing a dataset (`#291 <https://github.com/preset-io/backend-sdk/pull/291>`_).
- Datasets used in dashboard filters are now included when importing assets individually with the ``--split`` flag (`#292 <https://github.com/preset-io/backend-sdk/pull/292>`_).

Version 0.2.15 - 2024-04-22
===========================

- The ``profiles.yml`` content is now rendered so that Jinja variables are handled properly (`#280 <https://github.com/preset-io/backend-sdk/pull/280>`_).
- Added an upper bound limit to the ``sqlglot`` version that gets installed to avoid compatibility issues (`#283 <https://github.com/preset-io/backend-sdk/pull/283>`_).
- The ``sync native`` command now tries to retrieve the DB connection ``uuid`` through the API first, to avoid exporting assets if not necessary (`#284 <https://github.com/preset-io/backend-sdk/pull/284>`_).
- Added support for syncing derived metrics that rely on other derived metrics containing Superset-specific Jinja syntax (`#285 <https://github.com/preset-io/backend-sdk/pull/285>`_).

Version 0.2.14 - 2024-04-10
===========================

- Fixed an issue when syncing columns for datasets powered by BigQuery (`#278 <https://github.com/preset-io/backend-sdk/pull/278>`_).
- Added support for syncing derived metrics that don't rely on other metrics, and also metrics including Superset-Jinja specific syntax (`#277 <https://github.com/preset-io/backend-sdk/pull/277>`_).

Version 0.2.13 - 2024-03-25
===========================

- Improved metric parsing with sqlglot (`#273 <https://github.com/preset-io/backend-sdk/pull/273>`_ and `#274 <https://github.com/preset-io/backend-sdk/pull/274>`_).
- Fixed the dataset creation flow for the dbt sync (`#275 <https://github.com/preset-io/backend-sdk/pull/275>`_).

Version 0.2.12 - 2024-03-19
===========================

- Support for including the account and project IDs with the dbt Cloud command (`#264 <https://github.com/preset-io/backend-sdk/pull/264>`_).
- Support MetricFlow/new dbt Semantic Layer for the dbt Core sync (`#265 <https://github.com/preset-io/backend-sdk/pull/265>`_).
- New ``--raise-failures`` flag added to the dbt sync commands to end the execution with an error in case any model failed to sync (`#266 <https://github.com/preset-io/backend-sdk/pull/266>`_).
- Syncing from dbt with the ``--preserve-metadata`` / ``--merge-metadata`` flags now sync the dataset columns (`#268 <https://github.com/preset-io/backend-sdk/pull/268>`_).
- Derived metrics for older dbt versions are now syncing properly (`#270 <https://github.com/preset-io/backend-sdk/pull/270>`_).

Version 0.2.11 - 2024-02-14
===========================

- Support for custom access URLs when connecting to dbt Cloud APIs (`#262 <https://github.com/preset-io/backend-sdk/pull/262>`_).

Version 0.2.10 - 2024-01-10
===========================

- Small improvements to the dbt sync logic (`#258 <https://github.com/preset-io/backend-sdk/pull/258>`_ and `#259 <https://github.com/preset-io/backend-sdk/pull/259>`_).

Version 0.2.9 - 2024-01-10
==========================

- Initial support for syncing metrics from dbt/MetricFlow (`#256 <https://github.com/preset-io/backend-sdk/pull/256>`_).

Version 0.2.8 - 2023-09-12
==========================

- The Jinja rendering/escaping logic for content migration was improved (`#237 <https://github.com/preset-io/backend-sdk/pull/237>`_).
- It's now possible to specify dbt models to be synced using the file name/path  (`#242 <https://github.com/preset-io/backend-sdk/pull/242>`_).

Version 0.2.7 - 2023-09-08
==========================

- The CLI now has a re-try mechanism to address Session-related errors (`#235 <https://github.com/preset-io/backend-sdk/pull/235>`_).
- It's now possible to trigger a dbt sync and merge dbt metadata with Preset metadata (`#238 <https://github.com/preset-io/backend-sdk/pull/238>`_).

Version 0.2.6 - 2023-08-17
==========================

- The dbt sync now uses Superset updated endpoints to properly create a virtual dataset (`#232 <https://github.com/preset-io/backend-sdk/pull/232>`_).
- It's now possible to authenticate to Superset instances that require a CSRF token (`#233 <https://github.com/preset-io/backend-sdk/pull/233>`_).

Version 0.2.5 - 2023-07-26
==========================

- Further adjustments to dbt marshmallow schemas to avoid integration errors (`#229 <https://github.com/preset-io/backend-sdk/pull/229>`_).

Version 0.2.4 - 2023-07-20
==========================

- Further adjustments to dbt marshmallow schemas to avoid integration errors (`#228 <https://github.com/preset-io/backend-sdk/pull/228>`_).
- Export RLS rules is now compatible with Preset Cloud and older Superset installations (`#227 <https://github.com/preset-io/backend-sdk/pull/227>`_)

Version 0.2.3 - 2023-07-14
==========================

- Adjustments to dbt marshmallow schemas to avoid integration errors (`#225 <https://github.com/preset-io/backend-sdk/pull/225>`_).

Version 0.2.2 - 2023-07-05
==========================

- ``certification`` and additional ``extra`` information is now synced from dbt models (`#213 <https://github.com/preset-io/backend-sdk/pull/213>`_ and `#215 <https://github.com/preset-io/backend-sdk/pull/215>`_).
- Improved the ``exposures`` sync (`#221 <https://github.com/preset-io/backend-sdk/pull/221>`_).
- The ``--preserve-columns`` flag can now be used to preserve ``groupby`` and ``filterable`` values for existing columns during a dbt sync (`#221 <https://github.com/preset-io/backend-sdk/pull/221>`_).
- The search for roles during the ``sync roles`` command now uses ``Equals`` comparison, instead of ``Starts with`` (`#222 <https://github.com/preset-io/backend-sdk/pull/222>`_).

Version 0.2.1 - 2023-05-30
==========================

- Fix for https://github.com/apache/superset/pull/24067 (`#211 <https://github.com/preset-io/backend-sdk/pull/211>`_).

Version 0.2.0 - 2023-05-23
==========================

- Column descriptions and labels are now synced from dbt models (`#72 <https://github.com/preset-io/backend-sdk/pull/72>`_, `#111 <https://github.com/preset-io/backend-sdk/pull/111>`_, `#195 <https://github.com/preset-io/backend-sdk/pull/195>`_ and `#197 <https://github.com/preset-io/backend-sdk/pull/197>`_).
- CLI can now provision users directly to the team (doesn't require accepting the invitation) with the ``import-users`` command. The ``export-users`` command can now also be used with Preset Workspaces.  (`#74 <https://github.com/preset-io/backend-sdk/pull/74>`_, `#100 <https://github.com/preset-io/backend-sdk/pull/100>`_ and `#148 <https://github.com/preset-io/backend-sdk/pull/148>`_).
- It's possible to export roles information using the ``export-roles`` command (`#75 <https://github.com/preset-io/backend-sdk/pull/75>`_ and `#161 <https://github.com/preset-io/backend-sdk/pull/161>`_). 
- Exported roles information can be imported via the ``import-roles`` command (`#76 <https://github.com/preset-io/backend-sdk/pull/76>`_, `#167 <https://github.com/preset-io/backend-sdk/pull/167>`_ and `#179 <https://github.com/preset-io/backend-sdk/pull/179>`_).
- Improved session object logic (`#77 <https://github.com/preset-io/backend-sdk/pull/77>`_). 
- Improved export/import logic for owernship and role information (`#79 <https://github.com/preset-io/backend-sdk/pull/79>`_).
- CLI can now add users to imported roles (`#81 <https://github.com/preset-io/backend-sdk/pull/81>`_).
- A JWT token can now be passed for authentication (`#82 <https://github.com/preset-io/backend-sdk/pull/82>`_).
- Added debug logging to API requests (`#83 <https://github.com/preset-io/backend-sdk/pull/83>_`).
- CLI can now export specific asset types, using the ``--asset-type`` flag (`#84 <https://github.com/preset-io/backend-sdk/pull/84>`_).
- CLI can now export specific assets only, using the ``--$asset_type-ids`` (for example ``--dashboard-ids``) flag (`#85 <https://github.com/preset-io/backend-sdk/pull/85>`_ and `#88 <https://github.com/preset-io/backend-sdk/pull/88>`_).
- CLI can now authenticate to Superset (On Premises) without CSRF token (`#87 <https://github.com/preset-io/backend-sdk/pull/87>`_).
- Workspace/Team prompt no longer happens in case ``--help`` was pased (`#89 <https://github.com/preset-io/backend-sdk/pull/89>`_).
- Team Roles, Workspace Roles and DARs can now be synced to a Preset team based on a YAML file (`#90 <https://github.com/preset-io/backend-sdk/pull/90>`_).
- Added ``--version`` command to display the installed version (`#91 <https://github.com/preset-io/backend-sdk/pull/91>`_).
- Fixed parent/child node selection in dbt Core for proper graph selection (`#92 <https://github.com/preset-io/backend-sdk/pull/92>`_).
- Improved logging for the dbt Client (`#94 <https://github.com/preset-io/backend-sdk/pull/94>`_).
- CLI now can create datasets for different databases (for DB Engines that supports multiple databases like Snowflake, BigQuery, etc) (`#95 <https://github.com/preset-io/backend-sdk/pull/95>`_).
- BQ connection can now successfully be created/updated from the ``profiles.yml`` information (`#96 <https://github.com/preset-io/backend-sdk/pull/96>`_).
- Redshift connectons now get created with the ``redshift+psycopg2`` driver (`#97 <https://github.com/preset-io/backend-sdk/pull/97>`_).
- YAML files outside of asset folders aren't imported in the native sync (`#99 <https://github.com/preset-io/backend-sdk/pull/99>`_).
- Improved BQ DB detection (`#102 <https://github.com/preset-io/backend-sdk/pull/102>`_).
- Reduced the maximum amount of files included in an export file (`#105 <https://github.com/preset-io/backend-sdk/pull/105>`_).
- Workspaces can now be defined as environment variables (`#106 <https://github.com/preset-io/backend-sdk/pull/106>`_).
- CLI can now create Snowflake connections authenticated via private key pair (`#108 <https://github.com/preset-io/backend-sdk/pull/108>`_).
- Improved the ``--exclude`` filter for the dbt sync (`#109 <https://github.com/preset-io/backend-sdk/pull/109>`_).
- Improved database connection logic (`#111 <https://github.com/preset-io/backend-sdk/pull/111>`_).
- CLI can now create Snowflake connections authenticated with DUO MFA (`#112 <https://github.com/preset-io/backend-sdk/pull/112>`_).
- dbt target definition now defaults to the ``profile.yml`` if not specified (`#114 <https://github.com/preset-io/backend-sdk/pull/114>`_).
- The dbt sync can now be triggered using the ``dbt_project.yml`` file rather than the ``manifest.json`` (`#115 <https://github.com/preset-io/backend-sdk/pull/115>`_).
- CLI now supports `None` as column type (`#116 <https://github.com/preset-io/backend-sdk/pull/116>`_).
- Database connection is now tested before triggering the import (`#118 <https://github.com/preset-io/backend-sdk/pull/118>`_).
- Added support for companion YAML templates (`#120 <https://github.com/preset-io/backend-sdk/pull/120>`_).
- YAML rendering logic is now improved (`#121 <https://github.com/preset-io/backend-sdk/pull/121>`_ and `#205 <https://github.com/preset-io/backend-sdk/pull/205>`_).
- DB connection password is no longer logged in case the connection fails (`#122 <https://github.com/preset-io/backend-sdk/pull/122>`_).
- Import assets is now performed through the ``assets`` endpoint (`#124 <https://github.com/preset-io/backend-sdk/pull/124>`_).
- Large imports can be performed with the ``--split`` flag to prevent timeouts (`#124 <https://github.com/preset-io/backend-sdk/pull/124>`_). It also creates a ``checkpoint`` in case it fails so the retry would ignore already imported assets (`#137 <https://github.com/preset-io/backend-sdk/pull/137>`_ and `#139 <https://github.com/preset-io/backend-sdk/pull/139>`_).
- Preset Manager requests updated to use ``api.app.preset.io`` (`#127 <https://github.com/preset-io/backend-sdk/pull/127>`_).
- CLI now prompts user for job information if not specified when triggering a sync from dbt Cloud (`#128 <https://github.com/preset-io/backend-sdk/pull/128>`_).
- dbt exposures now includes assets that were created by manual datasets, based on the schema and table name (`#132 <https://github.com/preset-io/backend-sdk/pull/132>`_).
- Added support for Python 3.11 (`#133 <https://github.com/preset-io/backend-sdk/pull/133>`_).
- CLI now refreshes JWT token if needed (`#134 <https://github.com/preset-io/backend-sdk/pull/134>`_).
- Import failures due to connection errors are automatically retried (`#135 <https://github.com/preset-io/backend-sdk/pull/135>`_).
- Improved Get Resources logic (`#136 <https://github.com/preset-io/backend-sdk/pull/136>`_).
- CLI no longer prompts user to enter the DB password in case the connection already exists (`#140 <https://github.com/preset-io/backend-sdk/pull/140>`_).
- It's now possible to trigger a sync only for exposures back to dbt, using the ``--exposures-only`` flag (`#142 <https://github.com/preset-io/backend-sdk/pull/142>`_).
- CLI can be used to list SCIM groups and membership with the ``list-group-membership`` command (`#143 <https://github.com/preset-io/backend-sdk/pull/143>`_).
- The dbt profile name is now used to look for an existing DB connection in the Workspace, instead of the project name (`#151 <https://github.com/preset-io/backend-sdk/pull/151>`_).
- Added support for dbt derived metrics (`#154 <https://github.com/preset-io/backend-sdk/pull/154>`_, `#160 <https://github.com/preset-io/backend-sdk/pull/160>`_, `#196 <https://github.com/preset-io/backend-sdk/pull/196>`_, `#198 <https://github.com/preset-io/backend-sdk/pull/198>`_ and `#199 <https://github.com/preset-io/backend-sdk/pull/199>`_).
- Fixed column configuration issues after a dbt sync (`#156 <https://github.com/preset-io/backend-sdk/pull/156>`_ and `#165 <https://github.com/preset-io/backend-sdk/pull/165>`_).
- Added support for dbt 1.3 (`#159 <https://github.com/preset-io/backend-sdk/pull/159>`_).
- Improved the ``MetricSchema`` loading (`#159 <https://github.com/preset-io/backend-sdk/pull/159>`_).
- Added support for Secondary Contributor Workspace Role (`#186 <https://github.com/preset-io/backend-sdk/pull/186>`_).
- Use model table alias for dataset creation (`#192 <https://github.com/preset-io/backend-sdk/pull/192>`_).
- The dbt sync now only updates the DB connection in case ``--import-db`` is passed. It's also possible to trigger a sync without this flag (`#193 <https://github.com/preset-io/backend-sdk/pull/193>`_ and `#200 <https://github.com/preset-io/backend-sdk/pull/200>`_).
- Added support for specifying a certification payload for dbt syncs (`#203 <https://github.com/preset-io/backend-sdk/pull/203>`_).
- dbt models can now be filtered using ``config`` options (`#204 <https://github.com/preset-io/backend-sdk/pull/204>`_).
- It's now possible to disable Jinja syntax escaping during export, and Jinja syntax rendering during import (`#205 <https://github.com/preset-io/backend-sdk/pull/205>`_).

Version 0.1.1 - 2022-09-13
==========================

- File path is now passed to template as ``filepath`` in the ``sync native`` command.
- CLI can now invite users to Preset from a YAML file created by ``export-users``.
- Fix database update in the dbt sync.

Version 0.1.0 - 2022-09-09
==========================

- Initial release.
