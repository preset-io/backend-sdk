=========
Changelog
=========

Next
====

- Import users directly to Preset without invite.
- Export and import data access roles.
- Pass a JWT to Superset.
- Debug logging added to all network requests.
- Authenticate against Superset instances not using CSRF.
- Export assets by type or ID.
- Sync user roles (team, workspace, data access) from a file to a workspace.
- Add ``--version`` option.
- Do not prompt for workspaces if passing ``--help``.
- Fix ``export-users`` in Preset workspaces.

dbt
~~~

- Sync column descriptions.
- Fix parent/child node selection in dbt Core.
- Create datasets in different databases.
- Fix creating and updating BigQuery databases.
- Redshift databases are now created with the ``redshift+psycopg2://`` scheme.


Version 0.1.1 - 2002-09-13
==========================

- File path is now passed to template as ``filepath`` in the ``sync native`` command.
- CLI can now invite users to Preset from a YAML file created by ``export-users``.
- Fix database update in the dbt sync.

Version 0.1.0 - 2022-09-09
==========================

- Initial release.
