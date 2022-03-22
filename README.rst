==========
preset-cli
==========

    A CLI to interact with Preset workspaces.

This tool is a command line interface (CLI) to interact with your Preset workspaces. Currently it can be used to sync resources (databases, datasets, charts, dashboards) from source control, either in native format or from a `DBT <https://www.getdbt.com/>`_ project. It can also be used to run SQL against any database in any workspace. In the future, the CLI will also allow you to manage your workspaces and users.

Installation
============

Install the CLI with ``pip``:

.. code-block:: bash

    $ pip install -U setuptools setuptools_scm wheel  # for good measure
    $ pip install "git+https://github.com/preset-io/preset-cli.git"

Make sure you're using Python 3.8 or newer.

Authentication
==============

The CLI requires an API key for authentication, composed of token and an associated secret. Both can be defined in your environment as ``PRESET_API_TOKEN`` and ``PRESET_API_SECRET``, respectively, or can be stored in a configuration file. To store the credentials in a configuration file simply run ``preset-cli auth``. This should open https://manage.app.preset.io/app/user in your browser so you can generate a new token, and it will prompt you for the token and its secret:

.. code-block:: bash

    % preset-cli auth
    Please generate a new token at https://manage.app.preset.io/app/user if you don't have one already
    API token: 35dac901-c775-43ff-8eb4-816edc061487
    API secret: [will not echo]
    Credentials stored in /Users/beto/Library/Application Support/preset-cli/credentials.yaml

The credentials will be stored in a system-dependent location, in a file that's readable only by you.

This step is optional. If you try to run the CLI without the token/secret defined in your environment or stored in the expected location you will be prompted for them:

.. code-block:: bash

    % preset-cli superset sql
    You need to specify a JWT token or an API key (name and secret)
    API token: 35dac901-c775-43ff-8eb4-816edc061487
    API secret: [will not echo]
    Store the credentials in /Users/beto/Library/Application Support/preset-cli/credentials.yaml? [y/N]

You can also pass the token/secret (or even the JWT token) directly as an option:

.. code-block:: bash

    % preset-cli --api-token 35dac901-c775-43ff-8eb4-816edc061487 --api-secret XXX superset sql
    % preset-cli --jwt-token XXX superset sql

In summary, there are 3 options for credentials:

1. Stored in the environment as ``PRESET_API_TOKEN`` and ``PRESET_API_SECRET``.
2. Stored in a user-readable file called ``credentials.yaml``, with system-dependent location.
3. Passed directly to the CLI via ``--api-token`` and ``--api-secret`` (or ``--jwt-token``) options.

Workspaces
==========

The CLI can run commands against one or more Preset workspaces (Superset instances). When running a command you can specify the workspace(s) by passing a comma-separated list of URLs to the ``--workspaces`` option:

.. code-block:: bash

    % preset-cli --workspaces https://abcdef12.us1a.app.preset.io/,https://34567890.us1a.app.preset.io/ superset sql

If you omit the ``--workspaces`` option you will be prompted interactively:

.. code-block:: bash

    % preset-cli superset sql
    Choose one or more workspaces (eg: 1-3,5,8-):

    # Team 1 #
    ✅ (1) The Data Lab
    🚧 (2) New workspace

    # Dev #
    ⤴️ (3) Test workspace

Each workspace has an icon depicting its status:

- ✅ ready
- 📊 loading examples
- 💾 creating metadata database
- 💾 initializing metadata database
- 🚧 migrating metadata database
- 🕵️ migrating secrets
- ❓ unknown state
- ❗️ error
- ⤴️ upgrading workspace

You can specify one or more workspaces by using a comma-separated list of numbers and/or ranges:

- ``1``: workspace 1
- ``1,3``: workspaces 1 and 3
- ``1,3-5``: workspaces 1, 3, 4, and 5
- ``-3``: workspaces 1, 2, and 3
- ``1-``: all workspaces
- ``-``: all workspaces

Commands
========

The following commands are currently available:

- ``preset-cli auth``: store authentication credentials.
- ``preset-cli superset sql``: run SQL interactively or programmatically against an analytical database.
- ``preset-cli superset sync native``: synchronize the workspace from a directory of templated configuration files.
- ``preset-cli superset sync dbt``: synchronize the workspace from a DBT project.

Running SQL
-----------

The CLI offers an easy way to run SQL against an analytical database in a workspace. This can be done programmatically or interactively. For example, to run the query ``SELECT COUNT(*) FROM sales`` given a workspace URL and a database ID you can run:

.. code-block:: bash

    % preset-cli --workspaces https://abcdef12.us1a.app.preset.io/ superset sql \
    > --database-id 1 -e "SELECT COUNT(*) AS revenue FROM sales"

    https://abcdef12.us1a.app.preset.io/
      revenue
    ---------
           42

If you don't specify the database ID you will be shown a list of available databases in order to choose one. If you don't specify the SQL query via the ``-e`` option the CLI will start a simple REPL (read-eval-print loop) where you can run queries interactively.

Synchronizing from exports
--------------------------

Synchronizing to and from DBT
-----------------------------
