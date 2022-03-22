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

1. Stored in the environment as ``PRESET_API_TOKEN`` and ``PRESET_API_SECRET``;
2. Stored in a user-readable file called ``credentials.yaml``, with system-dependent location;
3. Passed directly to the CLI via ``--api-token`` and ``--api-secret`` (or ``--jwt-token``) options.

Workspaces
==========


Commands
========

Running SQL
-----------

Synchronizing from exports
--------------------------

Synchronizing to and from DBT
-----------------------------
