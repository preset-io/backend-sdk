==========
preset-cli
==========

    A CLI to interact with Preset workspaces.

This tool is a command line interface (CLI) to interact with your Preset workspaces. Currently it can be used to sync resources (databases, datasets, charts, dashboards) from source control, either in native format or from a `DBT <https://www.getdbt.com/>`_ project. It can also be used to run SQL against any database in any workspace. In the future, the CLI will also allow you to manage your workspaces and users.

Installation
============

Install the CLI with ``pip``:

.. code-block:: bash

    $ pip install -U setuptools setuptools_scm wheel
    $ pip install "git+https://github.com/preset-io/preset-cli.git#egg=preset-cli"


