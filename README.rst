==========
preset-cli
==========

    A CLI to interact with Preset workspaces.

This tool is a command line interface (CLI) to interact with your Preset workspaces. Currently it can be used to sync resources (databases, datasets, charts, dashboards) from source control, either in native format or from a `dbt <https://www.getdbt.com/>`_ project. It can also be used to run SQL against any database in any workspace. In the future, the CLI will also allow you to manage your workspaces and users.

Installation
============

Install the CLI with ``pip``:

.. code-block:: bash

    $ pip install -U setuptools setuptools_scm wheel  # for good measure
    $ pip install "git+https://github.com/preset-io/backend-sdk.git"

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

The CLI can run commands against one or more Preset workspaces (Apache Superset instances). When running a command you can specify the workspace(s) by passing a comma-separated list of URLs to the ``--workspaces`` option:

.. code-block:: bash

    % preset-cli \
    > --workspaces=https://abcdef12.us1a.app.preset.io/,https://34567890.us1a.app.preset.io/ \
    > superset sql

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
- 💾 creating/initializing metadata database
- 🚧 migrating metadata database
- 🕵️ migrating secrets
- ⤴️ upgrading workspace
- ❗️ error
- ❓ unknown state

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
- ``preset-cli superset export``: export resources (databases, datasets, charts, dashboards) into a directory as YAML files.
- ``preset-cli superset sync native``: synchronize the workspace from a directory of templated configuration files.
- ``preset-cli superset sync dbt``: synchronize the workspace from a dbt project.

All the ``superset`` sub-commands can also be executed against a standalone Superset instance, using the ``superset-cli`` command. This means that if you are running an instance of Superset at https://superset.example.org/ you can export its resources with the command:

.. code-block:: bash

    % superset-cli https://superset.example.org/ export /path/to/directory

And then import everything to a Preset workspace with:

.. code-block:: bash

    % preset-cli superset sync native /path/to/directory

Running SQL
-----------

The CLI offers an easy way to run SQL against an analytical database in a workspace. This can be done programmatically or interactively. For example, to run the query ``SELECT COUNT(*) AS revenue FROM sales`` given a workspace URL and a database ID you can run:

.. code-block:: bash

    % preset-cli --workspaces=https://abcdef12.us1a.app.preset.io/ superset sql \
    > --database-id=1 -e "SELECT COUNT(*) AS revenue FROM sales"

    https://abcdef12.us1a.app.preset.io/
      revenue
    ---------
           42

If you don't specify the database ID you will be shown a list of available databases in order to choose one. If you don't specify the SQL query via the ``-e`` option the CLI will start a simple REPL (read-eval-print loop) where you can run queries interactively.

Synchronizing from exports
--------------------------

You can use the CLI to manage workspaces resources — databases, datasets, charts, and dashboards — from source control. The configuration should be stored as YAML files, using the same format the Apache Superset uses for importing and exporting resources.

The easiest way to generate the configuration files is to build one or more dashboards in a Preset workspace, export them together, and unzip the generated file into a directory.

.. image:: https://github.com/preset-io/preset-cli/raw/master/docs/images/export_dashboards.png

After unzipping the directory should look like this:

- ``charts/``
- ``dashboards/``
- ``databases/``
- ``datasets/``
- ``metadata.yaml``

You can see an example `here <https://github.com/preset-io/preset-cli/tree/master/examples/exports>`_.

To synchronize these files to a Preset workspace you only need to run:

.. code-block:: bash

    % preset-cli --workspaces=https://abcdef12.us1a.app.preset.io/ \
    > superset sync native /path/to/directory/

If any of the resources already exist you need to pass the ``--overwrite`` flag in order to replace them. The CLI will warn you of any resources that already exist if the flag is not passed:

.. code-block:: bash

    % preset-cli --workspaces=https://abcdef12.us1a.app.preset.io/ \
    > superset sync native /path/to/directory/
    Error importing database
    The following file(s) already exist. Pass --overwrite to replace them.
    - databases/Google_Sheets.yaml
    Error importing dataset
    The following file(s) already exist. Pass --overwrite to replace them.
    - datasets/Google_Sheets/country_cnt.yaml
    Error importing chart
    The following file(s) already exist. Pass --overwrite to replace them.
    - charts/Total_count_134.yaml
    Error importing dashboard
    The following file(s) already exist. Pass --overwrite to replace them.
    - dashboards/White_label_test.yaml

Synchronized resources can be marked as "externally managed" by passing the ``--disallow-edits`` flag to the command. When the flag is passed users won't be able to edit the resources. It's also possible to provide a URL where the resource can be modified, eg, a link to a file in a Github repository. This can be done by passing the ``--external-url-prefix`` flag:

.. code-block:: bash

    % preset-cli --workspaces=https://abcdef12.us1a.app.preset.io/ \
    > superset sync native /path/to/directory/ --disallow-edits \
    > --external-url-prefix=https://github.com/org/project/blob/master/

This way, the file ``dashboards/White_label_test.yaml`` would have an external URL pointing to https://github.com/org/project/blob/master/dashboards/White_label_test.yaml. Currently the URL is not displayed anywhere, but in the near future we should have affordances pointing users to it from the instance UI.

Using templates
~~~~~~~~~~~~~~~

One of the most powerful features of this command is that the YAML configuration files are treated as `Jinja2 <https://jinja.palletsprojects.com/en/3.0.x/>`_ templates, allowing you to parametrize the synchronized files. For example, imagine a simple chart like this:

.. code-block:: yaml

    slice_name: Total sales
    viz_type: big_number_total
    params:
      metric: sum__sales
      adhoc_filters: []

The chart shows the metric ``sum__sales``, representing the total (unfiltered) sales of a given product. We can change the chart configuration to look like this instead:

.. code-block:: yaml

    {% if country %}
    slice_name: Sales in {{ country }}
    {% else %}
    slice_name: Total sales
    {% endif %}
    viz_type: big_number_total
    params:
      metric: sum__sales
      {% if country %}
      adhoc_filters:
        - clause: WHERE
          expressionType: SQL
          sqlExpression: country = '{{ country }}'
          subject: null
          operator: null
          comparator: null
      {% else %}
      adhoc_filters: []
      {% endif %}

Now, if the ``country`` parameter is set the chart will have a different title and an additional filter. Multiple parameters can be passed as optiona via the command line:

.. code-block:: bash

    % preset-cli --workspaces=https://abcdef12.us1a.app.preset.io/ \
    > superset sync native /path/to/directory/ -o country=BR

Templates also have access to the workspace name through the ``instance`` variable (a `URL object <https://pypi.org/project/yarl/>`_):

.. code-block:: yaml

    params:
      metric: sum__sales
      adhoc_filters:
        - clause: WHERE
          expressionType: SQL
          {% if instance.host == '//abcdef12.us1a.app.preset.io/ %}
          sqlExpression: warehouse_id = 1
          {% elif instance.host == '//34567890.us1a.app.preset.io/ %}
          sqlExpression: warehouse_id = 2
          {% else %}
          sqlExpression: warehouse_id = 3
          {% endif %}

You can also load variables from the environment by passing the ``--load-env`` (or ``-e``) flag:

.. code-block:: yaml

    database_name: Postgres
    sqlalchemy_uri: postgres://{{ env["POSTGRES_HOSTNAME"] }}


Finally, as shown in the next section, templates can leverage user-defined functions.

User defined functions
~~~~~~~~~~~~~~~~~~~~~~

You can create your own functions to be used in the configuration templates. Simply create a sub-directory called ``functions/`` in the directory where the configuration files are stored, and add one or more Python files. As a simple example, imagine a file called ``functions/demo.py`` with the following content:

.. code-block:: python

    # functions/demo.py
    def hello_world() -> str:
        return "Hello, world!"

The function can then be called from any template the following way:

.. code-block:: yaml

    slice_name: {{ functions.demo.hello_world() }}
    viz_type: big_number_total
    params:
      ...

Synchronizing to and from dbT
-----------------------------

The CLI also allows you to synchronize sources, models, and metrics from a `dbt <https://www.getdbt.com/>`_ project, together with databases from a profile. The full command is:

.. code-block:: bash

   % preset-cli --workspaces=https://abcdef12.us1a.app.preset.io/ \
   > superset sync dbt /path/to/dbt/my_project/target/manifest.json \
   > --project=my_project --target=dev --profile=${HOME}/.dbt/profiles.yml \
   > --exposures=/path/to/dbt/my_project/models/exposures.yaml \
   > --import-db \
   > --external-url-prefix=http://localhost:8080/

Running this command will:

1. Read the dbt profile and create the ``$target`` database for the specified project in the Preset workspace.
2. Every source in the project will be created as a dataset in the Preset workspace.
3. Every model in the project will be created as a dataset in the Preset workspace.
4. Any `metrics <https://docs.getdbt.com/docs/building-a-dbt-project/metrics>`_ will be added to the corresponding datasets.
5. Every dashboard built on top of the dbt sources and/or models will be synchronized back to dbt as an `exposure <https://docs.getdbt.com/docs/building-a-dbt-project/exposures>`_.

The ``--external-url-prefix`` should point to your dbt docs, so that the resources in the workspace can point to the source of truth where they are being managed. Similar to the native sync, the dbt sync also supports the ``--disallow-edits`` flag.

Exporting resources
-------------------

The CLI can also be used to export all resources (databases, datasets, charts, and dashboards) from a given Preset workspace (using ``preset-cli``) or Superset instance (using ``superset-cli``). This is useful for migrating resources between workspaces, from an existing Superset installation to Preset, or even from Preset to Superset (one of the advantages of Preset is no vendor lock in!).

The run the command on a self-hosted Superset instance:

.. code-block:: bash

    % superset-cli https://superset.example.org/ export /path/to/directory

This will create a nice directory structure in ``/path/to/directory``, ready to be imported using the ``sync native`` command.

To export resources from a Preset workspace:

.. code-block:: bash

    % preset-cli --workspaces=https://abcdef12.us1a.app.preset.io/ \
    > superset export /path/to/directory

To import the exported resources into a Preset workspace:

.. code-block:: bash

    % preset-cli --workspaces=https://abcdef12.us1a.app.preset.io/ \
    > superset sync native /path/to/directory

Finally, to import in a standalone Superset instance:

.. code-block:: bash

    % superset-cli https://superset.example.org/ sync native /path/to/directory
