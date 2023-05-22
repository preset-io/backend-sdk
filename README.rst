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
    $ pip install preset-cli

Make sure you're using Python 3.8 or newer.

You can also install from ``main``, if you need recent features:

.. code-block:: bash

     $ pip install "git+https://github.com/preset-io/backend-sdk.git"

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
- ``preset-cli invite-users``: invite users to Preset.
- ``preset-cli import-users``: automatically add users to Preset.
- ``preset-cli list-group-membership``: List SCIM groups from a team and their memberships.
- ``preset-cli superset sql``: run SQL interactively or programmatically against an analytical database.
- ``preset-cli superset export-assets`` (alternatively, ``preset-cli superset export``): export resources (databases, datasets, charts, dashboards) into a directory as YAML files.
- ``preset-cli superset export-ownership``: export resource ownership (UUID -> email) into a YAML file.
- ``preset-cli superset export-rls``: export RLS rules into a YAML file.
- ``preset-cli superset export-roles``: export user roles into a YAML file.
- ``preset-cli superset export-users``: export users (name, username, email, roles) into a YAML file.
- ``preset-cli superset sync native`` (alternatively, ``preset-cli superset import-assets``): synchronize the workspace from a directory of templated configuration files.
- ``preset-cli superset sync dbt-core``: synchronize the workspace from a dbt Core project.
- ``preset-cli superset sync dbt-cloud``: synchronize the workspace from a dbt Cloud project.

All the ``superset`` sub-commands can also be executed against a standalone Superset instance, using the ``superset-cli`` command. This means that if you are running an instance of Superset at https://superset.example.org/ you can export its resources with the command:

.. code-block:: bash

    % superset-cli https://superset.example.org/ export-assets /path/to/directory

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

Disabling Jinja Templating
~~~~~~~~~~~~~~~~~~~~~~~~~~

Both the CLI and Superset support Jinja templating. To prevent the CLI from loading Superset Jinja syntax, the export operation automatically escapes Jinja syntax from YAML files. As a consequence, this query:

.. code-block:: yaml

    sql: 'SELECT action, count(*) as times
        FROM logs
        {% if filter_values(''action_type'')|length %}
            WHERE action is null
            {% for action in filter_values(''action_type'') %}
                or action = ''{{ action }}''
            {% endfor %}
        {% endif %}
        GROUP BY action'

Becomes this:

.. code-block:: yaml

    sql: 'SELECT action, count(*) as times
        FROM logs
        {{ '{% if' }} filter_values(''action_type'')|length {{ '%}' }}
            WHERE action is null
            {{ '{% for' }} action in filter_values(''action_type'') {{ '%}' }}
                or action = ''{{ '{{' }} action {{ '}}' }}''
            {{ '{% endfor %}' }}
        {{ '{% endif %}' }}
        GROUP BY action'

When performing the import, the CLI would load any templating syntax that isn't escaped, and remove escaping. However, this escaping syntax isn't compatible with UI imports. 
To avoid issues when running migrations using both the CLI and the UI, you can use:

- ``--disable-jinja-escaping`` flag with the ``export-assets`` command to disable the escaping (so that exported assets can be imported via the UI)
- ``--disable-jinja-templating`` flag with the ``sync native`` command to disable jinja templating (so that assets exported via the UI can be imported via the CLI)

Note that using these flags would remove the ability to dynamically modify the content through the CLI. 

Synchronizing to and from dbt
-----------------------------

The CLI also allows you to synchronize models, and metrics from a `dbt <https://www.getdbt.com/>`_ project.

If you're using dbt Core you can point the CLI to your compiled manifest and your profiles file, so that the database is automatically created, together with all the models and metrics. The full command is:

.. code-block:: bash

   % preset-cli --workspaces=https://abcdef12.us1a.app.preset.io/ \
   > superset sync dbt-core /path/to/dbt/my_project/target/manifest.json \
   > --project=my_project --target=dev --profiles=${HOME}/.dbt/profiles.yml \
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

By default, the CLI sync would create a new database on the destination workspace using below name structure:

.. code-block:: python

    f"{project_name}_{target_name}"

If you want to sync data to an existing database connection on the workspace instead, you can specify the database connection name on the profiles YAML file. Add below structure under the ``<target-name>``:

.. code-block:: yaml
    
    meta:
      superset:
        database_name: my DB name # <= specify the database connection/display name used on the workspace
        
Example:

.. code-block:: yaml

    jaffle_shop:
      outputs:
        dev:
          meta:
            superset:
              database_name: Postgres - Production

If  ``--import-db`` was passed and a database connection was found on the workspace, the operation would update the connection configuration with the dbt connection settings.

If you're using dbt Cloud you can instead pass a job ID and a `service account access token <https://cloud.getdbt.com/#/accounts/72449/settings/service-tokens/new/>`_:

.. code-block:: bash

    % preset-cli --workspaces=https://abcdef12.us1a.app.preset.io/ \
    > superset sync dbt-cloud \
    > $TOKEN $JOB_ID \
    > --external-url-prefix=http://localhost:8080/

The token only needs access to the "Metadata only" permission set for your project. You can see the job ID by going to the project URL in dbt Cloud and looking at the last ID in the URL. For example, if the URL is https://cloud.getdbt.com/#/accounts/12345/projects/567890/jobs/ the job ID is 567890.

When syncing from dbt Cloud, the database connection must already exist on the target workspace. The connection display name on the workspace must match the database name from dbt Cloud.
              
Selecting models
~~~~~~~~~~~~~~~~

By default all the models will be synchronized to the workspace. The CLI supports a subset of the syntax used by the ``dbt`` command line to select which models should be synchronized. Models that should be synchronized can be specified via the ``--select`` flag:

.. code-block:: bash

    % preset-cli ... --select my_model    # sync only "my_model"
    % preset-cli ... --select my_model+   # sync "my_model" and its children
    % preset-cli ... --select my_model+2  # sync "my_model" and its children up to 2 degrees
    % preset-cli ... --select +my_model   # sync "my_model" and its parents
    % preset-cli ... --select +my_model+  # sync "my_model" with parents and children

Multiple selectors can be passed by repeating the ``--select`` flag (note that this is slightly different from dbt, which doesn't require repeating the flag):

.. code-block:: bash

    % preset-cli ... --select my_model --select my_other_model

The CLI also support the intersection operator:

.. code-block:: bash

    % preset-cli ... --select my_model+,tag:test

The command above will synchronize ``my_model`` and its children, as long as the models have the "test" tag.

Finally, the CLI also supports the ``--exclude`` flag in a similar way:

.. code-block:: bash

    % preset-cli --select my_model+ --exclude tag:test

The command above synchronizes "my_model" and its children, as long as the models don't have the "test" tag.

Exporting resources
-------------------

The CLI can also be used to export resources (databases, datasets, charts, and dashboards) from a given Preset workspace (using ``preset-cli``) or Superset instance (using ``superset-cli``). This is useful for migrating resources between workspaces, from an existing Superset installation to Preset, or even from Preset to Superset (one of the advantages of Preset is no vendor lock in!).

To export resources from a self-hosted Superset instance:

.. code-block:: bash

    % superset-cli https://superset.example.org/ export /path/to/directory

This will create a nice directory structure in ``/path/to/directory``, ready to be imported using the ``sync native`` command.

To export resources from a Preset workspace:

.. code-block:: bash

    % preset-cli --workspaces=https://abcdef12.us1a.app.preset.io/ \
    > superset export /path/to/directory

It's also possible to use the CLI to export specific resources:

Use ``--asset-type`` to export all assets from a given type. Available options:

- ``dashboard``
- ``chart``
- ``dataset``
- ``database``

For example, running below command would export all dashboards from this workspace (datasets, charts and DB connections wouldn't be included):

.. code-block:: bash

    % preset-cli --workspaces=https://abcdef12.us1a.app.preset.io/ \
    > superset export /path/to/directory --asset-type=dashboard
    
Use ``--asset-ids`` to filter for specific assets. Available options:

- ``dashboard-ids``
- ``chart-ids``
- ``dataset-ids``
- ``database-ids``

For example, running below command would export specified dashboards from this workspace (datasets, charts and DB connections would be included):

.. code-block:: bash

    % preset-cli --workspaces=https://abcdef12.us1a.app.preset.io/ \
    > superset export /path/to/directory --dashboard-ids=9,10

To import the exported resources into a Preset workspace:

.. code-block:: bash

    % preset-cli --workspaces=https://abcdef12.us1a.app.preset.io/ \
    > superset sync native /path/to/directory

Finally, to import in a standalone Superset instance:

.. code-block:: bash

    % superset-cli https://superset.example.org/ sync native /path/to/directory

Note that any existing Jinja2 template markers present will be escaped. For example, if you have a virtual dataset defined as:

.. code-block:: sql

    SELECT action, count(*) as times
    FROM logs
    WHERE
        action in {{ filter_values('action_type')|where_in }}
    GROUP BY action

The resulting YAML file will have the query defined as:

.. code-block:: sql

    SELECT action, count(*) as times
    FROM logs
    WHERE
        action in {{ '{{' }} filter_values('action_type')|where_in }} '}}' }}
    GROUP BY action

So that when processed by ``preset-cli superset sync native`` the original Jinja2 is reconstructed correctly.

Exporting users
~~~~~~~~~~~~~~~

The ``preset-cli superset export-users`` command can be used to export a list of users. These users can then be imported to Preset via the ``preset-cli import-users`` command.

You can also export roles via ``preset-cli superset export-roles``, and import with ``import-roles``.

Exporting RLS rules
~~~~~~~~~~~~~~~~~~~

The ``preset-cli superset export-rls`` command can be used to export a list of RLS rules. Currently there's no way to import this into a workspace, but work is in progress.

Exporting ownership
~~~~~~~~~~~~~~~~~~~

The ``preset-cli superset export-ownership`` command generates a YAML file with information about ownership of different resources. The file maps resource UUIDs to user email address, and in the future will be used to recreate ownership on a different instance of Superset.

Listing SCIM Groups
~~~~~~~~~~~~~~~~~~~
The ``preset-cli list-group-membership`` command prints all SCIM groups (including membership) associated with a Preset team. Instead of printing the results on the terminal (whcih can be useful for quick troubleshooting), it's possible to use ``--save-report=yaml`` or ``--save-report=csv`` to write results to a file. The file name would be ``{TeamSlug}__user_group_membership.{FileExtension}``.
