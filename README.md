# ETL

## Python environment

### Setup

Create a `conda` environment called `etl-env` and install the requirements in it.

```bash
conda create -yn etl-env python=3.7 --file requirements.txt
```

### Development

Use the new `conda` environment in jupyter notebook.

```bash
conda install -n base nb_conda_kernels
conda activate etl-env
python -m pip install -r requirements_dev.txt
conda deactivate
jupyter notebook
```

## Database

### Setup PostgreSQL database (macOS)

Install PostgreSQL:

1. Install Homebrew (macOS): <http://brew.sh/>
1. Install PostgreSQL (macOS): `brew install postgresql`

Start service:

1. Start PostgreSQL with [homebrew-services](https://github.com/Homebrew/homebrew-services):
`brew services run postgresql`
    - Alternative: `pg_ctl -D /usr/local/var/postgres start`
    - `brew service start <service>` starts the `<service>` at login, while `brew services run` runs
    the `<service>` but doesn't start it at login (nor boot).

## Run the ETL

```bash
python create_tables.py && python etl.py
```

To debug: `psql --dbname sparkifydb --username student`

If you receive the error `DETAIL:  There is 1 other session using the database.`, close all other
connections, except the current one:

```sql
SELECT pid, pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = current_database() AND pid <> pg_backend_pid();
```

## Cleanup

1. Remove the Python environment: `conda env remove -n etl-env`
1. Remove all tables:

    ```bash
    $ psql --dbname sparkifydb
   sparkifydb=# \dn+
   sparkifydb=# DROP SCHEMA public CASCADE;
   sparkifydb=# \dn+
   sparkifydb=# CREATE SCHEMA public;
   sparkifydb=# GRANT ALL ON SCHEMA public TO public;
   sparkifydb=# COMMENT ON SCHEMA public IS 'standard public schema';
   sparkifydb=# \dn+
    ```

1. Stop the PostgreSQL service: `brew services stop postgresql`
    - Alternative: `pg_ctl -D /usr/local/var/postgres stop`
    - Check by listing all services managed by `brew services` (`postgresql` should be `stopped`):
    `brew services list`
