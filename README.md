## DBMS Final Project - OrpheusDBPlus

This project is inspired by [OrpheusDB](https://github.com/orpheus-db/implementation). We aim to create a version control system on top of
relational databases. We currently decide to use MySQL as the backend database.

### Installation and Setup

1. Install `MySQL Community Server` and follow the instructions to set up it. ([MySQL Download](https://dev.mysql.com/downloads/mysql/))

2. Install this package (can use either `anaconda` or `virtualenv`)
    ```
    conda create -n orpheusplus python=3.11
    conda activate orpheusplus
    pip install -e .
    ```

3. Run `orpheusplus config` to perform the first-time setup.
    ```
    orpheusplus config 
    >> First time setup:
    >> Create `config.yaml` at ... 
    >> ...
    ```

3. Run `orpheusplus config` again to configure the user information and change the default settings if needed. (Please make sure that the MySQL service is running.)

### Usage
> If you encounter any errors, please run `orpheusplus remove -n TABLE_NAME`, `git pull` the latest update, and then try the operations again. If errors still occur, it is likely a bug.  

1. Initialize a version table
    ```
    orpheusplus init -n new_table -s ./examples/sample_schema.csv
    ```

2. Insert data into a version table
    ```
    orpheusplus insert -n new_table -d ./examples/data_1.csv
    ```

3. Commit changes (create a version)  
    *Note: the version identifier is automatically created and is a number that increments from 1*
    ```
    orpheusplus commit -n new_table -m new_version
    ```

4. Checkout a version (switch version)
    ```
    # Add other data to the table and commit again
    orpheusplus insert -n new_table -d ./examples/data_2.csv
    orpheusplus commit -n new_table -m another_new_version
    orpheusplus checkout -n new_table -v 1
    ```

5. Run an example script to get the data from the current version 
    ```
    # Current version: 1
    orpheusplus run ./examples/sql_script_1.sql

    ╒═══════════════╤═══════╤══════════╕
    │   employee_id │   age │   salary │
    ╞═══════════════╪═══════╪══════════╡
    │           101 │    30 │    10340 │
    ├───────────────┼───────┼──────────┤
    │           102 │    18 │     4000 │
    ├───────────────┼───────┼──────────┤
    │           103 │    40 │    20500 │
    ╘═══════════════╧═══════╧══════════╛

    # Check out to version 2
    orpheusplus checkout -n new_table -v 2
    orpheusplus run ./examples/sql_script_1.sql

    ╒═══════════════╤═══════╤══════════╕
    │   employee_id │   age │   salary │
    ╞═══════════════╪═══════╪══════════╡
    │           101 │    30 │    10340 │
    ├───────────────┼───────┼──────────┤
    │           102 │    18 │     4000 │
    ├───────────────┼───────┼──────────┤
    │           103 │    40 │    20500 │
    ├───────────────┼───────┼──────────┤
    │           104 │    23 │     7000 │
    ├───────────────┼───────┼──────────┤
    │           105 │    21 │     7400 │
    ├───────────────┼───────┼──────────┤
    │           106 │    32 │    10320 │
    ╘═══════════════╧═══════╧══════════╛
    ```
6. Drop the version table
    ```
    orpheusplus remove -n new_table
    ```


**Available commands:**
```
usage: orpheusplus [-h] {config,init,ls,remove,checkout,commit,insert,delete,update,run} ...

options:
  -h, --help            show this help message and exit

commands:
  valid commands

  {config,init,ls,remove,checkout,commit,insert,delete,update,run}
    config              Configure MySQL connection
    init                Initialize version control to a table
    ls                  List all tables under version control
    remove              Drop a version table
    checkout            Checkout a version
    commit              Create a new version
    insert              Insert data from file
    delete              Delete data from file
    update              Update data from file
    run                 Run a SQL script
```

