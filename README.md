## DBMS Final Project - OrpheusDBPlus

This project is inspired by [OrpheusDB](https://github.com/orpheus-db/implementation). We aim to create a version control system on top of
relational databases. We currently decide to use MySQL as the backend database.

### Installation

1. Install `MySQL Community Server` and follow the instructions to set up it. ([MySQL Download](https://dev.mysql.com/downloads/mysql/))

2. Install this package (can use either `anaconda` or `virtualenv`)
    ```bash
    conda create -n orpheusplus python=3.11
    conda activate orpheusplus
    pip install -e .
    ```

3. Run `create_config.py` to create the config file
    ```python
    python create_config.py
    ```

4. Specify `orpheusplus_root_dir` in `config.yaml` (e.g., `C:\\orpheus`, `/home/my_name/orpheus`)

5. Configure `OrpheusDBPlus`
    ```
    # Make sure `mysql` service is running before configuring `OrpheusDBPlus`
    orpheusplus config
    >> Enter database name: my_database
    >> Enter user name: root (the default root user of `MySQL`, or change to a user that has been created in `MySQL`) 
    >> Enter user password:: USER_PASSWORD
    ```
    
6. TODO: Initialize a table 
    <!-- If it is successfully configured, after prompting `orpheus ls`, you should see...
    ```
    orpheus ls
    >> Connecting to the database [test_orpheus] ...
    >> The current database contains the following CVDs:
    >> No dataset has been initialized before, try init first
    ``` -->

<!-- 5. Try initializing a dataset
    ```
    # Please copy `test/` to your `orpheus_home` before trying the following command
    orpheus init test/data.csv dataset1 -s test/sample_schema.csv
    >> Connecting to the database [test_orpheus] ...
    >> Creating the dataset [dataset1] to the database [test_orpheus] ...
    >> Creating the data table using the schema provided ...
    >> Creating the version table ...
    >> Creating the index table ...
    >> Initializing the version table ...
    >> Initializing the index table ...
    >> Dataset [dataset1] has been created successful
    ``` -->
