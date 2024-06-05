import re
import sys

import mysql.connector

from orpheusplus import ORPHEUSPLUS_CONFIG
from orpheusplus.exceptions import MySQLError

ERROR_CODE_PATTERN = re.compile(r"(\d+)")

class MySQLManager():
    def __init__(self, user, passwd, host=None, database=None, port=None):
        self.cnx_args = {"user": user, "passwd": passwd}
        self.cnx_args["database"] = database if database is not None else None
        self.cnx_args["host"] = host if host is not None else ORPHEUSPLUS_CONFIG["host"]
        self.cnx_args["port"] = int(
            port) if port is not None else ORPHEUSPLUS_CONFIG["port"]
        self.connect_to_mysql()

    def connect_to_mysql(self, args=None):
        args = args if args is not None else self.cnx_args
        try:
            self.cnx = mysql.connector.connect(**args)
            self.cursor = self.cnx.cursor()
        except mysql.connector.Error as e:
            self._handle_known_connection_error(e)

    def execute(self, stmt):
        try:
            self.cursor.execute(stmt)
            return self.cursor.fetchall()
        except mysql.connector.Error as e:
            self._handle_known_programming_error(e)

    def executemany(self, stmt, data):
        try:
            self.cursor.executemany(stmt, data)
            return self.cursor.fetchall()
        except mysql.connector.Error as e:
            self._handle_known_programming_error(e)

    def commit(self):
        self.cnx.commit()

    def create_database(self, db_name):
        query = f"CREATE DATABASE IF NOT EXISTS {db_name}"
        result = self.execute(query)

    def _handle_known_connection_error(self, e):
        error_code = int(ERROR_CODE_PATTERN.match(str(e)).group(0))
        # Unknown database error (1049)
        if error_code == 1049:
            # Error format: 1049 (42000): Unknown database 'db_name'
            db_name = str(e).split("'")[1]
            print(f"Database `{db_name}` does not exist.")
            print(f"Create {db_name}? (y/n)")
            if input() == "y":
                del self.cnx_args["database"]
                self.connect_to_mysql()

                self.create_database(db_name)
                print(f"Database `{db_name}` created.")

                self.cnx_args["database"] = db_name
                self.connect_to_mysql()
            else:
                print("Abort database creation.")
                raise MySQLError(error_code, e.msg)
        else:
            raise MySQLError(error_code, e.msg)

    def _handle_known_programming_error(self, e):
        VERSION_TABLE_PATTERN = re.compile(r"([^\s]+)_orpheusplus")
        error_code = int(ERROR_CODE_PATTERN.match(str(e)).group(1))
        # 1146: Table doesn't exist
        if error_code == 1146:
            table_name = str(e).split("'")[1]
            if (vtable_name := VERSION_TABLE_PATTERN.match(table_name)) is not None:
                vtable_name = vtable_name.group(1)
                msg = f"Version Table `{vtable_name}` doesn't exist." 
            else:
                msg = f"Table `{table_name}` doesn't exist."
        elif error_code == 1050:
            table_name = str(e).split("'")[1]
            if (vtable_name := VERSION_TABLE_PATTERN.match(table_name)) is not None:
                vtable_name = vtable_name.group(1)
                msg = f"Version Table `{vtable_name}` already exists."
            else:
                msg = f"Table `{table_name}` already exists."
        
        else:
            msg = e.msg

        raise MySQLError(error_code, msg)
