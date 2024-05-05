import mysql.connector
from orpheusplus.exceptions import MySQLConnectionError

class MySQLManager():
    def __init__(self, user, passwd, host, database=None, port=None):
        self.cnx_args = {"user": user, "passwd": passwd, "host": host}
        if database is not None:
            self.cnx_args["database"] = database
        if port is not None:
            self.cnx_args["port"] = port
        self.connect_to_mysql()
 
    def connect_to_mysql(self, args=None):
        args = args if args is not None else self.cnx_args
        try:
            self.cnx = mysql.connector.connect(**args)
            self.cursor = self.cnx.cursor() 
        except mysql.connector.Error as e:
            self._handle_known_connection_errors(e)
    
    def execute(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchall()
    
    def create_database(self, db_name):
        query = f"CREATE DATABASE IF NOT EXISTS {db_name}"
        result = self.execute(query)
    
    def _handle_known_connection_errors(self, e):
        # Unknown database error (1049)
        if str(e).startswith("1049"):
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
                raise MySQLConnectionError(e)
        else:
            raise MySQLConnectionError(e)
