import mysql.connector


class MySQLManager():
    def __init__(self, user, passwd, host, database=None, port=None):
        cnx_args = {"user": user, "passwd": passwd, "host": host}
        if database is not None:
            cnx_args["database"] = database
        if port is not None:
            cnx_args["port"] = port
        self.cnx = mysql.connector.connect(**cnx_args)
        self.cursor = self.cnx.cursor() 
    
    def execute(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchall()
    