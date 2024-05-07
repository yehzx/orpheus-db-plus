from orpheusplus.mysql_manager import MySQLManager


class VersionTable():
    def __init__(self, cnx: MySQLManager):
        self.version_table_suffix = "_orpheusplus_version"
        self.cnx = cnx
        self.table_name = None

    def init_version_table(self, table_name):
        self.table_name = table_name
        cols = "version INT, rid INT, PRIMARY KEY (version, rid)" 
        stmt = f"CREATE TABLE {table_name}{self.version_table_suffix} ({cols})"
        self.cnx.execute(stmt)

    def load_version_graph(self, table_name):
        self.table_name = table_name

    def add_version(self):
        # MySQL doesn't support array
        # TODO: insert relations as multiple new rows
        # stmt = f"INSERT INTO {self.table_name}{self.version_table_suffix} VALUES ()"
        # self.cnx.execute(stmt)
        pass