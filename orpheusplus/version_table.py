from orpheusplus.mysql_manager import MySQLManager
from orpheusplus.operation import Operation

class VersionTable():
    def __init__(self, cnx: MySQLManager):
        self.version_table_suffix = "_orpheusplus_version"
        self.cnx = cnx
        self.table_name = None

    def init_version_table(self, table_name):
        self.table_name = table_name
        cols = "version INT UNSIGNED, rid INT UNSIGNED, PRIMARY KEY (version, rid)" 
        stmt = f"CREATE TABLE {table_name}{self.version_table_suffix} ({cols})"
        self.cnx.execute(stmt)

    def load_version_table(self, table_name):
        self.table_name = table_name

    def add_version(self, operations: Operation, version_id, parent):
        # MySQL doesn't support array
        # Insert relations as multiple new rows
        rids = list(set(self._get_parent_rids(parent)))
        rids.extend(operations.add_rids)
        rids.extend(operations.remove_rids)
        values = [(version_id, rid) for rid in rids]
        stmt = f"INSERT INTO {self.table_name}{self.version_table_suffix} VALUES (%s, %s)" 
        self.cnx.executemany(stmt, values)

    def _get_parent_rids(self, parent):
        try:
            stmt = f"SELECT rid FROM {self.table_name}{self.version_table_suffix} WHERE version = {parent}"
            result = self.cnx.execute(stmt)
            print(f"Parent version: {result}")
            return [r[0] for r in result]
        except:
            print("Parent version: []")
            return [] 