from orpheusplus.mysql_manager import MySQLManager
from orpheusplus.operation import Operation


VERSION_TABLE_SUFFIX = "_orpheusplus_version" 

class VersionTable():
    version_table_suffix = VERSION_TABLE_SUFFIX
    
    def __init__(self, cnx: MySQLManager):
        self.cnx = cnx
        self.table_name = None

    def init_version_table(self, table_name):
        self.table_name = table_name
        cols = "version INT UNSIGNED, rid INT UNSIGNED, PRIMARY KEY (version, rid)" 
        stmt = f"CREATE TABLE {table_name}{self.version_table_suffix} ({cols})"
        self.cnx.execute(stmt)

    def load_version_table(self, table_name):
        self.table_name = table_name

    def add_version(self, operation: Operation, version, parent):
        # MySQL doesn't support array
        # Insert relations as multiple new rows
        rids = set(self.get_version_rids(parent))
        for rid in operation.add_rids:
            rids.add(rid)
        for rid in operation.remove_rids:
            rids.remove(rid)
        rids = sorted(list(rids))
        values = [(version, rid) for rid in rids]
        stmt = f"INSERT INTO {self.table_name}{self.version_table_suffix} VALUES (%s, %s)" 
        self.cnx.executemany(stmt, values)
        self.cnx.commit()

    def get_version_rids(self, version):
        try:
            stmt = f"SELECT rid FROM {self.table_name}{self.version_table_suffix} WHERE version = {version}"
            result = self.cnx.execute(stmt)
            return [r[0] for r in result]
        except:
            return [] 
    
    def delete(self):
        self.cnx.execute(f"DROP TABLE {self.table_name}{self.version_table_suffix}")