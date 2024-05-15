"""
Uses split-by-rlist approach
Reference:
ORPHEUSDB: Bolt-on Versioning for Relational Databases
(https://www.vldb.org/pvldb/vol10/p1130-huang.pdf)
"""
import csv
from collections import OrderedDict

from orpheusplus.mysql_manager import MySQLManager
from orpheusplus.operation import Operation
from orpheusplus.version_graph import VersionGraph

DATA_TABLE_SUFFIX = "_orpheusplus"
HEAD_SUFFIX = "_orpheusplus_head"
class VersionData():
    data_table_suffix = DATA_TABLE_SUFFIX
    # The table used to store the current version of the table
    head_suffix = HEAD_SUFFIX
    
    def __init__(self, cnx: MySQLManager):
        self.cnx = cnx
        self.table_name = None
        self.table_structure = None
        self.version_graph = None
        self.operation = None

    def init_table(self, table_name, table_structure):
        self.table_name = table_name
        table_structure = self._parse_csv_structure(table_structure)

        # Initialize data table
        # rid: as an index for each relation
        cols = "rid INT PRIMARY KEY, " + table_structure
        stmt = f"CREATE TABLE {table_name}{self.data_table_suffix} ({cols})"
        self.cnx.execute(stmt) 

        # Initialize the table for the current version
        stmt = f"CREATE TABLE {table_name}{self.head_suffix} LIKE {table_name}{self.data_table_suffix}"
        self.cnx.execute(stmt)

        # rid starts from 1 in data table
        stmt = f"ALTER TABLE {table_name}{self.data_table_suffix} AUTO_INCREMENT = 1"
        self.cnx.execute(stmt)

        # The graph for tracking version dependency
        self._create_version_graph()
        self._create_operation()
        self.table_structure = self._get_table_types()
        print(f"Table `{table_name}` initialized successfully.")

    def _create_version_graph(self):
        self.version_graph = VersionGraph(self.cnx)
        self.version_graph.init_version_graph(self.table_name)
    
    def _create_operation(self):
        self.operation = Operation()
        self.operation.init_operation(self.table_name, self.get_current_version())
    
    def load_table(self, table_name):
        self.table_name = table_name
        self.table_structure = self._get_table_types()        
        self.version_graph = VersionGraph(self.cnx)
        self.version_graph.load_version_graph(table_name)
        self.operation = Operation()
        self.operation.load_operation(self.table_name, self.get_current_version())
    
    def checkout(self, version):
        if version == self.get_current_version():
            print("Discard all changes.")
            self.operation.clear()
        elif not self.operation.is_empty():
            print("Please commit changes or discard them by `checkout head`")
            return
        rids = self.version_graph.switch_version(version)
        rids = [(rid,) for rid in rids]
        stmt = f"DELETE FROM {self.table_name}{self.head_suffix}"
        self.cnx.execute(stmt)
        stmt = (
            f"INSERT INTO {self.table_name}{self.head_suffix} "
            f"SELECT * FROM {self.table_name}{self.data_table_suffix} "
            f"WHERE rid = %s"
        )
        self.cnx.executemany(stmt, rids)
        self.cnx.commit()
    
    def from_file(self, operation, filepath):
        if operation == "insert":
            data = self._parse_csv_data(filepath)
            self.insert(data)
        elif operation == "delete":
            data = self._parse_csv_data(filepath)
            self.delete(data)
        elif operation == "update":
            old_data = self._parse_csv_data(filepath[0])
            new_data = self._parse_csv_data(filepath[1])
            self.update(old_data, new_data)
    
    def add_rid(self, data, max_rid):
        for row in data:
            max_rid += 1
            row.insert(0, max_rid)
        return data
            
    def insert(self, data): 
        if len(data) == 0:
            return
        max_rid = self._get_max_rid()
        arg_stmt = self._arg_stmt(self.table_structure, with_rid=True)   
        data = self.add_rid(data, max_rid)
        stmt = f"INSERT INTO {self.table_name}{self.head_suffix} VALUES {arg_stmt}"
        self.cnx.executemany(stmt, data)
        self.cnx.commit()
        self.operation.insert(start_rid=max_rid + 1, num_rids=len(data))
    
    def delete(self, data):
        if len(data) == 0:
            return
        arg_stmt = self._arg_stmt(self.table_structure, with_rid=False)
        cols = list(self.table_structure.keys())
        cols.remove("rid")
        data_stmt = tuple(tuple(each) for each in data)
        stmt = (
            f"SELECT rid FROM {self.table_name}{self.head_suffix} "
            f"WHERE ({', '.join(cols)}) IN {data_stmt}"
        )   
        rids = self.cnx.execute(stmt)
        rids = [rid[0] for rid in rids]

        stmt = (
            f"DELETE FROM {self.table_name}{self.head_suffix} "
            f"WHERE ({', '.join(cols)}) IN {data_stmt}"
        )
        self.cnx.execute(stmt)
        self.cnx.commit()
        self.operation.delete(rids)

    def update(self, old_data, new_data):
        if len(old_data) != len(new_data) or len(old_data) == 0:
            return
        self.delete(old_data)
        self.insert(new_data)
    
    def commit(self, *commit_info):
        self.operation.parse()
        cols = list(self.table_structure.keys())
        cols.remove("rid")
        rids = tuple(rid for rid in self.operation.add_rids)
        if not rids:
            print("No revision to the last version. Abort commit.")
            return
        stmt = (
            f"INSERT INTO {self.table_name}{self.data_table_suffix} "
            f"SELECT * FROM {self.table_name}{self.head_suffix} "
            f"WHERE rid IN {rids}"
        )
        self.cnx.execute(stmt)
        self.cnx.commit()
        self.version_graph.add_version(self.operation, commit_info)
        self._create_operation()
    
    def remove(self):
        self.version_graph.remove()
        self.operation.remove()
        self.cnx.execute(f"DROP TABLE {self.table_name}{self.data_table_suffix}")
        self.cnx.execute(f"DROP TABLE {self.table_name}{self.head_suffix}")

    def get_current_version(self):
        return self.version_graph.head
    
    @staticmethod 
    def _arg_stmt(table_structure, with_rid=True):
        length = len(table_structure) if with_rid else len(table_structure) - 1
        stmt = f"({', '.join(['%s'] * length)})"
        return stmt
    
    def _get_max_rid(self):
        stmt = f"SELECT MAX(rid) FROM {self.table_name}{self.data_table_suffix}"
        result_1 = self.cnx.execute(stmt)
        stmt = f"SELECT MAX(rid) FROM {self.table_name}{self.head_suffix}"
        result_2 = self.cnx.execute(stmt)
        try:
            max_rid_1 = int(result_1[0][0])
        except TypeError:
            max_rid_1 = 0
        try:
            max_rid_2 = int(result_2[0][0])
        except TypeError:
            max_rid_2 = 0
        max_rid = max(max_rid_1, max_rid_2)
        return max_rid
    
    def _get_table_types(self):
        schema = self.cnx.execute(f"SHOW COLUMNS FROM `{self.table_name}{self.data_table_suffix}`")
        type_dict = self._parse_table_types(schema) 
        return type_dict
    
    @staticmethod
    def _parse_table_types(schema):
        type_dict = OrderedDict()
        for col in schema:
            type_dict[col[0]] = col[1].decode()
        return type_dict

    @staticmethod
    def _parse_csv_structure(filepath):
        stmt = ""
        with open(filepath, newline="") as f:
            reader = csv.reader(f)
            for row in reader:
                stmt += " ".join(row)
                stmt += ", "
            stmt = stmt.rstrip(", ")
        return stmt 

    @staticmethod
    def _parse_csv_data(filepath):
        data = []
        with open(filepath, newline="") as f:
            reader = csv.reader(f)
            for row in reader:
                data.append(row)
        return data    
