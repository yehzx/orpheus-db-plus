"""
Uses split-by-rlist approach
Reference:
ORPHEUSDB: Bolt-on Versioning for Relational Databases
(https://www.vldb.org/pvldb/vol10/p1130-huang.pdf)
"""
import csv
from collections import OrderedDict
from orpheusplus.mysql_manager import MySQLManager
from orpheusplus.version_graph import VersionGraph
from orpheusplus.version_table import VersionTable
from orpheusplus.operation import Operation


class VersionData():
    def __init__(self, cnx: MySQLManager):
        self.cnx = cnx
        self.table_suffix = "_orpheusplus"
        self.table_name = None
        self.table_structure = None
        self.version_graph = None
        self.operations = Operation()

    def init_table(self, table_name, table_structure):
        self.table_name = table_name
        table_structure = self._parse_csv_structure(table_structure)

        # rid: as an index for each relation
        cols = "rid INT AUTO_INCREMENT PRIMARY KEY, " + table_structure
        stmt = f"CREATE TABLE {table_name}{self.table_suffix} ({cols})"
        self.cnx.execute(stmt) 
        # rid starts from 1
        stmt = f"ALTER TABLE {table_name}{self.table_suffix} AUTO_INCREMENT = 1"
        self.cnx.execute(stmt)

        # The graph for tracking version dependency
        self.create_version_graph()
        self.table_structure = self._get_table_types()
        print(f"Table `{table_name}` initialized successfully.")

    def create_version_graph(self):
        self.version_graph = VersionGraph(self.cnx)
        self.version_graph.init_version_graph(self.table_name)
    
    def load_table(self, table_name):
        self.table_name = table_name
        self.table_structure = self._get_table_types()        
        self.version_graph = VersionGraph(self.cnx)
        self.version_graph.load_version_graph(table_name)

    def insert(self, filepath): 
        data = self._parse_csv_data(filepath)
        max_rid = self._get_max_rid()
        if data is None:
            return 
        arg_stmt = self._arg_stmt(self.table_structure)
        stmt = f"INSERT INTO {self.table_name}{self.table_suffix} VALUES {arg_stmt}"
        self.cnx.executemany(stmt, data)
        # TODO: get `rid` of the inserted data, and pass it to version_table and version_graph
        self.operations.insert(start_rid=max_rid + 1, num_rids=len(data))
        self.operations.parse()

        self.version_graph.add_version(operations=self.operations)
        self.cnx.commit()

    @staticmethod 
    def _arg_stmt(table_structure):
        stmt = "(" + "%s, " * len(table_structure)
        stmt = stmt[:-2] + ")"
        return stmt

    
    def _get_max_rid(self):
        stmt = f"SELECT MAX(rid) FROM {self.table_name}{self.table_suffix}"
        result = self.cnx.execute(stmt)
        try:
            max_rid = int(result[0][0])
        # This raises when the table is empty
        except TypeError:
            max_rid = 0
        return max_rid
    
    def _get_table_types(self):
        schema = self.cnx.execute(f"SHOW COLUMNS FROM `{self.table_name}{self.table_suffix}`")
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
                row.insert(0, None)
                data.append(tuple(row))
        return data    