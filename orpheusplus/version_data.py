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


class VersionData():
    def __init__(self, cnx: MySQLManager):
        self.cnx = cnx
        self.table_suffix = "_orpheusplus"
        self.table_name = None
        self.table_structure = None
        self.version_table: VersionTable = None
        self.version_graph: VersionGraph = None

    def init_table(self, table_name, table_structure):
        self.table_name = table_name
        table_structure = self._parse_csv_structure(table_structure)

        # rid: as an index for each relation
        cols = "rid INT AUTO_INCREMENT PRIMARY KEY, " + table_structure
        stmt = f"CREATE TABLE {table_name}{self.table_suffix} ({cols})"
        self.cnx.execute(stmt) 
        # The table for tracking different versions of a version table
        self.create_version_table()
        # The graph for tracking version dependency
        self.create_version_graph()
        self.table_structure = self._get_table_types()
        print(f"Table `{table_name}` initialized successfully.")
    
    def create_version_table(self):
        self.version_table = VersionTable(self.table_name)
        self.version_table.init_version_table()
        pass

    def create_version_graph(self):
        self.version_graph = VersionGraph(self.table_name)
        self.version_graph.init_version_graph()
    
    def load_table(self, table_name):
        self.table_name = table_name
        self.table_structure = self._get_table_types()        

    def insert(self, filepath): 
        data = self._parse_csv_data(filepath, self.table_structure)
        if data is None:
            return 
        stmt = f"INSERT INTO {self.table_name}{self.table_suffix} VALUES {data}"
        self.cnx.execute(stmt)
        # TODO: get `rid` of the inserted data, and pass it to version_table and version_graph
        self.version_graph.add_version()
        self.version_table.add_version()
        self.cnx.commit()
    
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
    def _parse_csv_data(filepath, table_structure):
        data = []
        cast_types = VersionData._determine_cast_types(table_structure)
        with open(filepath, newline="") as f:
            reader = csv.reader(f)
            for row in reader:
                # Handle data type
                row = [f"CAST({value} AS {cast_type})" for value, cast_type in zip(row, cast_types)]
                # rid in table
                row.insert(0, "NULL")
                data.append(tuple(row))
        return data    

    @staticmethod 
    def _determine_cast_types(type_dict):
        cast_types = []
        for key, value in type_dict.items():
            if value.endswith("unsigned"):
                cast_types.append("UNSIGNED")
            elif value.endswith("signed"):
                cast_types.append("SIGNED")
            elif value in ["float", "double", "decimal", "dec"]:
                cast_types.append("DECIMAL")
            elif value.startswith("int") or value.endswith("int") or value.startswith("bool"):
                cast_types.append("INT")
            elif value == "date":
                cast_types.append("DATE")
            elif value == "datetime":
                cast_types.append("DATETIME")
            elif value == "time":
                cast_types.append("TIME")
            else:
                cast_types.append("CHAR")
        return cast_types