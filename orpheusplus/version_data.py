"""
Uses split-by-rlist approach
Reference:
ORPHEUSDB: Bolt-on Versioning for Relational Databases
(https://www.vldb.org/pvldb/vol10/p1130-huang.pdf)
"""
import csv
import sys
from collections import OrderedDict

from orpheusplus.utils import parse_commit, match_column_order, reorder_data, parse_csv_data, parse_csv_structure, parse_table_types
from orpheusplus import LOG_DIR
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
        self.db_name = cnx.cnx_args["database"]
        self.table_structure = None
        self.version_graph = None
        self.operation = None

    def init_table(self, table_name, table_structure_path):
        self.table_name = table_name
        table_structure = parse_csv_structure(table_structure_path)

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
        self.version_graph.init_version_graph(self.db_name, self.table_name)
    
    def _create_operation(self):
        self.operation = Operation()
        self.operation.init_operation(self.db_name, self.table_name, self.get_current_version())

    def load_table(self, table_name):
        self.table_name = table_name
        self.table_structure = self._get_table_types()        
        self.version_graph = VersionGraph(self.cnx)
        self.version_graph.load_version_graph(self.db_name, table_name)
        self.operation = Operation()
        self.operation.load_operation(self.db_name, table_name, self.get_current_version())
    
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
            data = parse_csv_data(filepath)
            self.insert(data)
        elif operation == "delete":
            data = parse_csv_data(filepath)
            self.delete(data)
        elif operation == "update":
            old_data = parse_csv_data(filepath[0])
            new_data = parse_csv_data(filepath[1])
            self.update(old_data, new_data)
    
    def from_parsed_data(self, operation, attrs):
        if operation == "insert":
            data = self._match_table_column(attrs["column"], attrs["data"])
            self.insert(data)
        elif operation == "delete":
            self.delete_from_sql(attrs["where"])
        elif operation == "update":
            self.update_from_sql(attrs["where"], attrs["set"])
    
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
    
    @staticmethod
    def add_rid(data, max_rid):
        for row in data:
            max_rid += 1
            row.insert(0, max_rid)
        return data
    
    @staticmethod 
    def _arg_stmt(table_structure, with_rid=True):
        length = len(table_structure) if with_rid else len(table_structure) - 1
        stmt = f"({', '.join(['%s'] * length)})"
        return stmt
    
    def delete(self, data, update=False):
        if len(data) == 0:
            return            
        cols = list(self.table_structure.keys())
        cols.remove("rid")
        data_stmt = tuple(tuple(each) for each in data)
        stmt = (
            f"SELECT rid FROM {self.table_name}{self.head_suffix} "
            f"WHERE ({', '.join(cols)}) IN {data_stmt}"
        )   
        result = self.cnx.execute(stmt)
        rids = [each[0] for each in result]

        # Behavior of `Update`
        # Duplicated entries will be completely removed but new entries will be added uniquely.
        if update:
            if len(data) != len(rids):
                print("Duplicated entries detected. After `UPDATE`, new entries will be unique but not duplicated.")
                ans = input("Proceed to update? (y/n)\n")
                if ans != "y":
                    print("Operation cancelled.")
                    sys.exit()
            
        stmt = (
            f"DELETE FROM {self.table_name}{self.head_suffix} "
            f"WHERE ({', '.join(cols)}) IN {data_stmt}"
        )
        self.cnx.execute(stmt)
        self.cnx.commit()
        self.operation.delete(rids)
    
    def delete_from_sql(self, where, return_data=False):
        stmt = (
            f"SELECT rid FROM {self.table_name}{self.head_suffix} "
            f"{where}"
        )   
        result = self.cnx.execute(stmt)
        rids = [each[0] for each in result]
        stmt = (
            f"DELETE FROM {self.table_name}{self.head_suffix} "
            f"{where}"
        )
        self.cnx.execute(stmt)            
        self.cnx.commit()
        self.operation.delete(rids)

        if return_data:
            data = [each[1:] for each in result]
            return data
        else:
            return None

    def update(self, old_data, new_data):
        if len(old_data) != len(new_data) or len(old_data) == 0:
            return
        self.delete(old_data, update=True)
        self.insert(new_data)
        self.operation.update()
    
    def update_from_sql(self, where, set):
        data = self.delete_from_sql(where, return_data=True)
        data = [list(row) for row in data]
        data = self._update_data(data, set)
        self.insert(data)
        self.operation.update()
    
    def _update_data(self, data, set: dict):
        cols = list(self.table_structure.keys())
        cols.remove("rid")
        for idx, col in enumerate(cols):
            value = set.get(col)
            if value is None:
                continue
            else:
                for row in data:
                    row[idx] = value
        return data
    
    def commit(self, **commit_info):
        self.operation.parse()
        cols = list(self.table_structure.keys())
        cols.remove("rid")
        add_rids = tuple(rid for rid in self.operation.add_rids)
        remove_rids = tuple(rid for rid in self.operation.remove_rids)
        if not (add_rids or remove_rids):
            print("No revision to the last version. Abort commit.")
            sys.exit()
        if add_rids:
            stmt = (
                f"INSERT INTO {self.table_name}{self.data_table_suffix} "
                f"SELECT * FROM {self.table_name}{self.head_suffix} "
                f"WHERE rid IN {add_rids}"
            )
            self.cnx.execute(stmt)
            self.cnx.commit()
        self.version_graph.add_version(self.operation, **commit_info)
        self._create_operation()
        self._save_log(**commit_info)
    
    def remove(self, keep_current=False):
        self.version_graph.remove()
        self.operation.remove()
        self._remove_log()
        self.cnx.execute(f"DROP TABLE {self.table_name}{self.data_table_suffix}")
        if keep_current:
            self.cnx.execute(f"RENAME TABLE {self.table_name}{self.head_suffix} TO {self.table_name}")
        else:
            self.cnx.execute(f"DROP TABLE {self.table_name}{self.head_suffix}")

    def parse_log(self):
        parsed_commits = []
        with open(LOG_DIR / f"{self.db_name}/{self.table_name}") as f:
            commits = f.read().split("\n\n")
            for commit in reversed(commits):
                if not commit:
                    continue
                parsed_commits.append(parse_commit(commit))
        return parsed_commits

    def _save_log(self, **commit_info):
        log_path = LOG_DIR / f"{self.db_name}/{self.table_name}"
        log_path.parent.mkdir(exist_ok=True)

        with open(log_path, "a") as f:
            f.write(
                f"commit {self.version_graph.head}\n"
                f"Author: {self.cnx.cnx_args['user']}\n"
                f"Date: {commit_info['now']}\n"
                f"Message: {commit_info['msg']}\n\n"
            )

    def _remove_log(self):
        log_path = LOG_DIR / f"{self.db_name}/{self.table_name}"
        try:
            log_path.unlink()
        except FileNotFoundError:
            pass
        try:
            log_path.parent.rmdir()
        except OSError:
            pass

    def get_current_version(self):
        return self.version_graph.head
    
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

    def _match_table_column(self, cols, data):
        if cols is None:
            return data
        if len(data[0]) != len(cols):
            raise ValueError("Column value length mismatch")
        table_cols = list(self._get_table_types().keys())
        order = match_column_order(table_cols, cols)
        reordered_data = reorder_data(data, order)
        return reordered_data
    
    def _get_table_types(self):
        schema = self.cnx.execute(f"SHOW COLUMNS FROM `{self.table_name}{self.data_table_suffix}`")
        type_dict = parse_table_types(schema) 
        return type_dict
    