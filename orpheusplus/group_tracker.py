import json
import sys
from datetime import datetime

from orpheusplus import GROUP_DIR
from orpheusplus.connection import connect_table
from orpheusplus.exceptions import MySQLError
from orpheusplus.user_manager import UserManager
from orpheusplus.version_data import VersionData


class GroupTracker():
    def __init__(self):
        self.group_name = None
        self.group_head = None
        self.group_versions = None
        self.count = None
        self.dbname = UserManager().info["database"]
        self.user = UserManager().info["user"]
        self.table_names: list[str] = []
        self.tables: list[VersionData] = []

    def init_group(self, group_name, table_names):
        group_filepath = GROUP_DIR / self.dbname / group_name
        if group_filepath.is_file():
            print("Group already exists. Please choose another name.")
            sys.exit()
        
        (GROUP_DIR / self.dbname).mkdir(exist_ok=True)
        group_info = {"tables": [], "versions": [], "count": 0, "head": 0}
        for table_name in table_names:
            try:
                table = connect_table().load_table(table_name)
                group_info["tables"].append(table_name)
            except MySQLError:
                sys.exit()
            
        with open(GROUP_DIR / self.dbname / group_name, "w") as f:
            json.dump(group_info, f)
        
        self.group_name = group_name
        self.group_head = group_info["head"]
        self.group_versions = group_info["versions"]
        self.count = group_info["count"]
        self.table_names = group_info["tables"]
    
    def remove_group(self, group_name):
        group_filepath = GROUP_DIR / self.dbname / group_name
        if not group_filepath.is_file():
            print(f"Group `{group_name}` does not exist.")
            sys.exit()

        group_filepath.unlink()

    def load_group(self, group_name):
        group_filepath = GROUP_DIR / self.dbname / group_name
        if not group_filepath.is_file():
            print("Group does not exist.")
            sys.exit()

        with open(group_filepath, "r") as f:
            group_info = json.load(f)

        self.group_name = group_name
        self.group_head = group_info["head"]
        self.group_versions = group_info["versions"]
        self.count = group_info["count"]
        self.table_names = group_info["tables"]
    
    def _save_group(self):
        group_filepath = GROUP_DIR / self.dbname / self.group_name
        group_info = {"head": self.group_head,
                      "tables": self.table_names,
                      "versions": self.group_versions,
                      "count": self.count}

        with open(group_filepath, "w") as f:
            json.dump(group_info, f)
    
    def commit(self, **commit_info):
        now = datetime.now()
        message = commit_info["msg"]
        now = commit_info["now"]
        group_message = f"group `{self.group_name}`: {message}"
        group_version_info = {"tables": {}, "author": self.user, "date": now, "message": message}

        if not self.tables:
            self._load_table()
        
        has_revision = False
        for table in self.tables:
            print(f"Committing {table.table_name}:")
            before_table_head = table.get_current_version()
            table.commit(msg=group_message, now=now)
            table_head = table.get_current_version()
            table_name = table.table_name
            group_version_info["tables"][table_name] = table_head

            if before_table_head != table_head:
                has_revision = True

        if has_revision:
            self.count += 1     
            self.group_head = self.count
            self.group_versions.append((self.count, group_version_info))
            self._save_group()
            print(f"Create group version {self.get_current_version()}")
        else:
            print("No changes made to the current group version.")
    
    def _load_table(self):
        for table_name in self.table_names:
            table = connect_table()
            table.load_table(table_name)
            self.tables.append(table)
    
    def parse_log(self):
        # parsed_format: {"version": ..., "author": ..., "date": ..., "message": ...}
        log = [{"version": version, "author": info["author"], "date": info["date"],
                "message": info["message"]} for version, info in self.group_versions]
        
        return log
        
    def checkout(self, version):
        self.group_head = version
        for table in self.tables:
            print(f"Checking out {table.table_name}:")
            table.checkout(self.group_versions[self.group_head][table.table_name])
    
    def get_current_version(self):
        return self.group_head