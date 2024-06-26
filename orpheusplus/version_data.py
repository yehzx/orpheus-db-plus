import csv
import sys
import time
from datetime import datetime
from pathlib import Path
from orpheusplus import LOG_DIR
from orpheusplus.mysql_manager import MySQLManager
from orpheusplus.operation import Operation
from orpheusplus.utils import (match_column_order, parse_commit,
                               parse_csv_data, parse_csv_structure,
                               parse_table_types, reorder_data)
from orpheusplus.version_graph import VersionGraph
from orpheusplus.exceptions import NonEmptyOperation

DATA_TABLE_SUFFIX = "_orpheusplus"
HEAD_SUFFIX = "_orpheusplus_head"


class VersionData():
    data_table_suffix = DATA_TABLE_SUFFIX
    # The table used to store the current version of the table

    def __init__(self, cnx: MySQLManager):
        self.cnx = cnx
        self.table_name = None
        self.user = cnx.cnx_args["user"]
        self.db_name = cnx.cnx_args["database"]
        self.table_structure = None
        self.version_graph = None
        self.operation = None
        VersionData.head_suffix = HEAD_SUFFIX + "_" + self.user

    def init_table(self, table_name, table_structure_path):
        self.table_name = table_name
        table_structure = parse_csv_structure(table_structure_path)

        # Initialize data table
        # rid: as an index for each relation
        stmt_structure = ", ".join(
            [f"`{each_col[0]}` {each_col[1]}" for each_col in table_structure])
        cols = "rid INT PRIMARY KEY, " + stmt_structure
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
        self._init_user_operation()
        self.table_structure = self._get_table_types()

    def _create_version_graph(self):
        self.version_graph = VersionGraph(self.cnx)
        self.version_graph.init_version_graph(self.db_name, self.table_name)

    def _init_user_operation(self):
        self.operation = Operation()
        try:
            self.operation.load_operation(self.db_name, self.table_name,
                                          self.user)
        except FileNotFoundError:
            self.operation.init_operation(self.db_name, self.table_name, 
                self.version_graph.version_count, self.user)

    def from_table(self, from_table, to_table):
        table_structure = self._get_table_types(from_table)
        rows = [[col, col_type] for col, col_type in table_structure.items()]
        try:
            temp_csv_filepath = Path("./temp_table_structure.csv")
            with open(temp_csv_filepath, "w", newline="",
                      encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerows(rows)
            self.init_table(to_table, temp_csv_filepath)
        except Exception as e:
            print(e)
        temp_csv_filepath.unlink()

    def load_table(self, table_name):
        self.table_name = table_name
        self.table_structure = self._get_table_types()
        self.version_graph = VersionGraph(self.cnx)
        self.version_graph.load_version_graph(self.db_name, table_name)
        self.operation = Operation()
        try:
            self.operation.load_operation(self.db_name, self.table_name,
                                          self.user)
        except FileNotFoundError:
            self.operation.init_operation(self.db_name, self.table_name, 
                                          self.version_graph.version_count, self.user)
            # Initialize the table for the current version
            stmt = f"CREATE TABLE {table_name}{self.head_suffix} LIKE {table_name}{self.data_table_suffix}"
            self.cnx.execute(stmt)
            rids = self.version_graph.version_table.get_version_rids(self.version_graph.version_count)
            stmt = (f"INSERT INTO {self.table_name}{self.head_suffix} "
                f"SELECT * FROM {self.table_name}{self.data_table_suffix} "
                f"WHERE rid = %s")
            rids = [(rid, ) for rid in rids]
            self.cnx.executemany(stmt, rids)
            self.cnx.commit()
            self.version_graph.head = self.version_graph.version_count
        except:
            print("Corrupted version table.")
            ans = input(
                "Revert back to a normal table or drop it? (y/n/drop)\n")
            if ans == "y":
                self.operation.init_operation(self.db_name, table_name,
                                              self.get_current_version(), self.user)
                self.remove(keep_current=True)
                print(
                    f"Turn version table `{self.table_name} into a normal table.`"
                )
            elif ans == "drop":
                self.remove()
                print(f"Drop `{self.table_name}`")
            else:
                print("Abort loading version table.")
            sys.exit()


    def checkout(self, version):
        if version == self.get_current_version():
            print("Discard all changes.")
            self.operation.clear()
        elif not self.operation.is_empty():
            raise NonEmptyOperation(
                "Please commit changes or discard them by `checkout head`")

        if int(version) > self.version_graph.version_count:
            print(f"Version {version} doesn't exist.")
            sys.exit()

        self.operation.switch_user_version_head(version)
        rids = self.version_graph.switch_version(version)
        rids = [(rid, ) for rid in rids]
        stmt = f"DELETE FROM {self.table_name}{self.head_suffix}"
        self.cnx.execute(stmt)
        stmt = (f"INSERT INTO {self.table_name}{self.head_suffix} "
                f"SELECT * FROM {self.table_name}{self.data_table_suffix} "
                f"WHERE rid = %s")
        self.cnx.executemany(stmt, rids)
        self.cnx.commit()

    def diff(self, version_1, version_2):
        rids_1 = self.version_graph.version_table.get_version_rids(version_1)
        rids_2 = self.version_graph.version_table.get_version_rids(version_2)
        rids_1 = set(rids_1)
        rids_2 = set(rids_2)
        v1_diff_v2 = self.select_by_rid(list(rids_1 - rids_2))
        v2_diff_v1 = self.select_by_rid(list(rids_2 - rids_1))
        fields = [col[0] for col in self.cnx.cursor.description]
        # Remove `rid`
        fields = fields[1:]

        return {
            "fields": fields,
            "v1_diff_v2": v1_diff_v2,
            "v2_diff_v1": v2_diff_v1
        }

    def merge(self, version, resolved_file=None):
        if not self.operation.is_empty():
            print(
                "Please commit changes or discard them by `checkout head` before merging"
            )
            sys.exit()

        changes_head, changes_version = self.version_graph.gather_changes(version)
        conflicts = self.version_graph.gather_conflicts(changes_head, changes_version)
        changed_rids = set(list(changes_head.keys()) + list(changes_version.keys()))
        keep_rids = set()
        delete_rids = set()
        if conflicts:
            if resolved_file is None:
                self._write_conflict_file(version, conflicts)
                sys.exit()
            else:
                try:
                    parsed = self._parse_keep_or_delete(resolved_file, conflicts, version)
                    keep_rids = set(parsed[0])
                    delete_rids = set(parsed[1])
                    print("Resolve conflicts.")
                except:
                    raise Exception("Invalid resolved file. Abort merge.")

        rids_in_head = self.version_graph.version_table.get_version_rids(
            self.version_graph.head)
        rids_in_version = self.version_graph.version_table.get_version_rids(
            version)
        next_version = self.version_graph.version_count + 1

        commit_info = {
            "msg":
            f"Create version {next_version} (merge version {self.version_graph.head} and version {version}).",
            "now": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        total_rids = set(rids_in_version + rids_in_head)
        total_rids = total_rids - changed_rids - delete_rids
        rids_in_head = set(rids_in_head)
        rids_in_version = set(rids_in_version)

        # Head
        rids_to_add = total_rids - rids_in_head
        rids_to_delete = rids_in_head - total_rids
        self.operation.insert(rids_to_add)
        self.operation.delete(rids_to_delete)
        self.operation.parse()
        self.version_graph.add_version(self.operation, **commit_info)

        # Version
        rids_to_add = total_rids - rids_in_version
        rids_to_delete = rids_in_version - total_rids
        operation = Operation()
        try:
            operation.load_operation(self.db_name, self.table_name, version)
        except FileNotFoundError:
            operation.init_operation(self.db_name, self.table_name, version)
        operation.insert(rids_to_add)
        operation.delete(rids_to_delete)
        operation.parse()
        self.version_graph.merge_version(operation,
                                         from_version=version,
                                         to_version=next_version,
                                         **commit_info)
        self._save_log(**commit_info)

    def _write_conflict_file(self, version, conflicts):
        cols = list(self.table_structure.keys())
        cols.remove("rid")
        write_cols = ["select_head"] + cols + ["timestamp"] + \
            [f"select_{version}"] + cols + ["timestamp"]
        conflict_path = f"./conflicts_{int(time.time())}.csv"
        with open(conflict_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(write_cols)
            for (head_rid, head_timestamp), (version_rid, version_timestamp) in conflicts.values():
                if head_rid is None:
                    head_data = [""] * len(cols)
                else:
                    head_data = list(self.select_by_rid(head_rid)[0])
                if version_rid is None:
                    version_data = [""] * len(cols)
                else:
                    version_data = list(self.select_by_rid(version_rid)[0])
                select = (1, 0) if head_timestamp > version_timestamp else (0, 1)
                writer.writerow([select[0]] + head_data + [head_timestamp] +
                                [select[1]] + version_data + [version_timestamp])
        print(f"Find conflicts in {len(conflicts)} rows.")
        print(
            f"Save conflicts to {conflict_path}. Please resolve it by marking the rows as `1`."
        )
        print(f"Try `merge` again with resolved file.")

    @staticmethod
    def _parse_keep_or_delete(resolved_file, conflicts, version):
        keep_rids = []
        delete_rids = []
        with open(resolved_file, "r") as f:
            reader = csv.reader(f)
            headers = next(reader)
            select_head_idx = headers.index("select_head")
            select_version_idx = headers.index(f"select_{version}")
            for idx, row in enumerate(reader):
                conflict = conflicts[idx + 1]
                (head_rid, _), (version_rid, _) = conflict
                if head_rid is not None:
                    if int(row[select_head_idx]) == 1:
                        keep_rids.append(head_rid)
                    else:
                        delete_rids.append(head_rid)
                if version_rid is not None:
                    if int(row[select_version_idx]) == 1:
                        keep_rids.append(version_rid)
                    else:
                        delete_rids.append(version_rid)
        return keep_rids, delete_rids

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
            data = self._match_table_column(attrs["columns"], attrs["data"])
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
        self.operation.insert(list(range(max_rid + 1,
                                         max_rid + 1 + len(data))))

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
        delete_rids = []
        total_rids = []
        for idx, each_stmt in enumerate(data_stmt):
            stmt = (f"SELECT rid FROM {self.table_name}{self.head_suffix} "
                    f"WHERE ({', '.join(cols)}) = {each_stmt}")
            result = self.cnx.execute(stmt)
            rids = [each[0] for each in result]
            delete_rids.append(rids)
            total_rids.extend(rids)

        # Behavior of `Update`
        # Duplicated entries will be completely removed but new entries will be added uniquely.
        if update:
            if len(data) != len(total_rids):
                print(
                    "Duplicated entries detected. After `UPDATE`, new entries will be unique but not duplicated."
                )
                ans = input("Proceed to update? (y/n)\n")
                if ans != "y":
                    print("Operation cancelled.")
                    sys.exit()

        stmt = (f"DELETE FROM {self.table_name}{self.head_suffix} "
                f"WHERE ({', '.join(cols)}) IN {data_stmt}")
        self.cnx.execute(stmt)
        self.cnx.commit()
        self.operation.delete(total_rids)
        return delete_rids

    def select_by_rid(self, rid):
        if not rid:
            return []
        if isinstance(rid, int):
            stmt = (f"SELECT * FROM {self.table_name}{self.data_table_suffix} "
                    f"WHERE rid = {rid}")
        elif isinstance(rid, list):
            stmt = (f"SELECT * FROM {self.table_name}{self.data_table_suffix} "
                    f"WHERE rid IN {tuple(rid)}")
        result = self.cnx.execute(stmt)
        return [each[1:] for each in result]

    def delete_from_sql(self, where, return_data=False):
        # TODO: split where stmt if not = but IN
        # TODO: rids should be a double layered list as in delete
        stmt = (f"SELECT * FROM {self.table_name}{self.head_suffix} "
                f"{where}")
        result = self.cnx.execute(stmt)
        rids = [each[0] for each in result]
        stmt = (f"DELETE FROM {self.table_name}{self.head_suffix} "
                f"{where}")
        self.cnx.execute(stmt)
        self.cnx.commit()
        self.operation.delete(rids)

        if return_data:
            data = [each[1:] for each in result]
            return rids, data
        else:
            return rids

    def update(self, old_data, new_data):
        if len(old_data) != len(new_data) or len(old_data) == 0:
            return
        delete_rids = self.delete(old_data, update=True)
        self.insert(new_data)
        insert_rids = self._get_insert_rids()
        self.operation.update(delete_rids, insert_rids)

    def update_from_sql(self, where, set):
        delete_rids, data = self.delete_from_sql(where, return_data=True)
        data = [list(row) for row in data]
        data = self._update_data(data, set)
        self.insert(data)
        insert_rids = self._get_insert_rids()
        self.operation.update(delete_rids, insert_rids)

    def _get_insert_rids(self):
        assert self.operation.stmts[-1][0] == "insert", self.operation.stmts
        _, (start, num), _ = self.operation.stmts[-1]
        insert_rids = list(range(start, start + num))
        return insert_rids

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
            return

        if add_rids:
            stmt = (f"INSERT INTO {self.table_name}{self.data_table_suffix} "
                    f"SELECT * FROM {self.table_name}{self.head_suffix} ")
            if len(add_rids) == 1:
                add_rid = add_rids[0]
                where_stmt = f"WHERE rid = {add_rid}"
            elif len(add_rids) > 1:
                where_stmt = f"WHERE rid IN {add_rids}"
            stmt += where_stmt

            self.cnx.execute(stmt)
            self.cnx.commit()
        self.version_graph.add_version(self.operation, **commit_info)
        self._save_log(**commit_info)

        print(f"Create version {self.get_current_version()}")

    def remove(self, keep_current=False):
        if keep_current:
            self.cnx.execute(
                f"RENAME TABLE {self.table_name}{self.head_suffix} TO {self.table_name}"
            )
            self.cnx.execute(f"ALTER TABLE {self.table_name} DROP COLUMN rid")
        else:
            self.cnx.execute(f"DROP TABLE {self.table_name}{self.head_suffix}")
        self.cnx.execute(
            f"DROP TABLE {self.table_name}{self.data_table_suffix}")
        self.version_graph.remove()
        self.operation.remove()
        self._remove_log()

    def dump(self, new_table_name=None):
        if new_table_name is None:
            new_table_name = self.table_name
        self.cnx.execute(
            f"CREATE TABLE {new_table_name} LIKE {self.table_name}{self.head_suffix}"
        )
        self.cnx.execute(
            f"INSERT INTO {new_table_name} SELECT * FROM {self.table_name}{self.head_suffix}"
        )
        self.cnx.execute(f"ALTER TABLE {new_table_name} DROP COLUMN rid")
        self.cnx.commit()

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
            f.write(f"commit {self.version_graph.head}\n"
                    f"Author: {self.cnx.cnx_args['user']}\n"
                    f"Date: {commit_info['now']}\n"
                    f"Message: {commit_info['msg']}\n\n")

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

    def _get_table_types(self, table_name=None):
        if table_name is None:
            table_name = self.table_name + self.data_table_suffix
        schema = self.cnx.execute(f"SHOW COLUMNS FROM `{table_name}`")
        type_dict = parse_table_types(schema)
        return type_dict
