import pickle
from collections import Counter, defaultdict
from contextlib import contextmanager
from datetime import datetime

from orpheusplus import OPERATION_DIR


class Operation():
    def __init__(self):
        self.stmts = []
        self.add_rids = []
        self.remove_rids = []
        self.history = []
        self.operation_path = None
    
    def init_operation(self, db_name, table_name, version):
        self.operation_path = OPERATION_DIR / f"{db_name}/{table_name}_{version}"
        self.operation_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.operation_path, "wb") as f:
            pickle.dump(self, f)
    
    def load_operation(self, db_name, table_name, version):
        self.operation_path = OPERATION_DIR / f"{db_name}/{table_name}_{version}"
        with open(self.operation_path, "rb") as f:
            self.__dict__.update(pickle.load(f).__dict__)
    
    def save_operation(self):
        try:
            with open(self.operation_path, "wb") as f:
                pickle.dump(self, f)
        except:
            raise Exception(f"{self.operation_path} not properly initialized.")
    
    def commit(self, child, **commit_info):
        # If commit message has more than 72 characters, truncate it.
        msg = commit_info["msg"]
        now = commit_info["now"]
        msg = msg[:72]
        self.add_rids = []
        self.remove_rids = []
        self.history.append(("commit", (child, msg), now))
        self.save_operation()
    
    def remove(self):
        # tablename_version
        table = self.operation_path.stem.rsplit("_", 1)[0]
        for operation_path in self.operation_path.parent.glob(f"{table}_[0-9]*"):
            operation_path.unlink()
        try:
            operation_path.parent.rmdir()
        except OSError:
            pass
    
    def insert(self, rids):
        now = datetime.now()
        rids = self._parse_rids(rids)
        for start_rid, num_rids in rids:
            self.stmts.append(("insert", (start_rid, num_rids), now))
        self.save_operation()

    def delete(self, rids):
        now = datetime.now()
        rids = self._parse_rids(rids)
        for start_rid, num_rids in rids:
            self.stmts.append(("delete", (start_rid, num_rids), now))
        self.save_operation()
    
    def update(self, delete_list, insert_list):
        insert_stmt = self.stmts.pop()
        delete_stmt = self.stmts.pop()
        mapping = {}
        for insert, delete in zip(insert_list, delete_list):
            for each_delete_rid in delete:
                mapping[each_delete_rid] = insert

        self.stmts.append(("update", (delete_stmt, insert_stmt, mapping), delete_stmt[2]))
        self.save_operation()

    def clear(self):
        self.stmts = []
        self.add_rids = []
        self.remove_rids = []
        self.save_operation()
    
    def get_commit_change(self, version):
        # Return the stmts before commiting `version`
        changes = []
        for stmt in self.history:
            changes.append(stmt)
            # ('commit', (2, 'version_2'), datetime.datetime(2024, 5, 18, 16, 52, 35, 107009))
            if stmt[0] == "commit":
                if stmt[1][0] == version:
                    return changes
                else:
                    changes = []
        raise Exception(f"Version {version} not found in commit history.")
    
    def is_empty(self):
        with self.dry_parse():
            if not(self.add_rids or self.remove_rids):
                empty = True
            else:
                empty = False
        # If empty, consume all stmts and save
        if empty:
            self.parse()
        return empty

    @staticmethod 
    def _parse_rids(rids):
        rids = sorted(rids)
        result = []
        try:
            start = rids[0]
        except IndexError:
            return result
        length = 1

        for idx in range(1, len(rids)):
            if rids[idx] == rids[idx - 1] + 1:
                length += 1
            else:
                result.append((start, length))
                start = rids[idx]
                length = 1
        result.append((start, length)) 

        return result

    def parse(self):
        while self.stmts:
            stmt = self.stmts.pop(0)
            self.history.append(stmt)
            self._parse_stmt(stmt)
        self._remove_overlapping_rids()
        self.save_operation()
    
    @contextmanager
    def dry_parse(self):
        add_rids = self.add_rids
        remove_rids = self.remove_rids
        for stmt in self.stmts:
            self._parse_stmt(stmt)
        self._remove_overlapping_rids()
        yield None
        self.add_rids = add_rids
        self.remove_rids = remove_rids
    
    def _parse_stmt(self, stmt):
        op, args, timestamp = stmt
        if op == "insert":
            start_rid, num_rids = args
            self.add_rids.extend(range(start_rid, start_rid + num_rids))
        elif op == "delete":
            start_rid, num_rids = args
            self.remove_rids.extend(range(start_rid, start_rid + num_rids))
        elif op == "update":
            delete, insert, mapping = args
            self._parse_stmt(delete)
            self._parse_stmt(insert)

    # @staticmethod
    # def _return_parsed_stmt(stmt, add_rids, remove_rids):
    #     def __return_parsed_stmt(stmt):
    #         op, args, timestamp = stmt
    #         if op == "insert":
    #             start_rid, num_rids = args
    #             add_rids.extend(range(start_rid, start_rid + num_rids))
    #         elif op == "delete":
    #             start_rid, num_rids = args
    #             remove_rids.extend(range(start_rid, start_rid + num_rids))
    #         elif op == "update":
    #             delete, insert = args
    #             __return_parsed_stmt(delete)
    #             __return_parsed_stmt(insert)
    #     __return_parsed_stmt(stmt)
    #     return add_rids, remove_rids

    def _remove_overlapping_rids(self):
        # Didn't use set because same entries can be inserted and deleted and then inserted.
        add = Counter(self.add_rids)
        remove = Counter(self.remove_rids)
        self.add_rids = sorted([rid for rid, count in add.items() if count > remove[rid]])
        self.remove_rids = sorted([rid for rid, count in remove.items() if count > add[rid]])

    # @staticmethod 
    # def _return_remove_overlapping_rids(add_rids, remove_rids):
    #     add = Counter(add_rids)
    #     remove = Counter(remove_rids)
    #     add_rids = sorted([rid for rid, count in add.items() if count > remove[rid]])
    #     remove_rids = sorted([rid for rid, count in remove.items() if count > add[rid]])
    #     return add_rids, remove_rids

    @staticmethod 
    def _merge_changes(stmts):
        history = Operation._construct_history(stmts)
        changes = Operation._solve_changes(history)
        return changes

    @staticmethod 
    def _construct_history(stmts):
        history = {}
        for stmt in stmts:
            op, args, timestamp = stmt
            if op == "insert":
                pass
            elif op == "delete":
                for rid in range(args[0], args[0] + args[1]):
                    history[rid] = (None, timestamp)
            elif op == "update":
                delete, _, mapping = args
                for rid in range(delete[1][0], delete[1][0] + delete[1][1]):
                    history[rid] = (mapping[rid], timestamp)
        return history
    
    @staticmethod
    def _solve_changes(history):
        change = {}
        history = dict(history)
        for parent_rid, value in history.items(): 
            next_state = value
            try:
                while history[next_state[0]]:
                    next_state = history[next_state[0]]
            except:
                pass
            change[parent_rid] =  next_state
        return change
    
    @staticmethod
    def _find_conflicts(changes_1, changes_2):
        conflicts = {}
        for rid, (change_to, timestamp) in changes_1.items():
            if not changes_2[rid]:
                continue
            if changes_2[rid][0] != change_to:
                conflicts[rid] = [(change_to, timestamp), changes_2[rid]]
        return conflicts
