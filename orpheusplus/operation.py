import pickle
from datetime import datetime
from orpheusplus import OPERATION_DIR
from collections import Counter
from contextlib import contextmanager

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
    
    def insert(self, start_rid, num_rids):
        self.stmts.append(("insert", (start_rid, num_rids), datetime.now()))
        self.save_operation()

    def delete(self, rids):
        now = datetime.now()
        rids = self._parse_rids(rids)
        for start_rid, num_rids in rids:
            self.stmts.append(("delete", (start_rid, num_rids), now))
        self.save_operation()
    
    def update(self):
        insert = self.stmts.pop()
        delete = self.stmts.pop()
        self.stmts.append(("update", (delete, insert), delete[2]))
        self.save_operation()

    def clear(self):
        self.stmts = []
        self.add_rids = []
        self.remove_rids = []
        self.save_operation()
    
    def is_empty(self):
        with self._dry_parse():
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
            stmt = self.stmts.pop()
            self.history.insert(0, stmt)
            self._parse_stmt(stmt)
        self._remove_overlapping_rids()
        self.save_operation()
    
    @contextmanager
    def _dry_parse(self):
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
            delete, insert = args
            self._parse_stmt(delete)
            self._parse_stmt(insert)
    
    def _remove_overlapping_rids(self):
        # Didn't use set because same entries can be inserted and deleted and then inserted.
        add = Counter(self.add_rids)
        remove = Counter(self.remove_rids)
        self.add_rids = sorted([rid for rid, count in add.items() if count > remove[rid]])
        self.remove_rids = sorted([rid for rid, count in remove.items() if count > add[rid]])