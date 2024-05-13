import pickle
from datetime import datetime
from orpheusplus import OPERATION_DIR
from collections import Counter

class Operation():
    def __init__(self):
        self.stmts = []
        self.add_rids = []
        self.remove_rids = []
        self.history = []
        self.operation_path = None
    
    def init_operation(self, table_name, version):
        self.operation_path = OPERATION_DIR / f"{table_name}_{version}"
        with open(self.operation_path, "wb") as f:
            pickle.dump(self, f)
    
    def load_operation(self, table_name, version):
        self.operation_path = OPERATION_DIR / f"{table_name}_{version}"
        with open(self.operation_path, "rb") as f:
            self.__dict__.update(pickle.load(f).__dict__)
    
    def save_operation(self):
        try:
            with open(self.operation_path, "wb") as f:
                pickle.dump(self, f)
        except:
            raise Exception(f"{self.operation_path} not properly initialized.")
    
    def commit(self, child):
        self.add_rids = []
        self.remove_rids = []
        self.history.append(("commit", child, datetime.now()))
        self.save_operation()
    
    def remove(self):
        # tablename_version
        table = self.operation_path.stem.rsplit("_")[0]
        for operation_path in self.operation_path.parent.glob(f"{table}_*"):
            operation_path.unlink()
    
    def insert(self, start_rid, num_rids):
        self.stmts.append(("insert", (start_rid, num_rids), datetime.now()))
        self.save_operation()

    def delete(self, rids):
        now = datetime.now()
        rids = self._parse_rids(rids)
        for start_rid, num_rids in rids:
            self.stmts.append(("delete", (start_rid, num_rids), now))
        self.save_operation()

    def clear_stmts(self):
        self.stmts = []
        self.save_operation()
    
    def is_empty(self):
        return self.stmts and self.add_rids and self.remove_rids

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
            op, args, timestamp = stmt
            if op == "insert":
                start_rid, num_rids = args
                self.add_rids.extend(range(start_rid, start_rid + num_rids))
            elif op == "delete":
                start_rid, num_rids = args
                self.remove_rids.extend(range(start_rid, start_rid + num_rids))
        self._remove_overlapping_rids()
        self.save_operation()
    
    def _remove_overlapping_rids(self):
        # Didn't use set because same entries can be inserted and deleted and then inserted.
        add = Counter(self.add_rids)
        remove = Counter(self.remove_rids)
        self.add_rids = sorted([rid for rid, count in add.items() if count > remove[rid]])
        self.remove_rids = sorted([rid for rid, count in remove.items() if count > add[rid]])