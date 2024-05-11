import pickle

from orpheusplus import OPERATION_DIR

class Operation():
    def __init__(self):
        self.stmts = []
        self.add_rids = []
        self.remove_rids = []
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
    
    def remove(self):
        # tablename_version
        table = self.operation_path.stem.rsplit("_")[0]
        for operation_path in self.operation_path.parent.glob(f"{table}_*"):
            operation_path.unlink()
    
    def insert(self, start_rid, num_rids):
        self.stmts.append(("insert", (start_rid, num_rids)))
        self.save_operation()

    def update(self, old_rid_start, new_rid_start, num_rids):
        self.stmts.append(("update", (old_rid_start, new_rid_start, num_rids)))
        self.save_operation()

    def delete(self, start_rid, num_rids):
        self.stmts.append(("delete", (start_rid, num_rids)))
        self.save_operation()

    def parse(self):
        while self.stmts:
            op, args = self.stmts.pop()
            if op == "insert":
                start_rid, num_rids = args
                self.add_rids.extend(range(start_rid, start_rid + num_rids))
            elif op == "update":
                old_rid_start, new_rid_start, num_rids = args
                self.add_rids.extend(range(new_rid_start, new_rid_start + num_rids))
                self.remove_rids.extend(range(old_rid_start, old_rid_start + num_rids))
            elif op == "delete":
                start_rid, num_rids = args
                self.remove_rids.extend(range(start_rid, start_rid + num_rids))
        self._remove_overlapping_rids()
    
    def _remove_overlapping_rids(self):
        """
        If self.add_rids and self.remove_rids contain some overlapping rids,
        these must be added at some stage and removed later, which have 
        no net effect to the newer version.
        """
        add = set(self.add_rids)
        remove = set(self.remove_rids)
        self.add_rids = sorted(list(add.difference(remove)))
        self.remove_rids = sorted(list(remove.difference(add)))