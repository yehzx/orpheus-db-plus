import pickle
import networkx as nx

from orpheusplus import VERSIONGRAPH_DIR
from orpheusplus.operation import Operation
from orpheusplus.version_table import VersionTable
from orpheusplus.mysql_manager import MySQLManager

class VersionGraph():
    def __init__(self, cnx: MySQLManager):
        self.cnx = cnx
        self.table_name = None
        self.db_name = None
        self.version_table = None
        self.version_graph_path = None
        self.G = None
        self.head = None
        self.version_count = None

    def init_version_graph(self, db_name, table_name):
        self.table_name = table_name
        self.db_name = db_name
        self.version_graph_path = VERSIONGRAPH_DIR / f"{db_name}/{table_name}"
        self.version_graph_path.parent.mkdir(parents=True, exist_ok=True)
        if self.version_graph_path.is_file():
            print(f"Version graph exists. Overwrite {self.version_graph_path}")

        self.head = 0
        self.version_count = 0
        self.G = nx.DiGraph()
        self._save_graph()
        print("Version graph created successfully.")
        print(f"Save to: {self.version_graph_path}")

        # The actual table for tracking relations in different versions
        self._init_version_table()
    
    def _init_version_table(self):
        self.version_table = VersionTable(self.cnx)
        self.version_table.init_version_table(self.table_name)

    def load_version_graph(self, db_name, table_name):
        self.table_name = table_name
        self.db_name = db_name
        self.version_graph_path = VERSIONGRAPH_DIR / f"{db_name}/{table_name}"
        try:
            with open(self.version_graph_path, "rb") as f:
                self.G = pickle.load(f)
                self._load_graph_attr()
        except:
            raise Exception(f"Fail loading version graph from {self.version_graph_path}")
        
        self._load_version_table()
    
    def switch_version(self, version):
        self.head = version
        self._save_graph()
        self._load_version_table()
        rids = self.version_table.get_version_rids(version)
        return rids

    def _save_graph(self):
        self._save_graph_attr() 
        with open(self.version_graph_path, "wb") as f:
            pickle.dump(self.G, f)    

    def _save_graph_attr(self):
        self.G.graph["version_count"] = self.version_count        

    def _load_graph_attr(self):
        self.head = Operation.get_user_head(self.db_name, self.table_name, self.cnx.cnx_args["user"])
        self.version_count = self.G.graph["version_count"]    

    def _load_version_table(self):
        self.version_table = VersionTable(self.cnx)
        self.version_table.load_version_table(self.table_name)

    def add_version(self, operation: Operation, **commit_info):
        num_rids, overlap = self._get_num_rids_and_overlap(self.head, operation)

        old_head = self.head
        self.version_count += 1
        self.G.graph["head"] = self.version_count
        self.head = self.version_count

        self.G.add_node(self.head, num_rids=num_rids)
        if self.G.has_node(old_head):
            self.G.add_edge(old_head, self.head, overlap=overlap)

        self.version_table.add_version(operation=operation,
                                       version=self.head,
                                       parent=old_head)
        
        operation.commit(self.head, **commit_info)
        self._save_graph()
    
    def merge_version(self, operation: Operation,
                      from_version, to_version, **commit_info):
        _, overlap = self._get_num_rids_and_overlap(from_version, operation)
        self.G.add_edge(from_version, to_version, overlap=overlap)
        operation.commit(to_version, **commit_info)    
        self._save_graph()

    def _get_num_rids_and_overlap(self, parent, operation: Operation):
        total_rids = 0
        overlap = 0
        try:
            total_rids += self.G.nodes[parent]["num_rids"]
            overlap = total_rids
        except KeyError:
            pass
        
        total_rids += len(operation.add_rids) - len(operation.remove_rids)
        overlap -= len(operation.remove_rids)
        
        return total_rids, overlap

    def gather_changes(self, version):
        path_1, path_2 = self._find_path_to_common_ancestor(self.head, version)
        stmts_1 = self._gather_changes_from_path(path_1)
        stmts_2 = self._gather_changes_from_path(path_2)
        changes_head = Operation._merge_changes(stmts_1)
        changes_version = Operation._merge_changes(stmts_2)
        return changes_head, changes_version

    def gather_conflicts(self, changes_1, changes_2):
        conflicts = Operation._find_conflicts(changes_1, changes_2)
        return conflicts
            
    def _gather_changes_from_path(self, path):
        changes = []
        ancestor = path.pop(0)
        while path:
            successor = path[0]
            op = Operation()
            op.load_operation(self.db_name, self.table_name, ancestor)
            changes.extend(op.get_commit_change(successor))
            ancestor = path.pop(0)
        return changes

    def _find_path_to_common_ancestor(self, version_1, version_2):
        ancestor = nx.lowest_common_ancestor(self.G, version_1, version_2)
        path_1 = nx.shortest_path(self.G, ancestor, version_1)
        path_2 = nx.shortest_path(self.G, ancestor, version_2)
        return path_1, path_2
    
    def remove(self):
        self.version_table.delete()
        self.version_graph_path.unlink()
        try:
            self.version_graph_path.parent.rmdir()
        except OSError:
            pass