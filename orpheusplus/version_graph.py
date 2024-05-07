import pickle

import networkx as nx

from orpheusplus import ORPHEUSPLUS_ROOT_DIR


class VersionGraph():
    def __init__(self, table_name):
        self.version_graph_path = ORPHEUSPLUS_ROOT_DIR / f".meta/versiongraph/{table_name}.gml"
        self.G = None

    def init_version_graph(self):
        if self.version_graph_path.is_file():
            print(f"Version graph exists. Overwrite {self.version_graph_path}")

        self.G = nx.DiGraph()
        with open(self.version_graph_path, "wb") as f:
            pickle.dump(self.G, f)
        # print creation successful
        print("Version graph created successfully.")
        print(f"Save to: {self.version_graph_path}")

    def load_version_graph(self):
        try:
            with open(self.version_graph_path, "rb") as f:
                self.G = pickle.load(f)
        except:
            raise Exception(f"Fail loading version graph from {self.version_graph_path}")

    def add_version(self):
        pass
