import csv
import re
import sys
from collections import OrderedDict


def parse_commit(commit):
    lines = commit.split("\n")
    version = re.search(r"^commit (\d+)", lines[0]).group(1)
    author = re.search(r"^Author: (.*)", lines[1]).group(1)
    date = re.search(r"^Date: (.*)", lines[2]).group(1)
    message = re.search(r"^Message: (.*)", lines[3]).group(1)
    return {"version": version, "author": author, "date": date,
            "message": message}

def parse_table_types(schema):
    type_dict = OrderedDict()
    for col in schema:
        type_dict[col[0]] = col[1].decode()
    return type_dict

def parse_csv_structure(filepath) -> list:
    cols = []
    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) != 2:
                print("Invalid data schema, should be COLUMN_NAME,DATA_TYPE")
                sys.exit()
            cols.append(row)
    return  cols

def parse_csv_data(filepath):
    data = []
    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        data = list(reader)
    return data    


def match_column_order(table_cols, cols):
    order = []
    for table_col in table_cols:
        match = False
        for idx, col in enumerate(cols):
            if table_col.lower() == col.lower():
                order.append(idx)
                match = True
                break
        if not match:
            order.append(None)
    return order


def reorder_data(data, order):
    reordered = []
    for row in data:
        new_row = [row[idx] if idx is not None else None for idx in order]
        reordered.append(new_row)
    return reordered