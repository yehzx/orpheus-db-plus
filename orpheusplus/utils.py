import re


def parse_commit(commit):
    lines = commit.split("\n")
    version = re.search(r"^commit (\d+)", lines[0]).group(1)
    author = re.search(r"^Author: (.*)", lines[1]).group(1)
    date = re.search(r"^Date: (.*)", lines[2]).group(1)
    message = re.search(r"^Message: (.*)", lines[3]).group(1)
    return {"version": version, "author": author, "date": date,
            "message": message}