import argparse
import re
import sys


def main():
    args = parse_args(sys.argv[1:])
    args.func(args)


def parse_args(args):
    parser = setup_argparsers()
    return parser.parse_args(args)


def setup_argparsers():
    parser = argparse.ArgumentParser()
    parser.set_defaults(func=default_handler)
    subparsers = parser.add_subparsers(title="commands",
                                       description="valid commands")

    config_parser = subparsers.add_parser("config", help="Configure MySQL connection")
    config_parser.set_defaults(func=dbconfig)

    init_parser = subparsers.add_parser("init", help="Initialize version control to a table")
    init_parser.add_argument("-n", "--name", required=True, help="table name")
    init_parser.add_argument("-s", "--structure", required=True, help="table structure")
    init_parser.set_defaults(func=init_table)

    ls_parser = subparsers.add_parser("ls", help="List all tables under version control")
    ls_parser.set_defaults(func=ls)

    log_parser = subparsers.add_parser("log", help="List all versions of a table")
    log_parser.add_argument("-n", "--name", required=True, help="table name")
    log_parser.add_argument("--oneline", action="store_true", help="print oneline log")
    log_parser.set_defaults(func=log)

    remove_parser = subparsers.add_parser("remove", help="Drop a version table")
    remove_parser.add_argument("-n", "--name", required=True, help="table name")
    remove_parser.add_argument("-y", "--yes", action="store_true")
    remove_parser.set_defaults(func=remove)

    checkout_parser = subparsers.add_parser("checkout", help="Checkout a version")
    checkout_parser.add_argument("-n", "--name", required=True, help="table name")
    checkout_parser.add_argument("-v", "--version", required=True, help="version number or `head`")
    checkout_parser.set_defaults(func=checkout)

    commit_parser = subparsers.add_parser("commit", help="Create a new version")
    commit_parser.add_argument("-n", "--name", required=True, help="table name")
    commit_parser.add_argument("-m", "--message", help="commit message")
    commit_parser.set_defaults(func=commit)

    insert_parser = subparsers.add_parser("insert", help="Insert data from file")
    insert_parser.add_argument("-n", "--name", required=True, help="table name")
    insert_parser.add_argument("-d", "--data", required=True)
    insert_parser.set_defaults(func=manipulate, op="insert")

    delete_parser = subparsers.add_parser("delete", help="Delete data from file")
    delete_parser.add_argument("-n", "--name", required=True, help="table name")
    delete_parser.add_argument("-d", "--data", required=True)
    delete_parser.set_defaults(func=manipulate, op="delete")

    update_parser = subparsers.add_parser("update", help="Update data from file")
    update_parser.add_argument("-n", "--name", required=True, help="table name")
    update_parser.add_argument("-d", "--data", required=True, nargs=2,
                               help="OLD_DATA NEW_DATA")
    update_parser.set_defaults(func=manipulate, op="update")

    run_parser = subparsers.add_parser("run", help="Run a SQL script")
    run_parser.add_argument("-f", "--file", help="script path")
    run_parser.add_argument("-i", "--input", help="SQL statement")
    run_parser.add_argument("-o", "--output", help="output file path")
    run_parser.set_defaults(func=run) 

    return parser


def default_handler():
    print("No command specified.")
    print("Use -h or --help for more information.")


def _connect_table():
    from orpheusplus.mysql_manager import MySQLManager
    from orpheusplus.user_manager import UserManager
    from orpheusplus.version_data import VersionData

    user = UserManager()
    mydb = MySQLManager(**user.info)
    table = VersionData(cnx=mydb)
    return table


def dbconfig(args):
    import maskpass
    import yaml

    from orpheusplus import DEFAULT_DIR, ORPHEUSPLUS_CONFIG
    from orpheusplus.exceptions import MySQLConnectionError
    from orpheusplus.mysql_manager import MySQLManager
    from orpheusplus.user_manager import UserManager

    host = ORPHEUSPLUS_CONFIG["host"]
    port = ORPHEUSPLUS_CONFIG["port"]
    root_dir = ORPHEUSPLUS_CONFIG["orpheusplus_root_dir"]

    print(f"Current host: {host} (press 'enter' to skip)")
    res = input("Enter host: ")
    ORPHEUSPLUS_CONFIG["host"] = res if res != "" else host
    print(f"Current port: {port} (press 'enter' to skip)")
    res = input("Enter port: ")
    ORPHEUSPLUS_CONFIG["port"] = res if res != "" else port
    print(f"Current orpheusplus directory: {root_dir} (press 'enter' to skip)")
    res = input("Enter orpheusplus directory: ")
    ORPHEUSPLUS_CONFIG["orpheusplus_root_dir"] = res if res != "" else root_dir

    try:
        user = UserManager()
        current_user = user.info["user"]
        current_db = user.info["database"]
        current_passwd = user.info["passwd"]
    except:
        current_user = ""
        current_passwd = ""
        current_db = ""

    if current_db != "":
        print(f"Current database: {current_db} (press 'enter' to skip)")
    res = input("Enter database name: ")
    db_name = res if res != "" else current_db

    if current_user != "":
        print(f"Current user: {current_user} (press 'enter' to skip)")
    res = input("Enter user name: ")
    
    if res != "":
        db_user = res
        user_passwd = maskpass.askpass(prompt="Enter user password: ")
    else:
        db_user = current_user
        user_passwd = current_passwd

    # Save config
    with open(DEFAULT_DIR / "config.yaml", "w", encoding="utf-8") as f:
        yaml.dump(ORPHEUSPLUS_CONFIG, f)

    UserManager.save_user(database=db_name, user=db_user, passwd=user_passwd)

    # Test connection
    user = UserManager()
    try:
        mydb = MySQLManager(**user.info)
        print("Connection succeeded.")
        print(f"User: {db_user}, Database: {db_name}")
    except MySQLConnectionError as e:
        msg = (
            f"Connection failed. Please check your input.\n"
            f"Reason: {e}"
        )
        raise Exception(msg)


def ls(args):
    from orpheusplus import VERSIONGRAPH_DIR

    # The files in VERSIONGRAPH_DIR are all version tables.
    tables = list(VERSIONGRAPH_DIR.glob("*"))
    if len(tables) == 0:
        print("No version table found.")
    else:
        print(f"Find {len(tables)} version tables.")
        for table in tables:
            print(table.stem)


def log(args):
    from orpheusplus import LOG_DIR

    yellow = "\033[93m"
    cyan = "\033[96m"
    off = "\033[00m"

    table = _connect_table()
    table.load_table(args.name)

    parsed_commits = []
    with open(LOG_DIR / f"{args.name}") as f:
        commits = f.read().split("\n\n")
        for commit in reversed(commits):
            if not commit:
                continue
            parsed_commits.append(_parse_commit(commit))

    msg = "" 
    if args.oneline:
        for commit in parsed_commits:
            msg += f"{yellow}{commit['version']:<4}{off}"
            if commit["version"] == str(table.version_graph.head):
                msg += f"{yellow}({cyan}HEAD{yellow}){off} "
            msg += commit["message"] + "\n"
        # Restrict the number of lines printed
        print("\n".join(msg.split("\n")[:100]), end="")
    else:
        for commit in parsed_commits:
            msg += f"{yellow}commit {commit['version']:<4}{off}"
            if commit["version"] == str(table.version_graph.head):
                msg += f"{yellow}({cyan}HEAD{yellow}){off} "
            msg += f"\nAuthor: {commit['author']}"
            msg += f"\nDate: {commit['date']}"
            msg += f"\nMessage: {commit['message']}\n\n"
        print("\n".join(msg.split("\n")[:200]), end="")


def _parse_commit(commit):
    lines = commit.split("\n")
    version = re.search(r"^commit (\d+)", lines[0]).group(1)
    author = re.search(r"^Author: (.*)", lines[1]).group(1)
    date = re.search(r"^Date: (.*)", lines[2]).group(1)
    message = re.search(r"^Message: (.*)", lines[3]).group(1)
    return {"version": version, "author": author, "date": date,
            "message": message}


def init_table(args):
    table = _connect_table()
    table.init_table(args.name, args.structure)


def checkout(args):
    table = _connect_table()
    table.load_table(args.name)
    if args.version == "head":
        version = table.get_current_version()
    else:
        version = args.version
    table.checkout(version)


def commit(args):
    from datetime import datetime

    from orpheusplus import LOG_DIR

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    table = _connect_table()
    table.load_table(args.name)
    table.commit(args.message, now)

    with open(LOG_DIR / f"{args.name}", "a") as f:
        f.write(
            f"commit {table.version_graph.head}\n"
            f"Author: {table.cnx.cnx_args['user']}\n"
            f"Date: {now}\n"
            f"Message: {args.message}\n\n"
        )


def manipulate(args):
    table = _connect_table()
    table.load_table(args.name)
    table.from_file(args.op, args.data)


def remove(args):
    from orpheusplus import LOG_DIR

    if not args.yes:
        ans = input(f"Do you really want to drop `{args.name}`? (y/n)\n")
        if ans == "y":
            (LOG_DIR / args.name).unlink(missing_ok=True)
            table = _connect_table()
            table.load_table(args.name)
            table.remove()
            print(f"Drop `{args.name}`")
        else:
            print(f"Keep `{args.name}`")
    else:
        table.remove()



def run(args):
    from tabulate import tabulate

    from orpheusplus.query_parser import SQLParser

    def _write_csv(data, path):
        import csv
        with open(path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(data)
        print(f"Save results to {path}")

    def _handle_result(result, mydb):
        field = [col[0] for col in mydb.cursor.description]
        if field[0] == "rid":
            field = field[1:]
            data = [row[1:] for row in result]
        return {"field": field, "data": data} 

    def _print_result(data, field=None):
        if field is None:
            print(tabulate(data, tablefmt="fancy_grid")) 
        else:
            print(tabulate(data, headers=field, tablefmt="fancy_grid")) 

    table = _connect_table()
    mydb = table.cnx
    parser = SQLParser()
    if args.file is not None:
        parser.parse_file(args.input)
    elif args.input is not None:
        parser.parse(args.input)
    else:
        raise Exception("No input provided.")
    # TODO: Refactor this block. `stmt` is either string or a dict here, make it consistent
    for is_modified, ori_stmt, stmt, op in zip(parser.is_modified, parser.stmts, parser.parsed, parser.operations):
        if op == "select" or not is_modified:
            result = mydb.execute(stmt)
            result = _handle_result(result, mydb)
            if args.output is not None:
                output = result["data"]
                output.insert(0, result["field"])
                _write_csv(output, args.output)
            else:
                print(ori_stmt)
                _print_result(result["data"], result["field"])
                print()
        elif op in ("insert", "delete", "update"):
            table.load_table(stmt["table_name"])
            table.from_parsed_data(op, stmt["attributes"])


if __name__ == "__main__":
    main()