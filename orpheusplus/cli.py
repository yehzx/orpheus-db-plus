import argparse
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

    drop_parser = subparsers.add_parser("drop", help="Drop a version table")
    drop_parser.add_argument("-n", "--name", required=True, help="table name")
    drop_parser.add_argument("--all", action="store_true",
                             help="don't keep the non-version-controlled table")
    drop_parser.add_argument("-y", "--yes", action="store_true", help="confirm dropping")
    drop_parser.set_defaults(func=drop)

    dump_parser = subparsers.add_parser("dump", help="Dump a normal table of the current version table")
    dump_parser.add_argument("-n", "--name", required=True, help="version table name")
    dump_parser.add_argument("-t", "--table", help="table name for the dumpped table")
    dump_parser.set_defaults(func=dump)

    checkout_parser = subparsers.add_parser("checkout", help="Switch to a version")
    checkout_parser.add_argument("-n", "--name", required=True, help="table name")
    checkout_parser.add_argument("-v", "--version", required=True, help="version number or `head`")
    checkout_parser.set_defaults(func=checkout)

    commit_parser = subparsers.add_parser("commit", help="Create a new version")
    commit_parser.add_argument("-n", "--name", required=True, help="table name")
    commit_parser.add_argument("-m", "--message", help="commit message")
    commit_parser.set_defaults(func=commit)

    merge_parser = subparsers.add_parser("merge", help="Combine two versions")
    merge_parser.add_argument("-n", "--name", required=True, help="table name")
    merge_parser.add_argument("-v", "--version", required=True, type=int,
                              help="the version to be merged into the current version")
    merge_parser.add_argument("-r", "--resolved", help="path to resolved conflict file")
    merge_parser.set_defaults(func=merge)

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
    from orpheusplus.user_manager import UserManager

    user = UserManager()
    db_name = user.info["database"]
    # The files in VERSIONGRAPH_DIR are all version tables.
    tables = list((VERSIONGRAPH_DIR / db_name).glob("*"))
    if len(tables) == 0:
        print("No version table found.")
    else:
        print(f"Find {len(tables)} version tables.")
        for table in tables:
            print(table.stem)


def log(args):
    yellow = "\033[93m"
    cyan = "\033[96m"
    off = "\033[00m"

    table = _connect_table()
    table.load_table(args.name)
    parsed_commits = table.parse_log()

    msg = "" 
    if args.oneline:
        for commit in parsed_commits:
            msg += f"{yellow}{commit['version']:<4}{off}"
            if commit["version"] == str(table.version_graph.head):
                msg += f"{yellow}({cyan}HEAD{yellow}){off} "
            msg += commit["message"] + "\n"
    else:
        for commit in parsed_commits:
            msg += f"{yellow}commit {commit['version']:<4}{off}"
            if commit["version"] == str(table.version_graph.head):
                msg += f"{yellow}({cyan}HEAD{yellow}){off} "
            msg += f"\nAuthor: {commit['author']}"
            msg += f"\nDate: {commit['date']}"
            msg += f"\nMessage: {commit['message']}\n\n"
    print(msg)


def init_table(args):
    table = _connect_table()
    table.init_table(args.name, args.structure)
    print(f"Table `{args.name}` initialized successfully.")


def checkout(args):
    table = _connect_table()
    table.load_table(args.name)
    if args.version == "head":
        version = table.get_current_version()
    else:
        try:
            version = int(args.version)
        except ValueError:
            print("Invalid version number.")
            sys.exit()
    table.checkout(version)
    print(f"Checkout to version {version}.")


def commit(args):
    from datetime import datetime

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    table = _connect_table()
    table.load_table(args.name)
    table.commit(msg=args.message, now=now)
    head = table.version_graph.head
    print(f"Create version {head}.")


def merge(args):
    table = _connect_table()
    table.load_table(args.name)
    head = table.version_graph.head
    if not table.operation.is_empty():
        print("Please commit before merge.")
        sys.exit()
    table.merge(args.version, args.resolved)
    new_head = table.version_graph.head
    print(f"Merge version {head} and {args.version} into version {new_head}.")


def manipulate(args):
    table = _connect_table()
    table.load_table(args.name)
    table.from_file(args.op, args.data)
    print(f"{str(args.op).title()} from file {args.data}")


def drop(args):
    from orpheusplus import LOG_DIR

    if args.yes:
        ans = "y"
    else:
        ans = input(f"Do you really want to drop `{args.name}`? (y/n)\n")

    if ans == "y":
        (LOG_DIR / args.name).unlink(missing_ok=True)
        table = _connect_table()
        table.load_table(args.name)
        if args.all:
            table.remove()
            print(f"Drop `{args.name}`")
        else:
            table.remove(keep_current=True)
            print(f"Drop version control to `{args.name}`. Fall back to a normal table `{args.name} in MySQL.")
    else:
        print(f"Keep `{args.name}`")


def dump(args):
    new_table_name = args.table if args.table is not None else args.name
    table = _connect_table()
    table.load_table(args.name)
    table.dump(new_table_name)
    print(f"Dump the version table `{args.name}` to `{args.table}` in MySQL.")


def run(args):
    import csv
    from pathlib import Path

    from tabulate import tabulate

    from orpheusplus.query_parser import SQLParser

    def _write_csv(data, path, count=[0]):
        if count[0] == 0:
        
            path = Path(path)
            path.unlink(missing_ok=True)
        
        with open(path, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(data)
        
        count[0] += 1
        return count[0]

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
        parser.parse_file(args.file)
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
                count = _write_csv(output, args.output)
            else:
                print(ori_stmt)
                _print_result(result["data"], result["field"])
                print()
        elif op in ("insert", "delete", "update"):
            table.load_table(stmt["table_name"])
            table.from_parsed_data(op, stmt["attributes"])

    if args.output is not None:
        print(f"Save results of {count} statements to {args.output}")


if __name__ == "__main__":
    main()