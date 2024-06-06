import argparse
import sys
from pathlib import Path
from orpheusplus.connection import connect_table
from orpheusplus.exceptions import MySQLError


def main():
    args = parse_args(sys.argv[1:])
    try:
        args.func(vars(args))
    except MySQLError as e:
        print(e.msg)


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
    init_parser.add_argument("-n", "--name", required=True, help="version table name")
    init_parser.add_argument("-s", "--structure", help="table structure")
    init_parser.add_argument("-t", "--table", help="table name for an existing table")
    init_parser.set_defaults(func=init_table)

    ls_parser = subparsers.add_parser("ls", help="List all tables under version control")
    ls_parser.add_argument("-g", "--group", help="group name")
    ls_parser.set_defaults(func=ls)

    log_parser = subparsers.add_parser("log", help="List all versions of a table")
    log_parser.add_argument("-n", "--name", help="table name")
    log_parser.add_argument("-g", "--group", help="group name")
    log_parser.add_argument("--oneline", action="store_true", help="print oneline log")
    log_parser.set_defaults(func=log)

    diff_parser = subparsers.add_parser("diff", help="Show the difference between two versions")
    diff_parser.add_argument("-n", "--name", help="table name")
    diff_parser.add_argument("-v",  "--version", nargs=2, help="version numbers or `head`")
    diff_parser.set_defaults(func=diff)

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

    snapshot_parser = subparsers.add_parser("snapshot", help="Create a snapshot for the current database (Not yet implemented)")
    snapshot_parser.add_argument("-m", "--message", required=True, help="message for the created version")
    snapshot_parser.set_defaults(func=snapshot)

    group_parser = subparsers.add_parser("group", help="Group multiple version tables together")
    group_parser.add_argument("-n", "--name", nargs="+", required=True, help="version table names")
    group_parser.add_argument("-g", "--group", help="group name")
    group_parser.set_defaults(func=group)

    ungroup_parser = subparsers.add_parser("ungroup", help="Ungroup version tables")
    ungroup_parser.add_argument("-g", "--group", help="group name")
    ungroup_parser.set_defaults(func=ungroup)

    checkout_parser = subparsers.add_parser("checkout", help="Switch to a version")
    checkout_parser.add_argument("-n", "--name", help="table name")
    checkout_parser.add_argument("-g", "--group", help="group name")
    checkout_parser.add_argument("-v", "--version", required=True, help="version number or `head`")
    checkout_parser.set_defaults(func=checkout)

    commit_parser = subparsers.add_parser("commit", help="Create a new version")
    commit_parser.add_argument("-n", "--name", help="table name")
    commit_parser.add_argument("-g", "--group", help="group name")
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
    run_parser.add_argument("--no_headers", action="store_true",
                            help="don't save headers to the output file")
    run_parser.set_defaults(func=run)

    return parser


def default_handler():
    print("No command specified.")
    print("Use -h or --help for more information.")


def dbconfig(args):
    import shutil

    import maskpass
    import yaml

    from orpheusplus import DEFAULT_DIR, ORPHEUSPLUS_CONFIG
    from orpheusplus.exceptions import MySQLError
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

    UserManager.remove_user(current_user)
    UserManager.save_user(database=db_name, user=db_user, passwd=user_passwd)

    # Test connection
    user = UserManager()
    try:
        mydb = MySQLManager(**user.info)
        print("Connection succeeded.")
        print(f"User: {db_user}, Database: {db_name}")
    except MySQLError as e:
        print(e.msg)

    # Save config
    with open(DEFAULT_DIR / "config.yaml", "w", encoding="utf-8") as f:
        yaml.dump(ORPHEUSPLUS_CONFIG, f)

    # Copy all files to the new root_dir
    if ORPHEUSPLUS_CONFIG["orpheusplus_root_dir"] != root_dir:
        shutil.copytree(root_dir + "/.meta",
                        ORPHEUSPLUS_CONFIG["orpheusplus_root_dir"] + "/.meta")
        shutil.rmtree(root_dir + "/.meta")


def ls(args):
    from tabulate import tabulate

    from orpheusplus import VERSIONGRAPH_DIR
    from orpheusplus.user_manager import UserManager

    def get_table_info(table_name):
        table = connect_table()
        table.load_table(table_name)
        head = table.version_graph.head
        try:
            parsed_commits = table.parse_log()
            commit_info = parsed_commits[-head]
            assert commit_info["version"] == str(head)
            return (table_name, commit_info["version"], commit_info["date"],
                    commit_info["message"])
        except FileNotFoundError:
            return (table_name, "-", "-", "-")

    user = UserManager()
    db_name = user.info["database"]
    # The files in VERSIONGRAPH_DIR are all version tables.
    if args["group"] is None:
        tables = list((VERSIONGRAPH_DIR / db_name).glob("*"))
        if len(tables) == 0:
            print("No version table found.")
        else:
            table_info = []
            print(f"Find {len(tables)} version tables.")
            for table_path in tables:
                table_info.append(get_table_info(table_path.stem))
    else:
        from orpheusplus.group_tracker import GroupTracker

        group = GroupTracker()
        group.load_group(args["group"])
        table_info = [get_table_info(table) for table in group.table_names]
        print(f"Find {len(table_info)} version tables in `{args['group']}`.")

    table_header = ["Table", "Current Version", "Created Time", "Message"]
    print(tabulate(table_info, headers=table_header, tablefmt="simple"))


def log(args):
    yellow = "\033[93m"
    cyan = "\033[96m"
    off = "\033[00m"

    if args["group"] is not None:
        from orpheusplus.group_tracker import GroupTracker

        group = GroupTracker()
        group.load_group(args["group"])
        parsed_commits = group.parse_log()
        entity = group

    else:
        table = connect_table()
        table.load_table(args["name"])
        parsed_commits = table.parse_log()
        entity = table

    msg = ""
    if args["oneline"]:
        for commit in parsed_commits:
            msg += f"{yellow}{commit['version']:<4}{off}"
            if commit["version"] == str(entity.get_current_version()):
                msg += f"{yellow}({cyan}HEAD{yellow}){off} "
            msg += commit["message"] + "\n"
    else:
        for commit in parsed_commits:
            msg += f"{yellow}commit {commit['version']:<4}{off}"
            if commit["version"] == str(entity.get_current_version()):
                msg += f"{yellow}({cyan}HEAD{yellow}){off} "
            msg += f"\nAuthor: {commit['author']}"
            msg += f"\nDate: {commit['date']}"
            msg += f"\nMessage: {commit['message']}\n\n"
    print(msg)


def diff(args):
    table = connect_table()
    table.load_table(args["name"])
    version_1 = _handle_version_arg(args["version"][0], table)
    version_2 = _handle_version_arg(args["version"][1], table)
    diff_result = table.diff(version_1, version_2)
    print(f"Diff version {version_1} and version {version_2}:")
    print(f"In version {version_1} only: {len(diff_result['v1_diff_v2'])} rows")
    _print_result(diff_result["v1_diff_v2"], diff_result["fields"])
    print(f"In version {version_2} only: {len(diff_result['v2_diff_v1'])} rows")
    _print_result(diff_result["v2_diff_v1"], diff_result["fields"])


def init_table(args):
    table = connect_table()
    if args["table"] is None:
        table.init_table(args["name"], args["structure"])
        print(f"Table `{args['name']}` initialized successfully.")
    else:
        if args["name"] is None:
            print("Please specify a table name by `-n`.")
            sys.exit()
        temp_data_path = Path("./temp_data.csv")
        table.from_table(from_table=args["table"], to_table=args["name"])

        args.update(input=f"SELECT * FROM {args['table']}",
                    file=None,
                    output=temp_data_path,
                    no_headers=True)
        run(args)

        args.update(op="insert", data=temp_data_path)
        manipulate(args)

        print(
            f"Table `{args['name']}` initialized successfully from `{args['table']}`.",
            "Please commit it if you want the current data be the first version."
        )
        temp_data_path.unlink()


def checkout(args):
    from orpheusplus.exceptions import NonEmptyOperation

    if args["name"] is not None and args["group"] is not None:
        print("Please specify only one of `-n` and `-g`.")
        sys.exit()

    if args["name"]:
        table = connect_table()
        table.load_table(args["name"])
        version = _handle_version_arg(args["version"], table)
        try:
            table.checkout(version)
            print(f"Checkout to version {version}.")
        except NonEmptyOperation:
            pass
    elif args["group"]:
        from orpheusplus.group_tracker import GroupTracker

        group = GroupTracker()
        group.load_group(args["group"])
        version = _handle_version_arg(args["version"], group)
        try:
            group.checkout(version)
            print(f"Checkout to group version {version}.")
        except NonEmptyOperation:
            pass


def _handle_version_arg(version, entity):
    if version == "head":
        version = entity.get_current_version()
    else:
        try:
            version = int(version)
        except ValueError:
            print("Invalid version number.")
            sys.exit()
    return version


def group(args):
    from orpheusplus.group_tracker import GroupTracker

    if len(args["name"]) == 1:
        print("Please specify more than one table name.")

    GroupTracker().init_group(group_name=args["group"],
                              table_names=args["name"])
    print(f"Group `{args['group']}` initialized successfully.")


def ungroup(args):
    from orpheusplus.group_tracker import GroupTracker

    GroupTracker().remove_group(group_name=args["group"])
    print(f"Group `{args['group']}` removed successfully.")


def commit(args):
    from datetime import datetime

    if args["name"] is not None and args["group"] is not None:
        print("Please specify only one of `-n` and `-g`.")
        sys.exit()

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if args["name"]:
        table = connect_table()
        table.load_table(args["name"])
        table.commit(msg=args["message"], now=now)
    elif args["group"]:
        from orpheusplus.group_tracker import GroupTracker
        group = GroupTracker()
        group.load_group(args["group"])
        group.commit(msg=args["message"], now=now)
        head = group.get_current_version()


def merge(args):
    table = connect_table()
    table.load_table(args["name"])
    head = table.version_graph.head
    if not table.operation.is_empty():
        print("Please commit before merge.")
        sys.exit()
    table.merge(args["version"], args["resolved"])
    new_head = table.version_graph.head
    print(
        f"Merge version {head} and {args['version']} into version {new_head}."
    )


def manipulate(args):
    table = connect_table()
    table.load_table(args["name"])
    table.from_file(args["op"], args["data"])
    print(f"{str(args['op']).title()} from file {args['data']}")


def drop(args):
    from orpheusplus import LOG_DIR

    if args["yes"]:
        ans = "y"
    else:
        ans = input(f"Do you really want to drop `{args['name']}`? (y/n)\n")

    if ans == "y":
        (LOG_DIR / args["name"]).unlink(missing_ok=True)
        table = connect_table()
        table.load_table(args["name"])
        ans_save = "y" if args["yes"] else None
        while ans_save not in ["y", "n"]:
            ans_save = input(
                f"Save a normal table copy of the version table in MySQL? (y/n)\n"
            )
        if args["all"] or ans_save == "n":
            table.remove()
            print(f"Drop `{args['name']}`")
        else:
            new_table_name = input("Enter a new table name: ")
            table.remove(keep_current=True)
            print(
                f"Drop version control to `{args['name']}`. Fall back to a normal table `{new_table_name}` in MySQL."
            )
    else:
        print(f"Keep `{args['name']}`")


def dump(args):
    new_table_name = args["table"] if args["table"] is not None else args[
        "name"]
    table = connect_table()
    table.load_table(args["name"])
    table.dump(new_table_name)
    print(
        f"Dump the version table `{args['name']}` to `{args['table']}` in MySQL."
    )


def snapshot(args):
    import re

    from orpheusplus.mysql_manager import MySQLManager
    from orpheusplus.user_manager import UserManager

    TABLE_PATTERN = re.compile(r"_orpheusplus")

    def classify_tables(tables):
        table_dict = {"normal": [], "version": []}
        for table in tables:
            if TABLE_PATTERN.search(table):
                if table.endswith("_orpheusplus"):
                    table_dict["version"].append(
                        table.replace("_orpheusplus", ""))
            else:
                table_dict["normal"].append(table)
        return table_dict

    user = UserManager()
    cnx = MySQLManager(**user.info)
    result = cnx.execute("SHOW TABLES")
    tables = classify_tables([row[0] for row in result])
    dbname = user.info["database"]

    len_normal = len(tables["normal"])
    len_version = len(tables["version"])
    print(f"Find {len_normal + len_version} tables in {dbname}")
    print(f"Non-version-controlled ({len_normal} tables):\n" +
          "\n".join(tables["normal"]) + "\n")
    print(f"Version-controlled ({len_version} tables):\n" +
          "\n".join(tables["version"]) + "\n")

def _print_result(data, fields=None):
    from tabulate import tabulate
    if fields is None:
        print(tabulate(data, tablefmt="fancy_grid"))
    else:
        print(tabulate(data, headers=fields, tablefmt="fancy_grid"))

def run(args):
    import csv

    from orpheusplus.query_parser import SQLParser

    def _write_csv(data, path, count=[0]):
        if count[0] == 0:
            path = Path(path)
            path.unlink(missing_ok=True)

        with open(path, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerows(data)

        count[0] += 1
        return count[0]

    def _handle_result(result, mydb):
        fields = [col[0] for col in mydb.cursor.description]
        if fields[0] == "rid":
            fields = fields[1:]
            data = [row[1:] for row in result]
        else:
            data = result
        return {"fields": fields, "data": data}

    table = connect_table()
    mydb = table.cnx
    parser = SQLParser()
    if args["file"] is not None:
        parser.parse_file(args["file"])
    elif args["input"] is not None:
        parser.parse(args["input"])
    else:
        raise Exception("No input provided.")
    # TODO: Refactor this block. `stmt` is either string or a dict here, make it consistent
    count = 0
    for is_modified, ori_stmt, stmt, op in zip(parser.is_modified,
                                               parser.stmts, parser.parsed,
                                               parser.operations):
        if op == "select":
            result = mydb.execute(stmt)
            result = _handle_result(result, mydb)
            if args["output"] is not None:
                output = result["data"]
                if not args["no_headers"]:
                    output.insert(0, result["fields"])
                count = _write_csv(output, args["output"])
            else:
                print(ori_stmt)
                _print_result(result["data"], result["fields"])
                print()
        elif not is_modified:
            result = mydb.execute(stmt)
        elif op in ("insert", "delete", "update"):
            table.load_table(stmt["table_name"])
            table.from_parsed_data(op, stmt["attributes"])

    if args["output"] is not None:
        print(f"Save results of {count} statements to {args['output']}")


if __name__ == "__main__":
    main()
