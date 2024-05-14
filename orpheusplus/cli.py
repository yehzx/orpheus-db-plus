import argparse
import sys
from tabulate import tabulate

from orpheusplus.exceptions import MySQLConnectionError
from orpheusplus.mysql_manager import MySQLManager
from orpheusplus.user_manager import UserManager
from orpheusplus.version_data import VersionData


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
    run_parser.add_argument("-f", "--file", required=True, help="file path")
    run_parser.set_defaults(func=run) 

    return parser


def default_handler():
    print("No command specified.")
    print("Use -h or --help for more information.")


def dbconfig(args):
    import maskpass
    import yaml
    from orpheusplus import ORPHEUSPLUS_CONFIG, DEFAULT_DIR

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


def _connect_table() -> VersionData:
    user = UserManager()
    mydb = MySQLManager(**user.info)
    table = VersionData(cnx=mydb)
    return table


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
    table = _connect_table()
    table.load_table(args.name)
    table.commit()


def manipulate(args):
    table = _connect_table()
    table.load_table(args.name)
    table.from_file(args.op, args.data)


def remove(args):
    table = _connect_table()
    table.load_table(args.name)
    if not args.yes:
        ans = input(f"Do you really want to drop `{args.name}`? (y/n)")
        if ans == "y":
            table.remove()
            print(f"Drop `{args.name}`")
        else:
            print(f"Keep `{args.name}`")
    else:
        table.remove()


def run(args):
    from orpheusplus.query_parser import SQLParser

    user = UserManager()
    mydb = MySQLManager(**user.info)
    parser = SQLParser()
    stmts, operations = parser.parse_file(args.file)
    ori_stmts = parser.stmts
    for ori_stmt, stmt, op in zip(ori_stmts, stmts, operations):
        result = mydb.execute(stmt)
        if result:
            print(ori_stmt)
            _print_result(result, mydb)
            print()


def _print_result(result, mydb):
    field = [col[0] for col in mydb.cursor.description]
    if field[0] == "rid":
        field = field[1:]
        result = [row[1:] for row in result]
    print(tabulate(result, headers=field, tablefmt="fancy_grid")) 


if __name__ == "__main__":
    main()