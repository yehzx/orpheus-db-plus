import argparse
import sys

import maskpass

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

    remove_parser = subparsers.add_parser("remove", help="Drop a version table")
    remove_parser.add_argument("-n", "--name", required=True, help="table name")
    remove_parser.add_argument("-y", "--yes", action="store_true")
    remove_parser.set_defaults(func=remove)

    checkout_parser = subparsers.add_parser("checkout", help="Checkout a version")
    checkout_parser.add_argument("-n", "--name", required=True, help="table name")
    checkout_parser.add_argument("-v", "--version", required=True, type=int,
                                 help="version number")
    checkout_parser.set_defaults(func=checkout)

    # TODO: this is not truly a "commit", it only allows insert now
    commit_parser = subparsers.add_parser("commit", help="Create a new version")
    commit_parser.add_argument("-n", "--name", required=True, help="table name")
    commit_parser.add_argument("-m", "--message", help="commit message")
    commit_parser.set_defaults(func=commit)

    insert_parser = subparsers.add_parser("insert", help="Insert data from file")
    insert_parser.add_argument("-n", "--name", required=True, help="table name")
    insert_parser.add_argument("-d", "--data", required=True)
    insert_parser.set_defaults(func=insert)

    return parser


def default_handler():
    print("No command specified.")
    print("Use -h or --help for more information.")


def dbconfig(args):
    print()
    db_name = input("Enter database name: ")
    db_user = input("Enter user name: ")
    user_passwd = maskpass.askpass(prompt="Enter user password: ")

    UserManager.save_user(database=db_name, user=db_user, passwd=user_passwd)
    user = UserManager()
    try:
        mydb = MySQLManager(**user.info)
        print("Connection successed.")
        print(f"User: {db_user}, Database: {db_name}")
    except MySQLConnectionError as e:
        msg = (
            f"Connection failed. Please check your input.\n"
            f"Reason: {e}"
        )
        raise Exception(msg)


def init_table(args):
    user = UserManager()
    mydb = MySQLManager(**user.info)
    table = VersionData(cnx=mydb)
    table.init_table(args.name, args.structure)


def checkout(args):
    user = UserManager()
    mydb = MySQLManager(**user.info)
    table = VersionData(cnx=mydb)
    table.load_table(args.name)
    table.checkout(args.version)


def commit(args):
    user = UserManager()
    mydb = MySQLManager(**user.info)
    table = VersionData(cnx=mydb)
    table.load_table(args.name)
    table.commit()


def insert(args):
    user = UserManager()
    mydb = MySQLManager(**user.info)
    table = VersionData(cnx=mydb)
    table.load_table(args.name)
    table.insert(args.data)


def remove(args):
    user = UserManager()
    mydb = MySQLManager(**user.info)    
    table = VersionData(cnx=mydb)
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

if __name__ == "__main__":
    main()