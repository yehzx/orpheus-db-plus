import argparse
import json
import sys

import maskpass
import mysql.connector

from orpheusplus import ORPHEUSPLUS_CONFIG, ORPHEUSPLUS_ROOT_DIR
from orpheusplus.mysql_manager import MySQLManager


def main():
    args = parse_args(sys.argv[1:])
    args.func()
    pass


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
    init_parser.add_argument("-t", "--table", required=True, help="table name")
    init_parser.add_argument("-s", "--structure", required=True, help="table structure")
    init_parser.add_argument("-d", "--data", help="table data")
    init_parser.set_defaults(func=init_table)

    return parser


def default_handler():
    print("No command specified.")
    print("Use -h or --help for more information.")


def dbconfig():
    print()
    db_name = input("Enter database name: ")
    db_user = input("Enter database user: ")
    user_passwd = maskpass.askpass(prompt="Enter user password: ")

    # Save to .meta/user
    with open(ORPHEUSPLUS_ROOT_DIR / ".meta/user", "w") as f:
        json.dump({"db_name": db_name, "user": db_user,
                  "passwd": user_passwd}, f)

    try:
        mydb = MySQLManager(host=ORPHEUSPLUS_CONFIG["host"],
                            port=ORPHEUSPLUS_CONFIG["port"],
                            user=db_user,
                            passwd=user_passwd,
                            database=db_name)
        print("Connection successed.")
        print(f"User: {db_user}, Database: {db_name}")
    except mysql.connector.Error as e:
        msg = (
            f"Connection failed. Please check your input.\n"
            f"Reason: {type(e).__name__} - {e}"
        )
        raise Exception(msg)


def init_table(args):
    table_name = args.table
    pass

if __name__ == "__main__":
    main()
