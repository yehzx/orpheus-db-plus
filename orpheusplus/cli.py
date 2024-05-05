import argparse
import json
import sys

import maskpass
import mysql.connector

from orpheusplus import ORPHEUSPLUS_CONFIG, ORPHEUSPLUS_ROOT_DIR
from orpheusplus.exceptions import MySQLConnectionError
from orpheusplus.mysql_manager import MySQLManager
from orpheusplus.user_manager import UserManager


def main():
    args = parse_args(sys.argv[1:])
    args.func(args)
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
    table_name = args.table
    table_structure = args.structure


if __name__ == "__main__":
    main()