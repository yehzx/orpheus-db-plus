import argparse
import sys
import maskpass

def main():
    args = parse_args(sys.argv[1:])
    args.func()
    pass

def parse_args(args):
    parser = setup_argparsers()
    return parser.parse_args(args)

def setup_argparsers():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title="commands",
                                       description="valid commands")
    subparsers.add_parser("init", help="Initialize datatable")
    config_parser = subparsers.add_parser("config", help="Configure MySQL connection")
    config_parser.set_defaults(func=config) 

    return parser

def config():
    print()
    db_name = input("Enter database name: ")
    db_user = input("Enter database user: ")
    user_passwd = maskpass.askpass(prompt="Enter user password: ")
    pass

    
if __name__ == "__main__":
    main()