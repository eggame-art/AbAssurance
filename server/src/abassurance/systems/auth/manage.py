import argparse
from getpass import getpass

from abassurance.systems.auth.service import create_user


def main():
    parser = argparse.ArgumentParser(description="Create an AbAssurance account")
    parser.add_argument("username")
    parser.add_argument("--role", required=True, choices=["admin", "user"])
    parser.add_argument("--source", choices=["abassurance", "assureplus"])
    parser.add_argument("--source-id")
    args = parser.parse_args()
    password = getpass("Password (minimum 12 characters): ")
    if password != getpass("Confirm password: "):
        parser.error("Passwords differ")
    create_user(args.username, password, args.role, args.source, args.source_id)
    print("Account created")


if __name__ == "__main__":
    main()
