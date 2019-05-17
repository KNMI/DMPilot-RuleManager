import sqlite3
import argparse

from configuration import config
from core.database import deletion_database

if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Tool to debug the DeletionDatabase.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--list", help="Lists the filenames in the table", action="store_true")
    group.add_argument("--count",
                       help="Counts how many filenames there are in the table",
                       action="store_true")
    group.add_argument("--remove", help="Removes one filename from the table")
    group.add_argument("--clear", help="Clears the table", action="store_true")
    parsedargs = parser.parse_args()

    if parsedargs.count:
        print(len(deletion_database.get_all_filenames()))

    if parsedargs.list:
        for filename in deletion_database.get_all_filenames():
            print(filename)

    if parsedargs.clear:
        for filename in deletion_database.get_all_filenames():
            deletion_database._delete_row(filename)

    if parsedargs.remove is not None:
        deletion_database._delete_row(parsedargs.remove)
