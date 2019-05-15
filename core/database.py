import logging
import sqlite3

from configuration import config
from datetime import datetime


class DeletionDatabase():

    """
    Class DeletionDatabase
    Manages an embedded database to keep track of deletion status of files
    """

    def __init__(self):

        # Initialize logger
        self.logger = logging.getLogger(__name__)
        self.logger.debug("Initializing the Deletion Database.")

        # Connect to (file) database
        self.logger.debug("Connecting to deletion database stored at '%s'" % config["DELETION_DB"])
        self.conn = sqlite3.connect(config["DELETION_DB"])

        # Create table if not exists
        self._create_table()

    def __del__(self):
        """
        Class destructor
        """
        self.disconnect()

    def disconnect(self):
        """Closes the connection to the database."""

        # Close the connection
        self.logger.debug("Disconnecting from deletion database")
        self.conn.close()

    def _create_table(self):
        """
        Creates the deletion table if it doesn't exist
        """

        c = self.conn.cursor()

        # Create table
        c.execute('''CREATE TABLE IF NOT EXISTS deletion
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                      file TEXT UNIQUE,
                      status TEXT,
                      created TEXT,
                      modified TEXT
                     )''')

        # Save (commit) the changes
        self.conn.commit()

    def _insert_row(self, filename, status):
        """
        Inserts a row into the deletion table
        """

        c = self.conn.cursor()

        # Insert a row of data
        c.execute("INSERT INTO deletion (file, status, created, modified) VALUES ('%s','%s','%s','%s')" % (
            filename, status,
            datetime.now().isoformat(), datetime.now().isoformat()
        ))

        # Save (commit) the changes
        self.conn.commit()

    def get_deletion_status(self):
        """
        Return the files status data stored in the deletion table
        """

        c = self.conn.cursor()

        # Insert a row of data
        c.execute("SELECT * FROM deletion")

        return c.fetchall()

    # TODO:
    # _update_row(), _delete_row() methods
    # add SDSFile.status property to store info of (un)successful deletion?
    # higher level method that receives the list of SDSFiles after sequence exec
    #   and inserts, updates or removes rows in the database
