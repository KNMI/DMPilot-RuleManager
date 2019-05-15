import logging
import sqlite3

from configuration import config
from datetime import datetime
from orfeus.sdsfile import SDSFile


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
                     (id INTEGER PRIMARY KEY,
                      file TEXT UNIQUE,
                      created TEXT
                     )''')

        # Save (commit) the changes
        self.conn.commit()

    def _insert_row(self, filename):
        """
        Inserts a row into the deletion table
        """

        c = self.conn.cursor()

        # Insert a row of data
        c.execute("INSERT INTO deletion (file, created) VALUES (?,?,?,?)",
                  (filename, datetime.now().isoformat()))

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

    def _delete_row(self, filename):
        """
        Deletes a row from the deletion table that matches the given filename.
        """

        c = self.conn.cursor()

        # Insert a row of data
        c.execute("DELETE FROM deletion WHERE file=?", (filename,))

        # Save (commit) the changes
        self.conn.commit()

    def add_many_files(self, file_list):
        """
        Adds a list of files to the deletion table.

        Parameters
        ----------
        file_list : `list` of `SDSFile`
        """

        for sds_file in file_list:
            self._insert_row(sds_file.filename)

    def get_all_files(self):
        """Returns a list of all files in the deletion table."""

        c = self.conn.cursor()

        # Insert a row of data
        c.execute("SELECT * FROM deletion")

        return [SDSFile(row["file"]) for row in c.fetchall()]
