from configuration import config

# MongoDB driver for Python
from pymongo import MongoClient

class MongoManager():
    """
    Class MongoManager
    Container for MongoDB connection
    """

    def __init__(self):
        """
        def MongoManager.__init__
        Initializes the class
        """

        self.client = None
        self.database = None

    def connect(self):
        """
        def MongoManager.connect
        Opens connection to the database
        """

        if self.client is not None:
            return

        self.client = MongoClient(
            config["MONGO"]["HOST"],
            config["MONGO"]["PORT"]
        )

        # Use the wfrepo database
        self.database = self.client.wfrepo
