config = {
    "IRODS_ROOT": "/ZoneA/home/rods/",
    "FDSNWS_ADDRESS": "https://www.orfeus-eu.org/fdsnws/station/1/query",
    "MONGO": {
        "HOST": "localhost",
        "PORT": 27017,
        "USER": "username",
        "PASS": "password",
        "DATABASE": "wfrepo",
        "DC_METADATA_COLLECTION": "wf_do",
        "WF_METADATA_COLLECTION": "daily_streams"
    },
    "IRODS": {
        "HOST": "localhost",
        "PORT": "1247",
        "USER": "username",
        "PASS": "password",
        "ZONE": "ZoneA"
    },
    "LOGGING": {
        "LEVEL": "INFO",
        "FILENAME": "~/log/sdsmanager.log" # use None for stdout
    },
    "DEFAULT_RULE_TIMEOUT" : 10,
    "DELETION_DB": "./deletion.db"
}
