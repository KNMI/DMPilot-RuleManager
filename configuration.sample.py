config = {
    "DATA_DIR": "/data/archive/",
    "IRODS_ROOT": "/ZoneA/home/rods/",
    "FDSNWS_ADDRESS": "https://www.orfeus-eu.org/fdsnws/station/1/query",
    "MONGO": [
        {
            "NAME": "WFCatalog-daily",
            "HOST": "wfcatalog_mongo",
            "PORT": 27017,
            "DATABASE": "wfrepo",
            "COLLECTION": "daily_streams"
        },
        {
            "NAME": "WFCatalog-segments",
            "HOST": "wfcatalog_mongo",
            "PORT": 27017,
            "DATABASE": "wfrepo",
            "COLLECTION": "c_segments"
        },
        {
            "NAME": "Dublin Core",
            "HOST": "wfcatalog_mongo",
            "PORT": 27017,
            "DATABASE": "wfrepo",
            "COLLECTION": "dublin_core"
        },
        {
            "NAME": "PPSD",
            "HOST": "ppsd_mongo",
            "PORT": 27017,
            "DATABASE": "ppsd",
            "COLLECTION": "ppsd"
        }
    ],
    "S3": {
        "BUCKET_NAME": "bucket-name",
        "PREFIX": "SDS",
        "PROFILE": None
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
