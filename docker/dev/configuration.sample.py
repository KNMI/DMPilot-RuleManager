config = {
    "DATA_DIR": "/data/temp_archive/",
    "IRODS_ROOT": "/ZoneA/home/rods/",
    "FDSNWS_ADDRESS": "http://rdsa-test.knmi.nl/fdsnws/station/1/query",
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
        "BUCKET_NAME": "seismo-test-sds",
        "PREFIX": "my-sds",
        "PROFILE": "knmi-sandbox-saml"
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
        "FILENAME": None # use None for stdout
    },
    "DEFAULT_RULE_TIMEOUT" : 10,
    "DELETION_DB": "./deletion.db"
}
