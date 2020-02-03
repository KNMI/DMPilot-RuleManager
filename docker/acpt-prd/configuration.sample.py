config = {
    "DATA_DIR": "/data/temp_archive",
    "IRODS_ROOT": "/ZoneA/home/rods/",
    "FDSNWS_ADDRESS": "http://rdsa-test.knmi.nl/fdsnws/station/1/query",
    "MONGO": [
        {
            "NAME": "WFCatalog",
            "HOST": "wfcatalog_mongo",
            "PORT": 27017,
            "DATABASE": "wfrepo",
            "COLLECTION": "daily_streams"
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
        "BUCKET_NAME": "knmi-rdsa-sds-fg-tst-ec2",
        "PREFIX": "sds",
        "PROFILE": "knmi-rdsa-storage-api-rw-tst"
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
        "FILENAME": "/tmp/rulemanager/logs/rulemanager.log" # use None for stdout
    },
    "DEFAULT_RULE_TIMEOUT" : 10,
    "DELETION_DB": "/var/rulemanager/deletion.db"
}
