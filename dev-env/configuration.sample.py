config = {
    "IRODS_ROOT": "/ZoneA/home/rods/",
    "FDSNWS_ADDRESS": "https://www.orfeus-eu.org/fdsnws/station/1/query",
    "MONGO": {
        "HOST": "wf_catalog_mongo",
        "PORT": 27017,
        "USER": "username",
        "PASS": "password",
        "DATABASE": "wfrepo",
        "DC_METADATA_COLLECTION": "wf_do",
        "WF_METADATA_COLLECTION": "daily_streams",
        "PPSD_METADATA_COLLECTION": "ppsd"
    },
    "S3": {
        "BUCKET_NAME": "seismo-test-sds",
        "PREFIX": "my-sds",
        "PROFILE": "sandbox"
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
