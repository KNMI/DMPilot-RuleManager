config = {
    "IRODS_ROOT": "/awsZone/home/rods/",
    "FDSNWS_ADDRESS": "https://www.orfeus-eu.org/fdsnws/station/1/query",
    "MONGO": {
        "HOST": "localhost",
        "PORT": 27017,
        "USER": "user",
        "PASS": "pass",
        "DATABASE": "wfrepo",
        "DC_METADATA_COLLECTION": "wf_do",
        "WF_METADATA_COLLECTION": "daily_streams"
    },
    "IRODS": {
        "ENABLED": True,
        "HOST": "localhost",
        "PORT": "1247",
        "USER": "rods",
        "PASS" : "xxxxxx",
        "ZONE": "awsZone"
    }
}
