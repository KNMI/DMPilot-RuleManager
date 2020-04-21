import boto3
from botocore.exceptions import ClientError

from configuration import config


BUCKET_NAME = config["S3"]["BUCKET_NAME"]
PROFILE = config["S3"]["PROFILE"]


def _get_bucket():
    """Returns the Bucket resource object for the SDS archive."""
    session = boto3.session.Session(profile_name=PROFILE)
    s3_resource = session.resource("s3")
    return s3_resource.Bucket(name=BUCKET_NAME)


def exists(sds_file):
    """Check whether a file is present in S3."""
    bucket = _get_bucket()

    try:
        bucket.Object(sds_file.s3Key).load()
    except ClientError as e:
        if int(e.response["Error"]["Code"]) == 404:
            return False
        else:
            raise
    return True


def put(sds_file):
    """Uploads a file to S3."""
    bucket = _get_bucket()
    new_object = bucket.Object(sds_file.s3Key)
    new_object.upload_file(sds_file.filepath,
                           ExtraArgs={"Metadata": {"checksum": str(sds_file.checksum)}})


def delete(sds_file):
    """Deletes a file from the S3 archive."""
    bucket = _get_bucket()
    bucket.Object(sds_file.s3Key).delete()


def get_checksum(sds_file):
    """Returns the checksum registered in the object S3 metadata. Assumes the object exists."""
    bucket = _get_bucket()
    obj = bucket.Object(sds_file.s3Key)
    try:
        checksum = obj.metadata["checksum"]
        if type(sds_file.checksum) is int:
            return int(checksum)
        else:
            return checksum
    except KeyError:
        return None


def download_file(sds_file, dest_path):
    """Downloads the file corresponding to `sds_file` from S3, and saves it at `dest_path`."""
    bucket = _get_bucket()
    bucket.Object(sds_file.s3Key).download_file(dest_path)
