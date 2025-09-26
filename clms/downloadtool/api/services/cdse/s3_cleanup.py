# -*- coding: utf-8 -*-
"""
CDSE: S3 Cleanup
"""
import boto3
from datetime import datetime, timezone, timedelta
from clms.downloadtool.api.services.cdse.cdse_integration import (
    get_portal_config
)
from logging import getLogger


log = getLogger(__name__)


def get_s3():
    """s3 client"""
    config = get_portal_config()
    s3 = boto3.client(
        "s3",
        endpoint_url=config['s3_endpoint_url'],
        aws_access_key_id=config['s3_access_key'],
        aws_secret_access_key=config['s3_secret_key']
    )
    log.info("s3: get s3 client")
    return s3


def get_s3_bucket():
    """Bucket name from our config"""
    config = get_portal_config()
    return config['s3_bucket_name']


def list_directories(s3, bucket):
    """List all directories in bucket"""
    all_results = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Delimiter="/"):
        if "CommonPrefixes" in page:
            all_results.extend([cp["Prefix"] for cp in page["CommonPrefixes"]])
    log.info("s3: list directories")
    return all_results


def list_root_files(s3, bucket):
    """List root files"""
    root_files = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Delimiter="/"):
        if "Contents" in page:
            # Collect the Key of each object
            for obj in page["Contents"]:
                root_files.append(obj["Key"])
    log.info("s3: list root files")
    return root_files


def get_creation_date(s3, bucket, key):
    """Get creation date of an object (file)"""
    try:
        head = s3.head_object(Bucket=bucket, Key=key)
        return head["LastModified"]
    except s3.exceptions.ClientError as e:
        log.info("s3: error on get creation date")
        log.info(e)
        return None


def get_dir_creation_date(s3, bucket, dir):
    """Get creation date of a directory based on the request file"""
    key = dir + "request-" + dir.replace('/', '') + ".json"
    log.info("s3: get dir creation date")
    return get_creation_date(s3, bucket, key)


def delete_directory(s3, bucket, prefix):
    """Delete all objects under a chosen directory (prefix)"""
    # Paginate in case there are many objects
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if "Contents" in page:
            # Build delete request
            delete_keys = {"Objects": [{"Key": obj["Key"]}
                                       for obj in page["Contents"]]}
            s3.delete_objects(Bucket=bucket, Delete=delete_keys)
            log.info(
                f"Deleted {len(delete_keys['Objects'])} objects from {prefix}")


def delete_file(s3, bucket, key):
    """Delete single file"""
    try:
        s3.delete_object(Bucket=bucket, Key=key)
        # print(f"Deleted {key} from {bucket}")
    except Exception as e:
        log.info(f"Error deleting {key}: {e}")


def delete_old_data(s3, bucket):
    """delete old files"""
    min_date = datetime.now(timezone.utc) - timedelta(weeks=4)

    dirs = list_directories(s3, bucket)
    # print(dirs)
    for d in dirs:
        crd = get_dir_creation_date(s3, bucket, d)
        # print(d, crd)
        if crd and crd < min_date:
            print(f"Deleting {d} (created {crd})")
            delete_directory(s3, bucket, d)

    files = list_root_files(s3, bucket)

    for f in files:
        crd = get_creation_date(s3, bucket, f)
        if crd and crd < min_date:
            print(f"Deleting {f} (created {crd})")
            delete_file(s3, bucket, f)
