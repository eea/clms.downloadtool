# -*- coding: utf-8 -*-
"""
CDSE: S3 Cleanup
"""
from datetime import datetime, timezone, timedelta
from logging import getLogger


log = getLogger(__name__)


def list_directories(s3, bucket, prefix="output/"):
    all_results = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        if "CommonPrefixes" in page:
            all_results.extend([cp["Prefix"] for cp in page["CommonPrefixes"]])
    return all_results


def list_files(s3, bucket, prefix=""):
    """List files"""
    files = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        if "Contents" in page:
            for obj in page["Contents"]:
                if obj["Key"] != prefix:  # avoid returning the directory itself
                    files.append(obj["Key"])
    return files


def get_creation_date(s3, bucket, key):
    """Get creation date of an object (file)"""
    try:
        head = s3.head_object(Bucket=bucket, Key=key)
        return head["LastModified"]
    except s3.exceptions.ClientError as e:
        log.info("s3: error on get creation date")
        log.info(e)
        return None


def get_dir_creation_date(s3, bucket, directory):
    """Get creation date of a directory based on the request file"""
    key = directory + "request-" + directory.replace('/', '') + ".json"
    log.info("s3: get dir creation date")
    return get_creation_date(s3, bucket, key)


def delete_directory(s3, bucket, prefix):
    """Delete all objects under a chosen directory (prefix)"""
    # Paginate in case there are many objects
    paginator = s3.get_paginator("list_objects_v2")
    log.info("s3: Delete directory %s", prefix)
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if "Contents" in page:
            # Build delete request
            delete_keys = {"Objects": [{"Key": obj["Key"]}
                                       for obj in page["Contents"]]}
            s3.delete_objects(Bucket=bucket, Delete=delete_keys)
            # log.info(
            #     f"Deleted {len(delete_keys['Objects'])} objects > {prefix}")


def delete_file(s3, bucket, key):
    """Delete single file"""
    try:
        s3.delete_object(Bucket=bucket, Key=key)
        log.info("Deleted: %s", key)
    except Exception as e:
        # log.info(f"Error deleting {key}: {e}")
        log.info(e)


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

    files = list_files(s3, bucket)

    for f in files:
        crd = get_creation_date(s3, bucket, f)
        if crd and crd < min_date:
            print(f"Deleting {f} (created {crd})")
            delete_file(s3, bucket, f)
