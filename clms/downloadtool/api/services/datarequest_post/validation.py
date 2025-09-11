"""Validation"""

MESSAGES = {
    "NOT_LOGGED_IN": "You need to be logged in to use this service",
    "UNDEFINED_DATASET_ID": "Error, DatasetID is not defined",
    "INVALID_DATASET_ID": "Error, the DatasetID is not valid",
    "INVALID_FILE_ID": "Error, the FileID is not valid",
    "NUTS_COUNTRY_ERROR": "NUTS country error",
    "NUTS_ALSO_DEFINED": "Error, NUTS is also defined",
    "INVALID_BOUNDINGBOX": "Error, BoundingBox is not valid",
    "TEMP_REST_NOT_ALLOWED": (
        "Error, temporal restriction is not allowed in not time-series "
        "enabled datasets"
    ),
    "TEMP_TOO_MANY": "Error, TemporalFilter has too many fields",
    "TEMP_MISSING_RANGE": (
        "Error, TemporalFilter does not have StartDate or EndDate"
    ),
    "INCORRECT_DATE": "Error, date format is not correct",
    "INCORRECT_DATE_RANGE":
    (
        "Error, difference between StartDate and EndDate is not coherent"
    ),
    "UNDEFINED_GCS": "Error, defined GCS not in the list",
    "MISSING_GCS": "The OutputGCS parameter is mandatory.",
    "UNDEFINED_INFO_ID": "Error, DatasetDownloadInformationID is not defined.",
    "NOT_DOWNLOADABLE": "Error, this dataset is not downloadable",
    "INVALID_OUTPUT": "Error, the specified output format is not valid",
    "NOT_COMPATIBLE": "Error, specified formats are not compatible",
    "INVALID_SOURCE": "Error, the dataset source is not valid",
    "INVALID_LAYER": "Error, the requested band/layer is not valid",
    "MISSING_TEMPORAL": (
        "You are requesting to download a time series enabled dataset and you "
        "are required to request the download of an specific date range. "
        "Please check the download documentation to get more information"
    ),
    "FULL_NOT_EEA": (
        "You are requesting to download the full dataset but this dataset is "
        "not an EEA dataset and thus you need to query an specific endpoint to"
        " request its download. Please check the API documentation to get"
        " more information about this specific endpoint."
    ),
    "FULL_EEA": (
        "To download the full dataset, please download it through the "
        "corresponding pre-packaged data collection"
    ),
    "MUST_HAVE_AREA": (
        "You have to select a specific area of interest. In case you want to "
        "download the full dataset, please use the Auxiliary API."
    ),
    "DOWNLOAD_LIMIT": (
        "The download queue can only process 5 items at a time."
        " Please try again with fewer items."
    ),
    "DUPLICATED": (
        "You have requested to download the same thing at least twice. "
        "Please check your download cart and remove any duplicates."
    ),
    "ALL_FAILED": "Error, all requests failed",
}
