"""Utils"""


def is_cdse_dataset(dataset_obj):
    """Check if a dataset is CDSE dataset"""
    is_cdse = False
    try:
        # If a download information has CDSE as source we assume it was
        # migrated. The best we can do when we know only the dataset, not
        # any details about the download. In download context we have a
        # dataset_json with multiple fields.
        sources = [x for x in dataset_obj.dataset_download_information.get(
            "items", []) if x.get('full_source', '') == 'CDSE']
        if len(sources) > 0:
            is_cdse = True
    except Exception:
        pass

    return is_cdse


def is_cdse_based_dataset(dataset_obj):
    """Check if a dataset uses CDSE infrastructure (CDSE or CDSE_CSV sources)"""
    try:
        sources = [x for x in dataset_obj.dataset_download_information.get(
            "items", []) if x.get('full_source', '') in ('CDSE', 'CDSE_CSV')]
        return len(sources) > 0
    except Exception:
        return False
