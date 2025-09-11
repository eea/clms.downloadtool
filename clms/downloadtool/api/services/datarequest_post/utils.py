"""Utils"""


def to_iso8601(dt_str):
    """Convert datetime in format requested by CDSE"""
    dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
    return dt.isoformat() + "Z"   # adding Z for UTC


def generate_task_group_id():
    """A CDSE parent task and its childs have the same group ID.
       Example: 4823-9501-3746-1835
    """
    groups = []
    for _ in range(4):
        group = ''.join(str(random.randint(0, 9)) for _ in range(4))
        groups.append(group)
    return '-'.join(groups)
