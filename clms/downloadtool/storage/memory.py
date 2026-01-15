"""In-memory storage for download tool tasks (used in tests)."""


class MemoryDownloadtoolRepository:
    """In-memory repository compatible with the DB repository interface."""

    def __init__(self):
        self._tasks = {}

    def insert_task(self, task_id, payload):
        """Insert a task, returning True if inserted."""
        task_key = str(task_id)
        if task_key in self._tasks:
            return False
        self._tasks[task_key] = dict(payload)
        return True

    def get_task(self, task_id):
        """Fetch a task payload by task id."""
        task_key = str(task_id)
        task = self._tasks.get(task_key)
        return dict(task) if task is not None else None

    def search_tasks(self, user_id, status=None):
        """Return list of (task_id, payload) for a user and status."""
        results = []
        user_key = str(user_id)
        for task_id, payload in self._tasks.items():
            if payload.get("UserID") != user_key:
                continue
            if status is not None and payload.get("Status") != status:
                continue
            results.append((task_id, dict(payload)))
        return results

    def inspect_tasks(self, query=None):
        """Return list of (task_id, payload) filtered by payload fields."""
        results = []
        if not query:
            for task_id, payload in self._tasks.items():
                results.append((task_id, dict(payload)))
            return results

        for task_id, payload in self._tasks.items():
            for parameter, value in query.items():
                if payload.get(parameter, "") == value:
                    results.append((task_id, dict(payload)))
                    break
        return results

    def update_task(self, task_id, updates, status=None):
        """Merge updates into the payload and return the new payload."""
        task_key = str(task_id)
        if task_key not in self._tasks:
            return None
        self._tasks[task_key].update(updates)
        return dict(self._tasks[task_key])

    def delete_task(self, task_id):
        """Delete one task by id, returning True when removed."""
        task_key = str(task_id)
        return self._tasks.pop(task_key, None) is not None

    def delete_all(self):
        """Delete all tasks and return the number removed."""
        count = len(self._tasks)
        self._tasks.clear()
        return count

    def has_tasks(self):
        """Return True when the repository has at least one task."""
        return bool(self._tasks)
