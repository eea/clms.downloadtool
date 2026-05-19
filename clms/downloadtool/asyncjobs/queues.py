"""Queue integration for DownloadTool async processing."""

import asyncio
import os
import logging
from bullmq import Queue
from clms.downloadtool.asyncjobs.manager import queue_callback

log = logging.getLogger("clms.async")

# Redis connection configuration
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
REDIS_DB = int(os.environ.get("REDIS_DB", 0))

# Connection options
redis_opts = dict(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)

# Define all queues used in CLMS async operations
QUEUES = {
    "downloadtool_jobs": Queue(
        "downloadtool_jobs", {"connection": redis_opts}),
}


def queue_job(queue_name, job_name, data, opts=None):
    """Add a job to Redis to be executed asynchronously *after commit*."""

    opts = opts or {
        "delay": 0,          # Delay in milliseconds
        "priority": 5,       # Higher = sooner
        "attempts": 1,       # Retry count
        "lifo": False,       # FIFO queueing
    }

    def callback():
        log.info("Scheduling async job '%s' in queue '%s'",
                 job_name, queue_name)

        async def inner():
            queue = QUEUES[queue_name]
            await queue.add(job_name, data, opts)
            await queue.close()

        asyncio.run(inner())

    # Use the transaction-aware queue_callback
    queue_callback(callback)
