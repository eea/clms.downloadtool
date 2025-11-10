"""A transaction-aware data manager for scheduling CDSE async jobs.

This replaces direct RabbitMQ or Celery integrations. It allows scheduling
callbacks to be executed only after the Plone transaction commits successfully,
ensuring data integrity between Plone and CDSE task creation.
"""


import logging

import transaction

logger = logging.getLogger(__name__)


class CallbacksDataManager:
    """Transaction aware data manager for calling callbacks at commit time"""

    def __init__(self):
        self.sp = 0
        self.callbacks = []
        self.txn = None

    def tpc_begin(self, txn):
        """tpc begin"""
        self.txn = txn

    def tpc_finish(self, txn):
        """tpc finish"""
        self.callbacks = []

    def tpc_vote(self, txn):
        """tpc vote"""
        pass

    def tpc_abort(self, txn):
        """tpc abort"""
        self._checkTransaction(txn)

        if self.txn is not None:
            self.txn = None

        self.callbacks = []

    def abort(self, txn):
        """abort"""
        self.callbacks = []

    def commit(self, txn):
        """commit"""
        self._checkTransaction(txn)

        for callback in self.callbacks:
            try:
                callback()
            except Exception:
                logger.exception("Error executing callback.")

        self.txn = None
        self.callbacks = []

    def savepoint(self):
        """savepoint"""
        self.sp += 1

        return Savepoint(self)

    def sortKey(self):
        """sortKey"""
        return self.__class__.__name__

    def add(self, callback):
        """add"""
        logger.info("Add callback to queue %s", callback)
        self.callbacks.append(callback)

    def _checkTransaction(self, txn):
        """check transaction"""
        if txn is not self.txn and self.txn is not None:
            raise TypeError("Transaction missmatch", txn, self.txn)


class Savepoint:
    """Savepoint implementation to allow rollback of queued callbacks"""

    def __init__(self, dm):
        self.dm = dm
        self.sp = dm.sp
        self.callbacks = dm.callbacks[:]
        self.txn = dm.txn

    def rollback(self):
        """rollback"""
        if self.txn is not self.dm.txn:
            raise TypeError("Attempt to rollback stale rollback")

        if self.dm.sp < self.sp:
            raise TypeError(
                "Attempt to roll back to invalid save point",
                self.sp, self.dm.sp
            )
        self.dm.sp = self.sp
        self.dm.callbacks = self.callbacks[:]


def queue_callback(callback):
    """queue callback"""
    cdm = CallbacksDataManager()
    transaction.get().join(cdm)
    cdm.add(callback)
