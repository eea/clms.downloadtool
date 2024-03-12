# -*- coding: utf-8 -*-
"""
The best way to save the download tool registry is to save plain data-types in
an annotation of the site object.

This way to store information is one of the techniques used in Plone to save
non-contentish information.

To achieve that we use the IAnnotations interface to abstract saving that
informations. This technique provides us with a dictionary-like interface
where we can save, update and retrieve information.

We will also encapsulate all operations with the download tool registry in
this utility, this way it will be the central point of the all functionality
involving the said registry.

Wherever we need to interact with it (ex, REST API) we will get the utility
and call its method.

We have to understand the utility as being a Singleton object.

"""
import json
import random
from datetime import datetime
from logging import getLogger

from clms.downloadtool.orm import DownloadRegistry, Session
from clms.downloadtool.utils import STATUS_LIST
from sqlalchemy import delete, update
from zope.interface import Interface, implementer

log = getLogger(__name__)


class IDownloadToolUtility(Interface):
    """Downloadtool utility interface"""


@implementer(IDownloadToolUtility)
class DownloadToolUtility:
    """Downloadtool request methods"""

    def datarequest_post(self, data_request):
        """register new download request"""
        session = Session()

        task_id = random.randint(0, 99999999999)
        str_task_id = str(task_id)
        while session.query(DownloadRegistry).filter_by(
            id=str_task_id
        ).all():
            task_id = random.randint(0, 99999999999)
            str_task_id = str(task_id)

        session.add(
            DownloadRegistry(
                id=str_task_id,
                content=json.dumps(data_request)
            )
        )

        return {str_task_id: data_request}

    def datarequest_delete(self, task_id, user_id):
        """cancel the download request"""
        session = Session()
        data_object = None
        tasks = session.query(
            DownloadRegistry
        ).filter_by(id=task_id).all()

        if not tasks:
            return "Error, TaskID not registered"

        data_object = json.loads(tasks[0].content)
        if user_id not in data_object.get("UserID"):
            return "Error, permission denied"

        data_object["Status"] = "Cancelled"
        data_object["FinalizationDateTime"] = datetime.utcnow().isoformat()

        session.execute(
            update(DownloadRegistry).filter_by(
                id=task_id
            ).values(content=json.dumps(data_object))
        )

        return data_object

    def datarequest_search(self, user_id, status):
        """search for download requests"""
        session = Session()
        data_object = {}

        if not user_id:
            return "Error, UserID not defined"

        if not status:
            items = session.query(DownloadRegistry).all()
            for item in items:
                values = json.loads(item.content)
                if str(user_id) == values.get("UserID"):
                    data_object[item.id] = values
            return data_object

        if status not in STATUS_LIST:
            return "Error, status not recognized"

        items = session.query(DownloadRegistry).all()
        for item in items:
            values = json.loads(item.content)
            if status == values.get("Status") and str(user_id) == values.get(
                "UserID"
            ):
                data_object[item.id] = values

        return data_object

    def datarequest_status_get(self, task_id):
        """get a given download task's information"""
        session = Session()
        items = session.query(DownloadRegistry).filter_by(id=task_id).all()
        if not items:
            return "Error, task not found"
        return json.loads(items[0].content)

    def datarequest_status_patch(self, data_object, task_id):
        """modify a given download task's information"""
        session = Session()
        items = session.query(DownloadRegistry).filter_by(id=task_id).all()
        if not items:
            return "Error, task_id not registered"

        registry_item = json.loads(items[0].content)

        if "Status" in data_object:
            registry_item["Status"] = data_object["Status"]
        if "DownloadURL" in data_object:
            registry_item["DownloadURL"] = data_object["DownloadURL"]
        if "FileSize" in data_object:
            registry_item["FileSize"] = data_object["FileSize"]
        if "FinalizationDateTime" in data_object:
            registry_item["FinalizationDateTime"] = data_object[
                "FinalizationDateTime"
            ]
        if "Message" in data_object:
            registry_item["Message"] = data_object["Message"]

        session.execute(update(DownloadRegistry).filter_by(id=task_id).values(content=json.dumps(registry_item)))

        return registry_item

    def delete_data(self):
        """a method to delete all data from the registry"""
        session = Session()

        items = session.query(DownloadRegistry).all()
        if not items:
            return {"status": "Error", "msg": "Registry is empty"}

        session.execute(delete(DownloadRegistry))

        return {}

    def datarequest_remove_task(self, task_id):
        """Remove all data about the given task"""
        session = Session()
        items = session.query(DownloadRegistry).filter_by(id=task_id).all()
        if not items:
            return "Error, TaskID not registered"

        session.execute(delete(DownloadRegistry).filter_by(id=task_id))
        return 1

    def datarequest_inspect(self, **query):
        """inspect the queries according to the query"""
        session = Session()
        items = session.query(DownloadRegistry).all()
        data_objects = []

        for item in items:
            db_value = json.loads(item.content)

            if query:
                for parameter, value in query.items():
                    if db_value.get(parameter, "") == value:
                        db_value.update({"TaskId": item.id})
                        data_objects.append(db_value)
                        continue
            else:
                db_value.update({"TaskId": item.id})

                data_objects.append(db_value)

        return data_objects
