"""Loads tasks from Asana."""
from __future__ import annotations

from typing import Any, List, Optional

from langchain.docstore.document import Document
from langchain.document_loaders.base import BaseLoader
from langchain.utils import get_from_env

import asana

class AsanaLoader(BaseLoader):
    """Asana loader. Reads all tasks from an Asana project or workspace."""

    def __init__(self, client: asana.Client, id: str, id_type: str):
        """Initialize Asana loader.

        Args:
            client: Asana API client.
            id: The id of the Asana project or workspace.
            id_type: The type of id. Must be either "project" or "workspace".
        """
        if id_type not in ["project", "workspace"]:
            raise ValueError("id_type must be either 'project' or 'workspace'")

        self.client = client
        self.id = id
        self.id_type = id_type

    @classmethod
    def from_credentials(cls, id: str, id_type: str, *, access_token: Optional[str] = None, **kwargs: Any) -> AsanaLoader:
        """Convenience constructor that builds AsanaClient init param for you.

        Args:
            id: The id of the Asana project or workspace.
            id_type: The type of id. Must be either "project" or "workspace".
            access_token: Asana personal access token. Can also be specified as environment variable ASANA_ACCESS_TOKEN.
        """
        try:
            import asana
        except ImportError as ex:
            raise ImportError(
                "Could not import Asana python package. "
                "Please install it with `pip install asana`."
            ) from ex

        access_token = access_token or get_from_env("access_token", "ASANA_ACCESS_TOKEN")
        client = asana.Client.access_token(access_token)
        return cls(client, id, id_type, **kwargs)

    def load(self) -> List[Document]:
        """Load tasks from Asana.

        Returns:
            A list of Document instances, one for each task.
        """
        if self.id_type == "workspace":
            workspace_id = self.id
            projects = self.client.projects.find_all({"workspace": workspace_id})
        elif self.id_type == "project":
            project_id = self.id
            projects = [self.client.projects.find_by_id(project_id)]
        else:
            raise ValueError("id_type must be either 'workspace' or 'project'")

        tasks = []
        for project in projects:
            tasks.extend(self.client.tasks.find_all(
                {
                    "project": project["gid"],
                    "opt_fields": "name,notes,completed,completed_at,completed_by,assignee,followers,custom_fields",
                }
            ))

        return [self._task_to_doc(task) for task in tasks]

    def _task_to_doc(self, task: Dict[str, Any]) -> Document:
        text_content = f"{task['name']}\n{task.get('notes', '')}"
        # metadata fields
        metadata = {
            "title": task['name'],
            "id": task.get('gid', 'unknown'),
            "assignee": task.get('assignee', {}).get('name', 'Unassigned') if task.get('assignee') else 'Unassigned',  # default to 'Unassigned' if no assignee
            "due_date": task.get('due_on', 'No due date'),  # default to 'No due date' if no due date is set
            "completed_at": task.get('completed_at', 'Not completed'),  # default to 'Not completed' if task is not completed
            "custom_fields": [i['display_value'] for i in task.get("custom_fields") if task.get("custom_fields") is not None],
            "completed_by": task.get('completed_by', {}).get('name', 'Unknown') if task.get('completed_by') else 'Unknown',  # default to 'Unknown' if not available
            "project_name": task.get('memberships', [{}])[0].get('project', {}).get('name', 'Unknown') if task.get('memberships') else 'Unknown',  # default to 'Unknown' if not available
            "workspace_name": task.get('workspace', {}).get('name', 'Unknown') if task.get('workspace') else 'Unknown'  # default to 'Unknown' if not available
        }

        if task.get("followers") is not None:
            metadata["followers"] = [i.get('name') for i in task.get("followers") if 'name' in i]
        else:
            metadata["followers"] = []

        return Document(page_content=text_content, metadata=metadata)
