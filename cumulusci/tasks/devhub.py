from pydantic import BaseModel, Field
from cumulusci.core.sfdx import get_devhub_api
from cumulusci.core.tasks import BaseTask


class DevHubOrgConfig(BaseModel):
    """Basic model for mimicking the org config for the Dev Hub org."""

    access_token: str = Field(
        ...,
        description="Access token for the Dev Hub org",
    )
    instance_url: str = Field(
        ...,
        description="Instance URL for the Dev Hub org",
    )


class BaseDevhubTask(BaseTask):
    """Base class for tasks that need DevHub access."""

    def _init_task(self):
        """Initialize the task and authenticate to DevHub."""
        super()._init_task()
        self.devhub = self._get_devhub_api()

    def _get_devhub_api(self, base_url=None):
        self.logger.info("Getting DevHub API for the default Dev Hub org")
        return get_devhub_api(
            project_config=self.project_config,
            api_version=self.api_version if hasattr(self, "api_version") else None,
            base_url=base_url,
        )
