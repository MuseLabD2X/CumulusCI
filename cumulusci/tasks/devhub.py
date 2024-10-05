import json
from pydantic import BaseModel, Field
from cumulusci.tasks.salesforce import BaseSalesforceTask
from cumulusci.core.exceptions import DevhubAuthError
from cumulusci.core.sfdx import sfdx
from cumulusci.salesforce_api.utils import get_simple_salesforce_connection


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


class BaseDevhubTask(BaseSalesforceTask):
    """Base class for tasks that need DevHub access."""

    def _init_task(self):
        """Initialize the task and authenticate to DevHub."""
        super()._init_task()
        self.devhub = self._get_devhub_api()

    def _get_devhub_api(self, base_url=None):
        self.logger.info("Getting Dev Hub access token")
        p = sfdx(
            "config get target-dev-hub --json",
        )
        try:
            result = p.stdout_text.read()
            data = json.loads(result)
            devhub_username = data["result"][0]["value"]
        except json.JSONDecodeError:
            raise DevhubAuthError(
                f"Failed to parse SFDX output: {p.stdout_text.read()}"
            )
        except KeyError:
            raise DevhubAuthError(
                f"Failed to get Dev Hub username from sfdx. Please use `sfdx force:config:set target-dev-hub=<username>` to set the target Dev Hub org."
            )

        p = sfdx(
            f"force:org:display --json",
            username=devhub_username,
            log_note="Getting Dev Hub org info",
        )

        try:
            devhub_info = json.loads(p.stdout_text.read())
        except json.JSONDecodeError:
            raise DevhubAuthError(
                f"Failed to parse SFDX output: {p.stdout_text.read()}"
            )

        if "result" not in devhub_info:
            raise DevhubAuthError(
                f"Failed to get Dev Hub information from sfdx: {devhub_info}"
            )
        devhub = DevHubOrgConfig(
            access_token=devhub_info["result"]["accessToken"],
            instance_url=devhub_info["result"]["instanceUrl"],
        )
        return get_simple_salesforce_connection(
            project_config=self.project_config,
            org_config=devhub,
            api_version=(
                self.api_version
                if hasattr(self, "api_version")
                else self.project_config.project__package__api_version
            ),
            base_url=base_url,
        )
