import copy
import json
from datetime import datetime
from enum import Enum
from logging import Logger
from pathlib import Path, PosixPath
from typing import Any, Dict, List, Optional, Tuple, Union
from typing_extensions import Literal
from jsonpath_ng import jsonpath, parse
from pydantic import (
    BaseModel,
    DirectoryPath,
    FilePath,
    Field,
    root_validator,
    validator,
)
from rich.text import Text
from cumulusci.core.declarations import PackageType
from cumulusci.core.exceptions import (
    OrgHistoryError,
    OrgActionNotFound,
)
from cumulusci.salesforce_api.package_models import (
    ApexCompileType,
    NameConflictResolution,
    SecurityType,
    UpgradeType,
)
from cumulusci.utils.hashing import hash_obj
from cumulusci.utils.serialization import (
    decode_value,
    encode_value,
)
from cumulusci.utils.yaml.render import yaml_dump
from cumulusci.utils.version_strings import StepVersion, LooseVersion
from cumulusci.utils.yaml.cumulusci_yml import Step, ScratchOrg
from cumulusci.utils.yaml.model_parser import CCIModel
from cumulusci.utils.masking import mask_in_json


class OrgActionStatus(Enum):
    """Enum for the status of an org history action"""

    SUCCESS = "success"
    FAILURE = "failure"
    ERROR = "error"


class BaseAction(BaseModel):
    class Config:
        use_enum_values = True

    @classmethod
    def parse_obj(cls, *args, **kwargs):
        obj = args[0]
        obj = decode_value(obj)
        obj = super().parse_obj(*args, **kwargs)
        return obj

    def dict(self, *args, **kwargs):
        kwargs.setdefault("exclude_defaults", True)
        value_only = kwargs.pop("value_only", False)
        obj = super().dict(*args, **kwargs)
        return encode_value(obj, value_only=value_only)


class ScratchOrgConfig(ScratchOrg, BaseAction):
    """Extension of ScratchOrg model for cumulusci.yml to include other fields added by org_config"""

    scratch: Literal[True] = True
    config_name: Optional[str] = None
    sfdx_alias: Optional[str] = None
    set_password: Optional[bool] = True


def encode_path(obj):
    import pdb

    pdb.set_trace()
    return str(obj)


class ActionFileReference(BaseAction):
    """Model for tracking task that references to files in the repo"""

    path: FilePath = Field(
        ...,
        description="The state of the metadata before the task ran",
    )
    hash: str = Field(
        ...,
        description="The hash of the file's contents",
    )

    class Config(BaseAction.Config):
        json_encoders = {Path: lambda v: encode_path(v), PosixPath: lambda v: str(v)}

    @root_validator(pre=True)
    def populate_hash(cls, values):
        path = values.get("path")
        if path:
            values["hash"] = hash_obj(path)
        return values


class ActionScratchDefReference(ActionFileReference):
    """Model for tracking task that references to scratch org definitions in the repo"""

    scratchdef: Dict[str, Any] = Field(
        ...,
        description="The scratch org definition used to create the org",
    )

    @root_validator(pre=True)
    def populate_hash(cls, values):
        path = values.get("path")
        if path:
            try:
                with open(path, "r") as file:
                    values["scratchdef"] = json.load(file)
            except Exception as e:
                raise ValueError(f"Error loading scratchdef from {path}: {e}")
            values["hash"] = (
                hash_obj(values["scratchdef"]) if values.get("scratchdef") else {}
            )

        return values

    def get_snapshot_shape_hash(self):
        hash_content = {
            "edition": self.scratchdef.get("edition"),
            "features": self.scratchdef.get("features"),
            "shape": self.scratchdef.get("shape"),
        }
        return hash_obj(hash_content)


class ActionDirectoryReference(BaseAction):
    """Model for tracking task that references to directories in the repo"""

    path: DirectoryPath = Field(
        ...,
        description="The path of the directory in the repo referenced by the task",
    )
    hash: str = Field(
        ...,
        description="The hash of the directory contents",
    )
    option: Optional[str] = Field(
        None,
        description="The option used to pass the directory to the task",
    )

    class Config(BaseAction.Config):
        json_encoders = {Path: lambda v: str(v), PosixPath: lambda v: str(v)}

    @root_validator(pre=True)
    def populate_hash(cls, values):
        path = values.get("path")
        if path:
            values["hash"] = hash_obj(path)
        return values


class ActionCommandExecution(BaseAction):
    """Model for tracking task command execution"""

    command: str = Field(
        ...,
        description="The command that was executed",
    )
    hash: str = Field(
        ...,
        description="The hash of the command",
    )
    return_code: int = Field(
        ...,
        description="The return code of the command",
    )
    output: str = Field(
        ...,
        description="The output of the command",
    )
    stderr: str = Field(
        ...,
        description="The stderr output of the command",
    )

    @root_validator(pre=True)
    def populate_hash(cls, values):
        command = values.get("command")
        if command:
            values["hash"] = hash_obj(command)
        return values

    # @validator("output", pre=True)
    # def mask_output(cls, v):
    #     # Mask accessToken in output
    #     v = mask_in_json(v)


class BaseMetadataApi(BaseAction):
    """Base class for tracking metadata api calls"""

    hash: str = Field(
        ...,
        description="The hash of the metadata deployment",
    )
    size: int = Field(
        ...,
        description="The size of the metadata deployment as a base64 encoded zip file",
    )


class ActionMetadataRetrieve(BaseMetadataApi):
    """Model for tracking task metadata retrieval"""

    pass


class ActionRepoMetadataDeploy(ActionDirectoryReference, BaseMetadataApi):
    """Model for tracking deployment of metadata from the repo"""

    pass


class ActionTransformMetadataDeploy(BaseMetadataApi):
    """Model for tracking task metadata deployments"""

    pass


class ActionMetadataTransform(BaseAction):
    """Model for tracking task metadata transformations (ETL)"""

    hash: str = Field(
        ...,
        description="The hash of the metadata transformation",
    )
    retrieve: Optional[ActionMetadataRetrieve] = Field(
        None,
        description="The metadata deployment used to retrieve metadata",
    )
    deploy: Optional[ActionTransformMetadataDeploy] = Field(
        None,
        description="The metadata deployment used to deploy metadata",
    )
    diff: Optional[str] = Field(
        None,
        description="The diff of the retrieved vs deployed metadata",
    )

    @root_validator(pre=True)
    def populate_hash(cls, values):
        if values.get("hash") is None:
            values["hash"] = hash_obj(values)
        return values


class BaseExternalZipMetadataDeploy(BaseMetadataApi):
    """Base class for tracking metadata deployments of zip file metadata not in the repo"""

    subfolder: Optional[str] = Field(
        None,
        description="The subfolder of the repository the metadata was deployed from",
    )


class ActionUrlMetadataDeploy(BaseExternalZipMetadataDeploy):
    url: str = Field(
        ...,
        description="The URL of the metadata deployment",
    )


class ActionGithubMetadataDeploy(BaseExternalZipMetadataDeploy):
    """Model for tracking task metadata deployments of metadata not in the repo"""

    repo: str = Field(
        ...,
        description="The name of the repository the metadata was deployed from",
    )
    commit: Optional[str] = Field(
        ...,
        description="The git ref of the metadata deployment",
    )
    branch: Optional[str] = Field(
        None,
        description="The branch of the repository the metadata was deployed from",
    )
    tag: Optional[str] = Field(
        None,
        description="The tag of the repository the metadata was deployed from",
    )


class ActionMetadataDeploys(BaseAction):
    """Model for tracking metadata deployments"""

    repo: Optional[List[ActionRepoMetadataDeploy]] = Field(
        ...,
        description="The metadata deployments from the repo",
    )
    github: Optional[List[ActionGithubMetadataDeploy]] = Field(
        ...,
        description="The metadata deployments from a GitHub repository",
    )
    zip_url: Optional[List[ActionUrlMetadataDeploy]] = Field(
        ...,
        description="The metadata deployments from a URL",
    )

    @property
    def has_deploys(self):
        return any([self.repo, self.github, self.zip_url])


class ActionPackageInstall(BaseAction):
    """Model for tracking task package installation"""

    hash: str = Field(
        ...,
        description="The hash of the metadata deployment",
    )
    version_id: Optional[str] = Field(
        None,
        description="The package version ID (04t...)",
    )
    namespace: Optional[str] = Field(
        None,
        description="The namespace of the package",
    )
    package_id: Optional[str] = Field(
        None,
        description="The package ID",
    )
    name: Optional[str] = Field(
        None,
        description="The package name",
    )
    version: Optional[str] = Field(
        None,
        description="The package version number",
    )
    package_type: Optional[PackageType] = Field(
        None,
        description="The type of package",
    )
    is_beta: Optional[bool] = Field(
        None,
        description="Whether the package is a beta package",
    )
    is_promotable: Optional[bool] = Field(
        None,
        description="Whether the package is promotable",
    )
    ancestor_id: Optional[str] = Field(
        None,
        description="The ancestor package version ID",
    )
    # password: Optional[str] = MaskedField(
    #     None,
    #     description="The password for the package",
    # )
    activate_remote_site_settings: bool = Field(
        False,
        description="Whether to activate remote site settings",
    )
    name_conflict_resolution: NameConflictResolution = Field(
        ...,
        description="The name conflict resolution strategy",
    )
    security_type: SecurityType = Field(
        ...,
        description="The security type",
    )
    apex_compile_type: Optional[ApexCompileType] = Field(
        None,
        description="The Apex compile type",
    )
    upgrade_type: Optional[UpgradeType] = Field(
        None,
        description="The upgrade type",
    )

    @root_validator(pre=True)
    def validate_package_version(cls, values):
        """Validate that either version_id or namespace and version are provided"""
        version_id = values.get("version_id")
        if not version_id:
            if not values.get("namespace") and not values.get("version"):
                raise ValueError(
                    "Either version_id or namespace and version is required"
                )
            if not values.get("namespace") or not values.get("version"):
                raise ValueError(
                    "Both namespace and version are required or provide a version_id"
                )
        return values

    @root_validator(pre=True)
    def populate_hash(cls, values):
        include = [
            "version_id",
            "namespace",
            "package_id",
            "name",
            "version",
            "package_type",
            "is_beta",
            "is_promotable",
            "ancestor_id",
            "activate_remote_site_settings",
            "name_conflict_resolution",
            "security_type",
            "apex_compile_type",
            "upgrade_type",
        ]
        values["hash"] = hash_obj(
            [values.get(key) for key in include if values.get(key)]
        )
        return values


class ActionPackageUpgrade(ActionPackageInstall):
    previous_version_id: Optional[str] = Field(
        None,
        description="The previous package version ID (04t...) if the package is an upgrade",
    )
    previous_version: Optional[str] = Field(
        None,
        description="The previous package version number if the package is an upgrade",
    )

    @root_validator(pre=True)
    def validate_previous_version(cls, values):
        """Validate that either previous_version_id or previous_version are provided"""
        if not values.get("previous_version_id") and not values.get("previous_version"):
            raise ValueError(
                "Either previous_version_id or previous_version must be set"
            )


class BaseOrgAction(BaseAction):
    """Base class for tracking actions run against an org"""

    timestamp: float = Field(
        default_factory=lambda: datetime.now().timestamp(),
        description="The timestamp the action was run",
    )
    repo: Optional[str] = Field(
        None,
        description="The name of the repository the action was run against",
    )
    branch: Optional[str] = Field(
        None,
        description="The name of the branch the action was run against",
    )
    commit: Optional[str] = Field(
        None,
        description="The commit SHA the action was run against",
    )


class BaseOrgActionResult(BaseOrgAction):
    hash_action: Optional[str] = Field(
        None,
        description="A unique hash for the action instance",
    )
    hash_config: Optional[str] = Field(
        None,
        description="A unique hash representing the action instance's configuration",
    )
    duration: float = Field(
        ...,
        description="The duration of the action",
    )
    status: OrgActionStatus = Field(
        ...,
        description="The status of the action",
    )
    log: str = Field(
        ...,
        description="The log output of the action",
    )
    exception: Optional[str] = Field(
        None,
        description="The exception message if the action failed",
    )

    @root_validator(pre=True)
    def populate_duration(cls, values):
        duration = values.get("duration")
        if not duration:
            timestamp = values.get("timestamp")
            if timestamp:
                values["duration"] = datetime.now().timestamp() - timestamp
        return values

    def get_config_hash_data(self):
        data = {
            "action_type": self.action_type,
        }
        return data

    def calculate_action_hash(self):
        """A unique hash for the action instance"""
        hash_data = self.dict()
        return hash_obj(hash_data)

    def calculate_config_hash(self):
        """A unique hash representing the action's non-instance specific configuration"""
        hash_data = self.get_config_hash_data()
        hashed = hash_obj(hash_data)
        return hashed

    @property
    def column_hash(self):
        return f"Action:\n[bold]{self.hash_action}[/bold]\n\nConfig:\n[bold]{self.hash_config}[/bold]"

    @property
    def column_date(self):
        date = datetime.fromtimestamp(self.timestamp)
        data = []
        data.append(date.strftime("%Y-%m-%d"))
        data.append(date.strftime("%H:%M:%S"))
        data.append(f"{self.duration:.2f}s")
        return "\n".join(data)

    @property
    def column_type(self):
        if hasattr(self, "action_type"):
            return self.action_type
        return self.__class__.__name__.replace("OrgAction", "").replace("Action", "")

    @property
    def column_status(self):
        status_colors = {
            "success": "green",
            "failure": "orange",
            "error": "red",
        }
        color = status_colors.get(self.status, "white")
        return Text(str(self.status), style=color)

    @property
    def column_details(self):
        if self.status == OrgActionStatus.SUCCESS.value:
            log = self.log
            if log:
                log_parts = log.split("\n")[:-3]
                if isinstance(log, Text):
                    log_text = Text()
                    for line in log_parts:
                        log_text.append(line)
                    log = log_text
                else:
                    log = "\n".join(log_parts)

            return log
        if self.status == OrgActionStatus.FAILURE.value:
            return self.exception
        if self.status == OrgActionStatus.ERROR.value:
            return self.exception


class SFCLIOrgActionMixin(BaseAction):
    sf_command: ActionCommandExecution = Field(
        ...,
        description="The Salesforce CLI command used to create the org",
    )

    def get_config_hash_data(self):
        data = super().get_config_hash_data()
        data.update(self.sf_command.command)
        return data

    @property
    def log(self):
        log = Text(self.sf_command.output)

        log.append(self.sf_command.stderr, style="bold yellow")
        return log


class OrgCreateAction(BaseOrgActionResult, SFCLIOrgActionMixin):
    """Model for tracking org creations"""

    action_type: Literal["OrgCreate"] = Field()
    days: int = Field(
        ...,
        description="The number of days the org is configured to live",
        max=30,
        min=1,
    )
    namespaced: bool = Field(
        ...,
        description="Whether the org is namespaced",
    )
    scratch_org: ScratchOrgConfig = Field(
        ...,
        description="The scratch org configuration from cumulusci.yml's orgs -> scratch section.",
    )
    config: Optional[ActionScratchDefReference] = Field(
        None,
        description="The scratch org definition used to create the org",
    )
    org_id: Optional[str] = Field(
        None,
        description="The Salesforce org ID",
    )
    username: Optional[str] = Field(
        None,
        description="The username of the org",
    )
    devhub: Optional[str] = Field(
        None,
        description="The username of the Dev Hub org",
    )
    sfdx_alias: Optional[str] = Field(
        None,
        description="The alias of the org in the SF CLI keychain",
    )
    login_url: Optional[str] = Field(
        None,
        description="The instance URL of the org",
    )
    instance: Optional[str] = Field(
        None,
        description="The instance of the org",
    )

    def get_snapshot_shape_hash(self):
        return self.config.get_snapshot_shape_hash()

    def get_config_hash_data(self):
        data = super().get_config_hash_data()
        data["config"] = self.config.hash
        data["namespaced"] = self.namespaced
        return data

    @property
    def column_details(self):
        if self.status == OrgActionStatus.SUCCESS.value:
            details = []
            details.append(f"SF CLI Alias:  {self.sfdx_alias}")
            details.append(f"Org ID:        {self.org_id}")
            details.append(f"Username:      {self.username}")
            # details.append(f"Dev Hub:       {self.devhub}")
            # details.append(f"Instance:      {self.instance}")
            # details.append(f"Login URL:     {self.login_url}")
            details.append(f"Days:          {self.days}")
            details.append(f"Namespaced:    {self.namespaced}")
            details.append(f"ScratchDef:    {self.config.path}")
            return "\n".join(details)
        if self.status == OrgActionStatus.FAILURE.value:
            return self.exception
        if self.status == OrgActionStatus.ERROR.value:
            return self.exception


class OrgConnectAction(BaseOrgActionResult):
    """Model for tracking org connections"""

    action_type: Literal["OrgConnect"] = Field()


class OrgDeleteAction(BaseOrgActionResult, SFCLIOrgActionMixin):
    """Model for tracking org deletions"""

    action_type: Literal["OrgDelete"] = Field()
    org_id: str = Field(
        ...,
        description="The Salesforce org ID",
    )


class OrgImportAction(BaseOrgActionResult):
    """Model for tracking org imports"""

    action_type: Literal["OrgImport"] = Field()
    sf_org_name: str = Field(
        ...,
        description="The name of the Salesforce org in the SF CLI keychain",
    )


class BaseTaskAction(BaseAction):
    """Model for tracking an actively running task run against an org
    for later reporting as an action with results
    """

    name: str = Field(
        ...,
        description="The name of the task",
    )
    description: Optional[str] = Field(
        None,
        description="The description of the task",
    )
    group: Optional[str] = Field(
        None,
        description="The group of the task",
    )
    class_path: str = Field(
        ...,
        description="The class of the task",
    )
    options: dict = Field(
        ...,
        description="The options passed to the task",
    )
    parsed_options: dict = Field(
        {},
        description="The options after being parsed by the task",
    )
    files: Optional[List[ActionFileReference]] = Field(
        [],
        description="The file references of the task",
    )
    directories: Optional[List[ActionDirectoryReference]] = Field(
        [],
        description="The directory references of the task",
    )
    commands: Optional[List[ActionCommandExecution]] = Field(
        [],
        description="The commands executed by the task",
    )
    deploys: Optional[ActionMetadataDeploys] = Field(
        {},
        description="The metadata deployments executed by the task",
    )
    retrieves: Optional[List[ActionMetadataRetrieve]] = Field(
        [],
        description="The metadata retrievals executed by the task",
    )
    transforms: Optional[List[ActionMetadataTransform]] = Field(
        [],
        description="The metadata transformations executed by the task",
    )
    package_installs: Optional[List[ActionPackageInstall | ActionPackageUpgrade]] = (
        Field(
            [],
            description="The package installs executed by the task",
        )
    )

    def get_config_hash_data(self, static: bool = True):
        deploy_hashes = []
        if self.deploys:
            for deploy in self.deploys.dict().values():
                deploy_hashes.extend(deploy)
        return {
            "action_type": self.action_type,
            "class_path": self.class_path,
            "options": self.parsed_options if static else self.options,
            "files": [f.hash for f in self.files],
            "directories": [d.hash for d in self.directories],
            "commands": [c.hash for c in self.commands],
            "deploys": deploy_hashes,
            "retrieves": [r.hash for r in self.retrieves],
            "transforms": [t.hash for t in self.transforms],
            "package_installs": [p.hash for p in self.package_installs],
        }

    def get_org_tracker_hash(
        self,
        return_data: bool = False,
    ) -> str | Dict[str, Dict[str, List[str]]]:
        hashes = {
            "deploys": [],
            "transforms": [],
            "package_installs": [],
        }
        if self.deploys:
            for deploy in self.deploys.dict().values():
                hashes["deploys"].extend([d.get("hash") for d in deploy])
        if self.transforms:
            for transform in self.transforms:
                hashes["transforms"].append(transform.hash)
        if self.package_installs:
            for package_install in self.package_installs:
                hashes["package_installs"].append(package_install.hash)
        if all([not value for value in hashes.values()]):
            return None
        if return_data:
            return hashes
        return hash_obj(hashes)

    @property
    def column_details(self):
        details = []
        details.append(f"Task: {self.name}")
        details.append(f"Class: {self.class_path}")
        details.append("Options:")
        for key, value in self.options.items():
            details.append(f"  {key}: {value}")

        if self.exception is not None and self.exception != "None":
            details.append("Exception:")
            details.append(self.exception)
        return "\n".join(details)


class TaskActionTracker(BaseTaskAction, BaseOrgAction):
    """Model for tracking a single task run against an org"""

    pass


class TaskOrgAction(BaseTaskAction, BaseOrgActionResult):
    """Model for the outcome of a single task run against an org"""

    action_type: Literal["Task"] = Field()
    return_values: dict = Field(
        ...,
        description="The return values of the task",
    )

    def get_runnable_dependencies(self, project_config):
        """Return a list of dependencies that are valid input ProjectDependencies"""

        # Handle update_dependencies task
        if self.name == "update_dependencies" and "dependencies" in self.return_values:
            dependencies = self.return_values["dependencies"]

            # Convert the dependencies to the format expected by the task
            formatted_dependencies = []
            for dep in dependencies:
                if "namespace" in dep:
                    formatted_dep = {
                        "namespace": dep["namespace"],
                        "version": dep["version"],
                    }
                elif "github" in dep:
                    formatted_dep = {
                        "github": dep["github"],
                        "subfolder": dep.get("subfolder"),
                        "ref": dep.get("ref"),
                        "unmanaged": dep.get("unmanaged", False),
                        "namespace_inject": dep.get("namespace_inject"),
                    }
                    formatted_dep = {
                        k: v for k, v in formatted_dep.items() if v is not None
                    }
                else:
                    # Handle other types of dependencies if necessary
                    formatted_dep = dep

                formatted_dependencies.append(formatted_dep)

            return formatted_dependencies


class BaseFlowAction(BaseAction):
    name: str = Field(
        ...,
        description="The name of the flow",
    )
    description: Optional[str] = Field(
        None,
        description="The description of the flow",
    )

    group: Optional[str] = Field(
        None,
        description="The group of the flow",
    )
    config_steps: Dict[StepVersion, Step] = Field(
        ...,
        description="The flow configuration",
    )

    class Config(BaseAction.Config):
        arbitrary_types_allowed = True

    @root_validator(pre=True)
    def validate_version(cls, v):
        if "config_steps" not in v:
            raise ValueError("config_steps is required")
        if not isinstance(v["config_steps"], dict):
            raise ValueError("config_steps must be a dictionary")
        keys = [k for k in v["config_steps"].keys() if not isinstance(k, StepVersion)]
        for key in keys:
            v["config_steps"][StepVersion(str(key))] = v["config_steps"].pop(key)
        return v

    def get_config_hash_data(self):
        return {
            "action_type": self.action_type,
            "config_steps": self.config_steps,
        }

    def _get_details(self):
        details = []
        details.append(f"Flow: {self.name}")
        if self.group is not None and self.group != "None":
            details.append(f"Group: {self.group}")
        if self.config_steps and len(self.config_steps) > 0:
            details.append("Config:")
            details.append(yaml_dump(self.config_steps, include_types=False))
        return details

    @property
    def column_details(self):
        return "\n".join(self._get_details())


class FlowActionStepTracker(BaseAction):
    task: TaskActionTracker = Field(
        ...,
        description="The initialized task tracker for the step",
    )
    when: Optional[str] = Field(
        None,
        description="The condition for the step to run",
    )


class FlowActionStep(BaseAction):
    task: TaskOrgAction = Field(
        ...,
        description="The initialized task tracker for the step",
    )


class FlowActionTracker(BaseFlowAction, BaseOrgAction):
    """Model for tracking a flow run against an org"""

    steps: List[FlowActionStepTracker] = Field(
        ...,
        description="The initialized step trackers for each flow step",
    )


class FlowOrgAction(BaseFlowAction, BaseOrgActionResult):
    """Model for the outcome of a flow run against an org"""

    action_type: Literal["Flow"] = Field()
    steps: List[FlowActionStep] = Field(
        ...,
        description="The details and results from all steps in the flow",
    )

    def get_config_hash_data(self):
        return {
            "action_type": self.action_type,
            "config_steps": self.config_steps,
            "steps": [step.task.hash_config for step in self.steps],
        }

    def get_org_tracker_hash(
        self,
        return_data: bool = False,
    ) -> str | Dict[str, Dict[str, List[str]]] | None:
        hashes = {
            "deploys": [],
            "transforms": [],
            "package_installs": [],
        }
        for step in self.steps:
            step_data = step.task.get_org_tracker_hash(return_data=True)
            if step_data is None:
                continue
            hashes["deploys"].extend(step_data["deploys"])
            hashes["transforms"].extend(step_data["transforms"])
            hashes["package_installs"].extend(step_data["package_installs"])

        if all([not value for value in hashes.values()]):
            return None
        if return_data:
            return hashes
        return hash_obj(hashes)

    @property
    def log(self):
        log = []
        log.append(f"Flow: {self.name}")
        if self.group is not None and self.group != "None":
            log.append(f"Group: {self.group}")
        log.append(f"Status: {self.status}")
        log.append(f"Duration: {self.duration}")
        log.append("Details:")
        log.extend(self._get_details())
        return "\n".join(log)

    @property
    def return_values(self):
        return_values = {}
        for step in self.steps:
            return_values.setdefault(step.task.name, {}).update(step.task.return_values)

    def _get_details(self):
        details = super()._get_details()
        if self.exception is not None and self.exception != "None":
            details.append("Exception:")
            details.append("\n".join(self.exception.split("\n")[:3]) + "\n...")
        details.append("Steps:")
        for step in self.steps:
            details.append(f"  {step.task.name}")
            details.append(f"    Status: {step.task.status}")
            details.append(f"    Duration: {step.task.duration}")
        return details


OrgActionType = Union[
    OrgConnectAction,
    OrgImportAction,
    OrgDeleteAction,
    OrgCreateAction,
    TaskOrgAction,
    FlowOrgAction,
]


class FilterOrgActions(BaseAction):
    action_type: Optional[
        List[
            Literal["OrgCreate", "OrgConnect", "OrgImport", "OrgDelete", "Task", "Flow"]
        ]
        | str
    ] = Field(
        None,
        description="Filter to only actions of the specified type(s)",
    )
    status: Optional[List[OrgActionStatus] | str] = Field(
        None,
        description="Filter to only actions with the specified status(es)",
    )
    action_hash: Optional[List[str] | str] = Field(
        None,
        description="Filter to only actions with the specified action hash(es)",
    )
    config_hash: Optional[List[str] | str] = Field(
        None,
        description="Filter to only actions with the specified config hash(es)",
    )
    exclude_action_hash: Optional[List[str] | str] = Field(
        None,
        description="Exclude actions with the specified action hash(es)",
    )
    exclude_config_hash: Optional[List[str] | str] = Field(
        None,
        description="Exclude actions with the specified config hash(es)",
    )
    before: Optional[str] = Field(
        None,
        description="Filter to only actions before the specified action hash",
    )
    after: Optional[str] = Field(
        None,
        description="Filter to only actions after the specified action hash",
    )


class BaseOrgHistory(BaseAction):
    """Base model for tracking the history of actions against an org instance"""

    hash_config: Optional[str] = Field(
        None,
        description="A unique hash representing the complete configuration of the org",
    )

    actions: List[OrgActionType] = Field(
        ...,
        description="The actions run against the org",
    )

    def calculate_config_hash(self):
        hash_data = [action.hash_config for action in self.actions]
        return hash_obj(hash_data)

    def filtered_actions(
        self,
        filters: FilterOrgActions,
    ) -> List[OrgActionType]:
        """Return a list of actions that match the provided filters"""

        def process_string_to_list(value) -> List[str] | None:
            if value is None:
                return None
            if isinstance(value, str):
                return str(value).split(",")
            if isinstance(value, list):
                return value
            raise ValueError(f"{value} must be a string or list")

        # A list of strings that should be converted to lists
        strings_to_lists = [
            "status",
            "action_hash",
            "config_hash",
            "exclude_action_hash",
            "exclude_config_hash",
        ]

        if isinstance(filters, dict):
            filters = FilterOrgActions(**filters)

        # Convert strings to lists as needed
        for field in filters.__fields__.keys():
            value = getattr(filters, field)
            if field in strings_to_lists:
                setattr(filters, field, process_string_to_list(value))

        actions = self.actions
        if filters.action_type:
            actions = [a for a in actions if a.action_type in filters.action_type]
        if filters.status:
            actions = [a for a in actions if a.status in filters.status]
        if filters.action_hash:
            actions = [a for a in actions if a.hash_action in filters.action_hash]
        if filters.config_hash:
            actions = [a for a in actions if a.hash_config in filters.config_hash]
        if filters.exclude_action_hash:
            actions = [
                a for a in actions if a.hash_action in filters.exclude_action_hash
            ]
        if filters.exclude_config_hash:
            actions = [
                a for a in actions if a.hash_config not in filters.exclude_config_hash
            ]
        if filters.before:
            actions = [a for a in actions if a.hash_action < filters.before]
        if filters.after is not None:
            actions = [a for a in actions if a.hash_action > filters.after]
        return actions

    def get_org_actions(self, org_id: str = None) -> List[OrgActionType]:
        if org_id:
            if hasattr(self, "previous_orgs") and org_id in self.previous_orgs:
                return self.previous_orgs[org_id].actions
            if org_id != self.config.get("org_id"):
                raise OrgActionNotFound(
                    f"Org with ID {org_id} not found in org history"
                )
            return self.filtered_actions(action_type="OrgCreate", status="success")
        return self.actions

    def get_action_by_hash(
        self, hash: str, previous_org: str = None
    ) -> OrgActionType | None:
        for action in self.actions:
            if action.hash_action == hash:
                return action
        raise OrgActionNotFound(f"Action with hash {hash} not found in org history")

    def get_org_tracker_hash(
        self,
        filters: FilterOrgActions | None = None,
        return_data: bool = False,
    ) -> str | Dict[str, List[str]]:
        """Calculate a hash of all tracked operations that impacted the org"""
        hashes: Dict[str, Optional[Union[str, List[str]]]] = {
            "deploys": [],
            "package_installs": [],
            "transforms": [],
        }

        if filters is None:
            filters = FilterOrgActions()
        if isinstance(filters, dict):
            filters = FilterOrgActions.parse_obj(filters)
        filters.action_type = ["Task", "Flow"]
        filters.status = "success"

        for action in self.filtered_actions(filters):
            action_data = action.get_org_tracker_hash(return_data=True)
            if action_data is None:
                continue
            hashes["deploys"].extend(action_data["deploys"])
            hashes["package_installs"].extend(action_data["package_installs"])
            hashes["transforms"].extend(action_data["transforms"])

        if return_data:
            return hashes
        return hash_obj(hashes)

    def get_snapshot_hash(
        self,
        filters: FilterOrgActions | None = None,
        return_data: bool = False,
    ) -> str | Dict[str, Dict[str, List[str] | str]] | None:
        """Calculate a hash of the org's shape and all tracked operations that impacted the org"""
        tracker_hash_content = self.get_org_tracker_hash(filters, return_data=True)

        if filters is None:
            filters = FilterOrgActions()
        if isinstance(filters, dict):
            filters = FilterOrgActions.parse_obj(filters)
        filters.action_type = ["Task", "Flow"]
        filters.status = "success"

        org_shape = None
        create_action = self.filtered_actions(
            FilterOrgActions(action_type="OrgCreate", status="success")
        )
        if create_action:
            org_shape = create_action[0].get_snapshot_shape_hash()

        if not org_shape and not tracker_hash_content:
            return None

        hashes: Dict[str, Optional[Union[str, List[str]]]] = {
            "org_shape": org_shape,
            "tracker": tracker_hash_content,
        }
        if return_data:
            return hashes
        return hash_obj(hashes)

    def clear_history(
        self,
        filters: FilterOrgActions,
        logger: Logger,
    ):
        """Clear the org's history"""
        logger.info(f"Starting length: {len(self.history.actions)}")

        if isinstance(filters, dict):
            filters = FilterOrgActions.parse_obj(filters)

        if org_id:
            history = self.previous_orgs[org_id]
        else:
            history = self.history

        actions = history.filtered_actions(filters)
        for action in actions:
            logger.warning(f"Removing {action.action_type} action: {action}")
            history.actions.remove(action)

        if org_id:
            history.hash_config = history.calculate_config_hash()
        self.history.hash_config = self.history.calculate_config_hash()
        logger.info(f"Recalculated history config hash: {self.history.hash_config}")

        self.logger.info(f"Ending length: {len(self.history.actions)}")
        self.config["history"] = self.history.dict()
        self.save()


class PreviousOrgHistory(BaseOrgHistory):
    """Model for tracking the history of actions run against a previous org instance"""

    org_config: Optional[dict] = Field(
        None,
        description="The config of the previous org",
    )


class OrgHistory(BaseOrgHistory):
    """Model for tracking the history of actions run against a CumulusCI org profile"""

    org_id: Optional[str] = Field(
        None,
        description="The Salesforce org ID",
    )
    previous_orgs: Dict[str, PreviousOrgHistory] = Field(
        {},
        description="The previous orgs that were created and deleted and their action history.",
    )

    def lookup_org(
        self, org_id: str | None = None
    ) -> Union["OrgHistory", PreviousOrgHistory]:
        if org_id:
            if hasattr(self, "previous_orgs") and org_id in self.previous_orgs:
                try:
                    return self.previous_orgs[org_id].org_config
                except KeyError:
                    raise OrgHistoryError(
                        f"Org with ID {org_id} not found in org history",
                    )
            if org_id != self.config.get("org_id"):
                raise OrgHistoryError(f"Org with ID {org_id} not found in org history")
            return self
        return self

    def rotate_org(self, org_config: dict):
        """Rotate the org history to a new org ID"""
        org_id = org_config["org_id"]
        if org_id in self.previous_orgs.keys():
            raise OrgHistoryError(f"Org with ID {org_id} already exists in org history")

        org_config = copy.deepcopy(org_config)

        # Remove keys that should not be stored in the org history
        remove = [
            "history",
            "password",
            "access_token",
            "refresh_token",
            "email_address",
        ]
        for key in remove:
            if key in org_config:
                del org_config[key]

        self.previous_orgs[org_id] = PreviousOrgHistory(
            hash_config=self.hash_config,
            org_config=org_config,
            actions=self.actions.copy(),
        )
        self.previous_orgs[org_id].hash_config = self.calculate_config_hash()
        self.actions = []
        self.hash_config = self.calculate_config_hash()

    def filtered_actions(
        self,
        filters: FilterOrgActions,
        org_id: Optional[str] = None,
    ) -> List[OrgActionType]:
        if org_id:
            # For previous orgs, pass off to their history
            try:
                history = self.previous_orgs[org_id]
                return history.filtered_actions(filters)
            except KeyError:
                raise OrgActionNotFound(
                    f"Org with ID {org_id} not found in org history"
                )
        return super().filtered_actions(filters)

    @classmethod
    def parse_obj(cls, obj):
        if isinstance(obj, dict):
            obj = history_from_dict(obj)

        obj = super().parse_obj(obj)
        return obj


def actions_from_dicts(actions: list[dict]) -> List[OrgActionType]:
    mapped = []
    for action_data in actions:
        try:
            action: OrgActionType | None = None
            if "action_type" not in action_data:
                raise ValueError("Missing required key 'action_type'")
            if action_data["action_type"] == "OrgConnect":
                action = OrgConnectAction.parse_obj(action_data)
            elif action_data["action_type"] == "OrgImport":
                action = OrgImportAction.parse_obj(action_data)
            elif action_data["action_type"] == "OrgDelete":
                action = OrgDeleteAction.parse_obj(action_data)
            elif action_data["action_type"] == "OrgCreate":
                action = OrgCreateAction.parse_obj(action_data)
            elif action_data["action_type"] == "Task":
                action = TaskOrgAction.parse_obj(action_data)
            elif action_data["action_type"] == "Flow":
                keys = [key for key in action_data["config_steps"].keys()]
                for key in keys:
                    if isinstance(key, LooseVersion):
                        action_data["config_steps"][StepVersion(str(key))] = (
                            action_data["config_steps"].pop(key)
                        )
                action = FlowOrgAction.parse_obj(action_data)
            else:
                raise ValueError(f"Unknown action_type: {action_data['action_type']}")
            action.hash_action = action.calculate_action_hash()
            action.hash_config = action.calculate_config_hash()

        except Exception as e:
            import pdb

            pdb.set_trace()
            raise OrgHistoryError(f"Error loading action: {e}")
        mapped.append(action)
    return mapped


def history_from_dict(data: dict) -> "OrgHistory":
    actions: List[OrgActionType] = []
    previous_orgs: Dict[str, List[OrgActionType]] = {}
    actions = actions_from_dicts(data.get("actions", []))
    for org_id, previous_org in data.get("previous_orgs", {}).items():
        if isinstance(previous_org, list):
            hash_config = None
            prev_actions = actions_from_dicts(previous_org)
        else:
            hash_config = previous_org.get("hash_config")
            prev_actions = actions_from_dicts(previous_org.get("actions", []))
        previous_orgs[org_id] = PreviousOrgHistory(
            hash_config=hash_config,
            actions=prev_actions,
        )
        if hash_config is None:
            previous_orgs[org_id].hash_config = previous_orgs[
                org_id
            ].calculate_config_hash()
    history = OrgHistory(
        hash_config=None,
        actions=actions,
        previous_orgs=previous_orgs,
    )
    if "hash_config" not in data or data["hash_config"] is None:
        history.hash_config = history.calculate_config_hash()
    return history
