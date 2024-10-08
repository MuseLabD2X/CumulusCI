import re
from enum import Enum
from pathlib import Path
from typing import List, Optional
from pydantic import AnyUrl, BaseModel, Field, root_validator
from cumulusci.core.exceptions import DeclarationConfigError
from cumulusci.utils.yaml.model_parser import CCIDictModel


class PackageType(Enum):
    """Enum for the type of package"""

    UNMANAGED = "unmanaged"
    MANAGED_1GP = "managed_1gp"
    MANAGED_2GP = "managed_2gp"
    UNLOCKED_2GP = "unlocked_2gp"


class TemplateAwareFilePath:
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, value):
        # Convert Path to string if necessary
        if isinstance(value, Path):
            value = str(value)
        if not isinstance(value, str):
            raise TypeError("Expected a string or Path-like object.")

        # Check for template tags like {foo}
        if re.search(r"{[^}]*}", value):
            return Path(value)  # Skip validation
        else:
            # Perform standard FilePath validation
            path = Path(value)
            if not path.exists():
                raise ValueError(f'File path "{value}" does not exist.')
            if not path.is_file():
                raise ValueError(f'Path "{value}" is not a file.')
            return path


class TemplateAwareDirectoryPath:
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, value):
        # Convert Path to string if necessary
        if isinstance(value, Path):
            value = str(value)
        if not isinstance(value, str):
            raise TypeError("Expected a string or Path-like object.")

        # Check for template tags like {foo}
        if re.search(r"{[^}]*}", value):
            return Path(value)  # Skip validation
        else:
            # Perform standard DirectoryPath validation
            path = Path(value)
            if not path.exists():
                raise ValueError(f'Directory path "{value}" does not exist.')
            if not path.is_dir():
                raise ValueError(f'Path "{value}" is not a directory.')
            return path


class HTTPMethod(Enum):
    GET = "GET"
    DELETE = "DELETE"
    PATCH = "PATCH"
    POST = "POST"
    PUT = "PUT"


class BaseDeclaration(CCIDictModel):
    @root_validator(pre=True)
    def validate_config(cls, values):  # pylint: disable=no-self-argument
        description = values.pop("description")
        if not any(values.values()):
            raise DeclarationConfigError(
                f"At least one {', '.join(list(values.keys()))} must be set"
            )
        values["description"] = description
        return values


class OrgSnapshotDeclaration(BaseDeclaration):
    creates: bool = Field(
        default=False,
        description="Whether the task creates a snapshot of the org's state",
    )
    modifies: bool = Field(
        default=False,
        description="Whether the task modifies a snapshot of the org's state",
    )
    deletes: bool = Field(
        default=False,
        description="Whether the task deletes a snapshot of the org's state",
    )
    description: str = Field(
        ...,
        description="A description of the task's interaction with snapshots",
    )


class PackageDeclaration(BaseDeclaration):
    name: Optional[str] = Field(
        default=None,
        description="The name of the package the task interacts with",
    )
    namespace: Optional[str] = Field(
        default=None,
        description="The namespace of the package the task interacts with",
    )
    package_type: Optional[str] = Field(
        default=None,
        description="The type of the package the task interacts with",
    )
    version: Optional[str] = Field(
        default=None,
        description="The version of the package the task interacts with",
    )
    version_name: Optional[str] = Field(
        default=None,
        description="The name of the package version the task interacts with",
    )
    package_id: Optional[str] = Field(
        default=None,
        description="The ID of the package the task interacts with",
    )
    version_id: Optional[str] = Field(
        default=None,
        description="The ID of the package version the task interacts with",
    )


class PackagesDeclaration(BaseDeclaration):
    installs: bool = Field(
        default=False,
        description="Whether the task installs a package version",
    )
    upgrades: bool = Field(
        default=False,
        description="Whether the task upgrades a package version",
    )
    uninstalls: bool = Field(
        default=False,
        description="Whether the task uninstalls a package version",
    )
    packages: List[PackageDeclaration] = Field(
        default=[],
        description="A list of packages the task interacts with, if known. Leave unset for tasks that can variably operate on many orgs",
    )
    description: str = Field(
        ...,
        description="A description of the task's interaction with packages in the org",
    )


class PackagingDeclaration(BaseDeclaration):
    creates: bool = Field(
        default=False,
        description="Whether the task creates a package version",
    )
    promotes: bool = Field(
        default=False,
        description="Whether the task promotes a package version",
    )
    deletes: bool = Field(
        default=False,
        description="Whether the task deletes a package version",
    )
    description: str = Field(
        ...,
        description="A description of the task's interaction with package versions",
    )


class DataDeclaration(BaseDeclaration):
    reads: bool = Field(
        default=False,
        description="Whether the task reads data from the org",
    )
    modifies: bool = Field(
        default=False,
        description="Whether the task modifies data in the org",
    )
    deletes: bool = Field(
        default=False,
        description="Whether the task deletes data from the org",
    )
    objects: List[str] = Field(
        default=[],
        description="A list of objects the task interacts with, if known. Leave unset for tasks that can variably operate on many orgs",
    )
    description: str = Field(
        ...,
        description="A description of the task's interaction with data",
    )


class MetadataDeclaration(BaseDeclaration):
    retrieves: bool = Field(
        default=False,
        description="Whether the task retrieves metadata from the org",
    )
    transforms: bool = Field(
        default=False,
        description="Whether the task transforms metadata",
    )
    deploys: bool = Field(
        default=False,
        description="Whether the task deploys metadata to the org",
    )
    deletes: bool = Field(
        default=False,
        description="Whether the task deletes metadata from the org",
    )
    metadata_types: List[str] = Field(
        default=[],
        description="A list of metadata types the task interacts with, if known. Leave unset for tasks that can variably operate on many orgs",
    )
    description: str = Field(
        ...,
        description="A description of the task's interaction with metadata",
    )


class SecurityDeclaration(BaseDeclaration):
    users: bool = Field(
        default=False,
        description="Whether the task modifies users",
    )
    assignments: bool = Field(
        default=False,
        description="Whether the task modifies permission set assignments",
    )
    licenses: bool = Field(
        default=False,
        description="Whether the task modifies license assignments",
    )
    description: str = Field(
        ...,
        description="A description of the task's interaction with security",
    )


class BaseApiTrackerConfig(BaseModel):
    actions: List[HTTPMethod] = Field(
        ...,
        description="A list of HTTP methods the task uses to interact with the API",
    )
    base_url: str = Field(
        ...,
        description="The base URL for the API. Can use template strings for version and other variables like {api_version} or {instance_url}.",
    )
    endpoints: Optional[List[str]] = Field(
        default=None,
        description="A list of API endpoints the task interacts with, if known. Leave unset for tasks that can variably operate on many orgs",
    )
    description: str = Field(
        ...,
        description="A description of the task's interaction with the API",
    )

    @root_validator(pre=True)
    def validate_api_config(cls, values) -> dict:  # pylint: disable=no-self-argument
        actions = values.pop("actions", None)
        if not actions:
            raise DeclarationConfigError("At least one action must be provided")

        validated = {k: v for k, v in values.items() if k != "description"}
        if not any(validated.values()):
            keys = list(validated.keys())
            keys = f"At least one of {', '.join(keys)}" if len(keys) > 1 else keys[0]
            raise DeclarationConfigError(f"{keys} must be provided")

        # Add actions back in
        values["actions"] = actions
        return values


class ToolingDeclaration(BaseApiTrackerConfig):
    runs_queries: bool = Field(
        default=False,
        description="Whether the task uses the Tooling API to query data. If True, the query endpoint is populated automatically",
    )
    runs_anon_apex: bool = Field(
        default=False,
        description="Whether the task uses the Tooling API to run anonymous Apex",
    )
    objects: Optional[List[str]] = Field(
        default=[],
        description="A list of Tooling API objects the task interacts with, if known. Leave unset for tasks that can variably operate on many orgs",
    )
    description: str = Field(
        ...,
        description="A description of the task's interaction with the Tooling API",
    )

    @root_validator(pre=True)
    def populate_query_endpoint(  # pylint: disable=no-self-argument
        cls, values
    ) -> dict:
        is_query = values.get("is_query")
        runs_anon_apex = values.get("runs_anon_apex")

        tooling_anon_apex_endpoint = (
            "/services/data/v{version}/tooling/executeAnonymous"
        )
        tooling_query_endpoint = "/services/data/v{version}/tooling/query"
        if is_query and tooling_query_endpoint not in values.setdefault(
            "endpoints", []
        ):
            values["endpoints"].append(tooling_query_endpoint)
        if runs_anon_apex and tooling_anon_apex_endpoint not in values.setdefault(
            "endpoints", []
        ):
            values["endpoints"].append(tooling_anon_apex_endpoint)
        return values


class CommandDeclaration(BaseModel):
    command: str = Field(
        ...,
        description="The command the task runs. Can be a template using {variables} to be replaced with task options",
    )
    command_options: List[str] = Field(
        default=[],
        description="The task options used to construct the command, if any",
    )
    passes_credentials: bool = Field(
        default=False,
        description="Whether the task passes credentials to the command",
    )
    description: str = Field(
        ...,
        description="A description of the task's command",
    )


class ExternalRepoReference(BaseModel):
    name: str = Field(
        ...,
        description="The name of the external repo",
    )
    url: AnyUrl = Field(
        ...,
        description="The URL of the external repo",
    )
    branch: Optional[str] = Field(
        default=None,
        description="The branch of the external repo",
    )


class FilesDeclaration(BaseModel):
    uses_repo_files: bool = Field(
        default=False,
        description="Whether the task uses files in the repo",
    )
    uses_repo_directories: bool = Field(
        default=False,
        description="Whether the task uses directories in the repo",
    )
    uses_external_repos: bool = Field(
        default=False,
        description="Whether the task uses files in external repos",
    )
    uses_external_urls: bool = Field(
        default=False,
        description="Whether the task uses files from external URLs",
    )
    repo_files: Optional[List[TemplateAwareFilePath]] = Field(
        default=None,
        description="A list of files in the repo the task interacts with. Can use template strings for variables like {options.path}",
    )
    repo_directories: Optional[List[TemplateAwareDirectoryPath]] = Field(
        default=None,
        description="A list of directories in the repo the task interacts with. Can use template strings for variables like {options.path}",
    )
    external_repos: Optional[List[TemplateAwareDirectoryPath]] = Field(
        default=None,
        description="A list of directories outside the repo the task interacts with. Can use template strings for variables like {options.repo_url}",
    )
    external_urls: Optional[List[str]] = Field(
        default=None,
        description="A list of URLs the task interacts with. Can use template strings for variables like {options.url}",
    )
    description: str = Field(
        ...,
        description="A description of the task's interaction with files",
    )


class DevhubDeclaration(BaseModel):
    uses_devhub: bool = Field(
        default=False,
        description="Whether the task uses the Dev Hub",
    )
    devhub_org: Optional[str] = Field(
        default=None,
        description="The Dev Hub org the task interacts with. If not specified, the default SF CLI target-dev-hub is looked up an used.",
    )
    description: str = Field(
        ...,
        description="A description of the task's interaction with the Dev Hub",
    )


class TaskDeclarations(BaseModel):
    """
    Model for tasks to declare their potential interactions with the org, API, filesystem, and external resources

    Task declarations are initialized in three stages:
    * **Class Level**: Without initializing a task instance, the class-level declarations represent the potential interactions of the task. Useful for task documentation.
    * **Instance Level**: When a task instance is created, the task can modify its declarations based on the options passed to the task.
    """

    can_predict_hashes: bool = Field(
        default=False,
        description="Whether the task can predict the action it will take to generate hashes for comparison",
    )
    can_rerun_safely: bool = Field(
        default=False,
        description="Whether the task can be rerun safely",
    )
    uses_services: Optional[List[str]] = Field(
        default=None,
        description="A list of services the task uses from the CumulusCI keychain",
    )
    commands: Optional[List[CommandDeclaration]] = Field(
        default=None,
        description="Configuration for tracking command operations",
    )
    data: Optional[List[DataDeclaration]] = Field(
        default=None,
        description="Configuration for tracking data operations",
    )
    files: Optional[FilesDeclaration] = Field(
        default=None,
        description="Configuration for tracking file references",
    )
    devhub: Optional[DevhubDeclaration] = Field(
        default=None,
        description="Configuration for tracking Dev Hub operations",
    )
    metadata: Optional[List[MetadataDeclaration]] = Field(
        default=None,
        description="Configuration for tracking metadata operations",
    )
    packages: Optional[PackagesDeclaration] = Field(
        default=None,
        description="Configuration for tracking package operations",
    )
    packaging: Optional[PackagingDeclaration] = Field(
        default=None,
        description="Configuration for tracking packaging operations",
    )
    security: Optional[SecurityDeclaration] = Field(
        default=None,
        description="Configuration for tracking security operations",
    )
    snapshots: Optional[OrgSnapshotDeclaration] = Field(
        default=None,
        description="Configuration for tracking org snapshot operations",
    )
    tooling: Optional[List[ToolingDeclaration]] = Field(
        default=None,
        description="Configuration for tracking Tooling API operations",
    )
