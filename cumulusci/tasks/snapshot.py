import json
import os
import time
from typing import Dict, Literal, List, Optional, Tuple
import yaml
from datetime import datetime, timedelta
from dateutil.parser import parse
from cumulusci.core.dependencies import (
    StaticDependency,
)
from cumulusci.core.exceptions import (
    SalesforceException,
    ScratchOrgSnapshotError,
    ScratchOrgSnapshotFailure,
)
from cumulusci.core.github import set_github_output
from cumulusci.core.sfdx import sfdx

from cumulusci.core.utils import process_bool_arg, process_list_arg
from cumulusci.salesforce_api.utils import get_simple_salesforce_connection
from cumulusci.tasks.salesforce import BaseSalesforceTask
from cumulusci.tasks.devhub import BaseDevhubTask
from cumulusci.tasks.github.base import BaseGithubTask
from cumulusci.utils.hashing import hash_dict
from cumulusci.utils.options import (
    CCIOptions,
    DirectoryPath,
    FilePath,
    Field,
    ListOfStringsOption,
)
from cumulusci.utils.yaml.cumulusci_yml import ProjectDependencies
from github3 import GitHubError
from pydantic import BaseModel, Field, validator
from simple_salesforce import Salesforce
from simple_salesforce.exceptions import SalesforceResourceNotFound
from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    Progress,
    SpinnerColumn,
    TextColumn,
    BarColumn,
    TimeElapsedColumn,
)
from rich.table import Table

ORG_SNAPSHOT_FIELDS = [
    "Id",
    "SnapshotName",
    "Description",
    "Status",
    "SourceOrg",
    "CreatedDate",
    "LastModifiedDate",
    "ExpirationDate",
    "Error",
]


class SnapshotNameValidator(BaseModel):
    base_name: str = Field(..., max_length=13)

    @classmethod
    @validator("base_name")
    def validate_name(cls, name):
        if len(name) > 13:
            raise ValueError("Snapshot name cannot exceed 13 characters")
        if not name.isalnum():
            raise ValueError("Snapshot name must only contain alphanumeric characters")
        if name[0].isdigit():
            raise ValueError("Snapshot name must start with a letter")
        return name

class SnapshotManager:
    def __init__(self, devhub, logger):
        self.devhub = devhub
        self.logger = logger
        self.existing_active_snapshot_id = None
        self.temporary_snapshot_name = None
        self.console = Console()

    def generate_temp_name(self, base_name: str, max_length: int = 14) -> str:
        temp_name = f"{base_name}0"
        if len(temp_name) > max_length:
            temp_name = temp_name[:max_length]
        self.logger.info(f"Generated temporary snapshot name: {temp_name}")
        return temp_name

    def query(
        self,
        snapshot_id: Optional[str] = None,
        snapshot_name: Optional[str] = None,
        description: Optional[str] = None,
        status: Optional[list] = None,
    ):
        query = f"SELECT {', '.join(ORG_SNAPSHOT_FIELDS)} FROM OrgSnapshot WHERE SnapshotName = '{snapshot_name}'"
        where = []
        if snapshot_id:
            where.append(f"Id = '{snapshot_id}'")
        if snapshot_name:
            where.append(f"SnapshotName = '{snapshot_name}'")
        if description:
            where.append(f"Description LIKE '{description}'")
        if status:
            where.append(f"Status IN ({', '.join([f'\'{s}\'' for s in status])})")
        return self.devhub.query(query)

    def query_existing_active_snapshot(self, snapshot_name: str):
        self.logger.info(
            f"Checking for existing active snapshot with name: {snapshot_name}"
        )
        result = self.query(snapshot_name=snapshot_name, status=["Active"])
        if result["totalSize"] > 0:
            self.existing_active_snapshot_id = result["records"][0]["Id"]
            self.logger.info(
                f"Found existing active snapshot: {self.existing_active_snapshot_id}"
            )
        else:
            self.logger.info(f"No active snapshot found with name {snapshot_name}")

    def query_and_delete_in_progress_snapshot(self, snapshot_name: str):
        self.logger.info(
            f"Checking for in-progress snapshot with name: {snapshot_name}"
        )
        result = self.query(snapshot_name=snapshot_name, status=["In Progress"])

        if result["totalSize"] > 0:
            snapshot_id = result["records"][0]["Id"]
            self.logger.info(
                f"Found in-progress snapshot {snapshot_id}, deleting it..."
            )
            self.devhub.OrgSnapshot.delete(snapshot_id)
            self.logger.info(f"Deleted in-progress snapshot: {snapshot_id}")
        else:
            self.logger.info(f"No in-progress snapshot found with name {snapshot_name}")

    def create_org_snapshot(
        self, snapshot_name: str, description: str, source_org: str
    ):
        self.logger.info(f"Creating new org snapshot: {snapshot_name}")
        snapshot_body = {
            "Description": description,
            "SnapshotName": snapshot_name,
            "SourceOrg": source_org,
            "Content": "metadatadata",
        }
        try:
            snapshot_result = self.devhub.OrgSnapshot.create(snapshot_body)
            snapshot_id = snapshot_result["id"]
            self.logger.info(f"Org snapshot {snapshot_id} created.")
            return snapshot_id
        except SalesforceException as e:
            if "NOT_FOUND" in str(e):
                raise ScratchOrgSnapshotError(
                    "Org snapshot feature is not enabled for this Dev Hub or the OrgSnapshot object is not accessible to the user."
                ) from e
            raise

    def poll_for_completion(
        self,
        snapshot_id: str,
        progress,
        task,
        timeout: int = 1200,
        initial_poll_interval: int = 10,
    ):
        poll_interval = initial_poll_interval
        start_time = time.time()
        end_time = start_time + timeout

        while time.time() < end_time:
            try:
                snapshot = self.devhub.OrgSnapshot.get(snapshot_id)
            except SalesforceResourceNotFound as exc:
                raise ScratchOrgSnapshotFailure(
                    "Snapshot not found. This usually happens because another build deleted the snapshot while it was being built."
                ) from exc

            status = snapshot.get("Status")
            progress.update(
                task, description=f"[cyan]Creating snapshot... Status: {status}"
            )

            if status == "Active":
                progress.update(task, completed=100)
                self.logger.info(f"Snapshot {snapshot_id} completed successfully.")
                return snapshot
            if status == "Error":
                progress.update(task, completed=100)
                raise ScratchOrgSnapshotFailure(
                    f"Snapshot {snapshot_id} failed to complete. Error: {snapshot.get('Error')}"
                )

            time.sleep(poll_interval)
            elapsed = time.time() - start_time
            progress.update(task, completed=min(int((elapsed / timeout) * 100), 100))
            poll_interval = min(poll_interval * 1.5, 30)

        raise TimeoutError(
            f"Snapshot {snapshot_id} did not complete within {timeout} seconds."
        )

    def delete_snapshot(self, snapshot_id: str = None):
        snapshot_id = snapshot_id or self.existing_active_snapshot_id
        if snapshot_id:
            self.logger.info(f"Deleting snapshot: {snapshot_id}")
            self.devhub.OrgSnapshot.delete(snapshot_id)
            self.logger.info(f"Deleted snapshot: {snapshot_id}")

    def rename_snapshot(self, snapshot_id: str, new_name: str):
        self.logger.info(f"Renaming snapshot {snapshot_id} to {new_name}")
        update_body = {"SnapshotName": new_name}
        self.devhub.OrgSnapshot.update(snapshot_id, update_body)
        self.logger.info(f"Snapshot {snapshot_id} renamed to {new_name}")

    def update_snapshot_from_org(
        self, base_name: str, description: str, source_org: str, wait: bool = True
    ):
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            console=self.console,
        ) as progress:
            task = progress.add_task("[green]Creating snapshot", total=100)

            # Step 1: Generate temporary name (5% progress)
            progress.update(
                task, advance=5, description="[green]Generating temporary name"
            )
            temp_name = self.generate_temp_name(base_name)

            # Step 2: Check for existing snapshots (10% progress)
            progress.update(
                task, advance=5, description="[green]Checking for existing snapshots"
            )
            self.query_existing_active_snapshot(base_name)
            self.query_and_delete_in_progress_snapshot(temp_name)

            # Step 3: Create new snapshot (10% progress)
            progress.update(
                task, advance=10, description="[green]Creating new snapshot"
            )
            snapshot_id = self.create_org_snapshot(temp_name, description, source_org)

            if not wait:
                snapshot = self.devhub.OrgSnapshot.get(snapshot_id)
                return snapshot

            # Step 4: Wait for snapshot to complete (60% progress)
            progress.update(task, description="[green]Waiting for snapshot to complete")
            snapshot = self.poll_for_completion(snapshot_id, progress, task)

            # Step 5: Finalize snapshot (10% progress)
            progress.update(task, advance=10, description="[green]Finalizing snapshot")
            self.delete_snapshot()  # Deletes the existing active snapshot if it exists
            self.rename_snapshot(snapshot_id, base_name)

        self.console.print(
            Panel(
                f"Snapshot {snapshot_id} created successfully!",
                title="Snapshot Creation",
                border_style="green",
            )
        )
        self.logger.info(f"Snapshot management complete for {snapshot_id}")
        return snapshot

    def finalize_temp_snapshot(
        self, snapshot_name: str, description: str, snapshot_id: str
    ):
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            console=self.console,
        ) as progress:
            task = progress.add_task("[green]Creating snapshot", total=100)

            # Step 1: Check for existing snapshots (10% progress)
            progress.update(
                task,
                advance=10,
                description=f"[green]Checking for existing active snapshot named {snapshot_name}",
            )
            self.query_existing_active_snapshot(snapshot_name)

            # Step 2: Wait for snapshot to complete (60% progress)
            progress.update(task, description="[green]Waiting for snapshot to complete")
            snapshot = self.poll_for_completion(snapshot_id, progress, task)

            # Step 3: Finalize snapshot (30% progress)
            progress.update(task, advance=30, description="[green]Finalizing snapshot")
            self.delete_snapshot()
            self.rename_snapshot(snapshot_id, snapshot_name)
            return snapshot



class BaseCreateScratchOptions(CCIOptions):
    wait: bool = Field(
        True,
        description = (
            "Whether to wait for the snapshot creation to complete. "
            "Defaults to True. If False, the task will return immediately "
            "after creating the snapshot. Use for running in a split "
            "workflow on GitHub. Looks for the GITHUB_OUTPUT environment "
            "variable and outputs SNAPSHOT_ID=<id> to it if found for use "
            "in later steps."
        ),
    )
    snapshot_id: Optional[str] = Field(
        None,
        description = (
            "The ID of the in-progress snapshot to wait for completion. "
            "If set, the task will wait for the snapshot to complete and "
            "update the existing snapshot with the new details. Use for "
            "the second step of a split workflow on GitHub."
        ),
    )
    source_org_id: Optional[str] = Field(
        None,
        description = (
            "The Salesforce Org ID of the source org to create the snapshot from."
            "Must be a valid scratch org for snapshots in the default devhub."
            "Defaults to the org passed to the task or flow."
        ),
    )
    
class HashedValue(BaseModel):
    key: str = Field(
        ...,
        description = "The key of the hashed value.",
    )
    hashed: Optional[str] = Field(
        None,
        description = "The hashed value, usually generated by `cci hash *` commands or task return_values.",
    )

class HashedFlow(HashedValue):
    frozen: Optional[bool] = Field(
        False,
        description = "Whether the flow is frozen. Defaults to False.",
    )
    yaml: Optional[str] = Field(
        None,
        description = "The YAML representation of the flow.",
    )
    yaml_path: Optional[FilePath] = Field(
        None,
        description = "The path to the YAML file containing the flow.",
    )
    
class HashedDependencies(HashedValue):
    source_dependencies: Optional[ProjectDependencies] = Field(
        None,
        description = "The project dependencies used to generate the hash.",
    )
    dependencies: Optional[List[StaticDependency]] = Field(
        None,
        description = "The resolved static dependencies used to generate the hash.",
    )
    
    
class DescriptionHashMixin:
    flows: Optional[List[HashedFlow]] = Field(
        None,
        description = (
            "A dictionary of flow names and their corresponding hash values. "
            "If not provided, hashed values will be generated for all listed flows. "
            "Used to include in the snapshot description."
        ),
    )
    pull_request: Optional[int] = Field(
        None,
        description = (
            "The GitHub pull request number. If set, generated snapshot "
            "names will include the PR number and the snapshot description "
            "will contain pr:<number>."
        ),
    )
    dependencies_hash: Optional[str] = Field(
        None,
        description = (
            "The hash of the project's dependencies deployed to the scratch org "
            "generated by the `update_dependencies` task or the `cci hash dependencies` command."
        ),
    )
    
class GithubCommitStatusMixin:
    create_commit_status: bool = Field(
        False,
        description = (
            "Whether to create a GitHub commit status for the snapshot. "
            "Defaults to False."
        ),
    )
    commit_status_context: str = Field(
        "Snapshot",
        description = (
            "The GitHub commit status context to use for reporting the "
            "snapshot status. If set, the task will create a commit status "
            "with the snapshot status."
        ),
    )
    
class GithubEnvironmentMixin:
    create_environment: bool = Field(
        False,
        description = (
            "Whether to create a GitHub Environment for the snapshot. "
            "Defaults to False."
        ),
    )
    environment_prefix: str = Field(
        "Snapshot-",
        description = (
            "The prefix to use for the GitHub Environment name if create_github_environment is True"
        ),
    )
    
class SnapshotNameMixin:
    snapshot_name: str = Field(
        ...,
        description = (
            "Name of the snapshot to create. Must be a valid snapshot name. "
            "Max 14 characters, alphanumeric, and start with a letter including product code and packaging suffix."
        ),
    )

class NameContextMixin:
    include_product_code: bool = Field(
        True,
        description = (
            "Whether to include the project code as a prefix in the snapshot name. "
            "Defaults to True."
        ),
    )
    include_packaging_suffix: bool = Field(
        True,
        description = (
            "Whether to include a packaging suffix in the snapshot name. "
            "Defaults to True."
        ),
    )
    packaging_suffix: Literal["P","U"] = Field(
        "P",
        description = (
            "The packaging suffix to use in the snapshot name. Defaults to 'P'."
        ),
    )
    


class BaseCreateOrgSnapshot(BaseDevhubTask, BaseSalesforceTask):
    """Base class for tasks that create Scratch Org Snapshots."""

    class Options(BaseCreateScratchOptions):
        pass

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.current_snapshot_id = None
        self.temp_snapshot_name = None
        self.devhub = None
        self.snapshot_id = None
        self.start_time = None

    def _init_task(self):
        self.devhub = self._get_devhub_api()
        self.console = Console()
        self.is_github_job = os.getenv("GITHUB_ACTIONS") == "true"

    def _init_options(self, kwargs):
        super()._init_options(kwargs)
        self.parsed_options.flows = process_list_arg(
            self.parsed_options.get("flows", self.flow.name if self.flow else None)
        )
        self.options["wait"] = process_bool_arg(self.options.get("wait", True))
        self.options["snapshot_id"] = self.options.get("snapshot_id")
        if self.options.get("snapshot_name"):
            self._validate_snapshot_name(self.options["snapshot_name"])
            self._set_temp_snapshot_name(self.options["snapshot_name"])
        else:
            self.options["snapshot_name"] = None
        self.options["source_org_id"] = self.options.get("source_org_id")
        self.options["pull_request"] = self._lookup_pull_request()
        self.options["create_commit_status"] = process_bool_arg(
            self.options.get("create_commit_status", False)
        )
        self.options["create_environment"] = process_bool_arg(
            self.options.get("create_environment", False)
        )
        self.options["commit_status_context"] = self.options.get("commit_status_context")
        self.options["environment_prefix"] = self.options.get("environment_prefix") 
            
        self.description = {
            "pr": None,
            "org": self.org_config.name if self.org_config else None,
            "commit": (
                self.project_config.repo_commit[:7]
                if self.project_config.repo_commit
                else None
            ),
            "branch": self.project_config.repo_branch,
            "flows": ",".join(self.options["flows"]) if self.options["flows"] else None,
        }

    def _run_task(self):
        skip_reason = self._should_create_snapshot()
        snapshot_manager = SnapshotManager(self.devhub, self.logger)
        if skip_reason:
            if skip_reason is not True:
                self.logger.info(f"Skipping snapshot creation: {skip_reason}")
                if self.options.get("snapshot_id"):
                    self.logger.warning(
                        "In-progress snapshot does not meet conditions for finalization. Deleting..."
                    )
                    self.console.print(
                        Panel(
                            f"No snapshot creation required based on current conditions. {self.return_values.get('skip_reason','')}",
                            title="Snapshot Creation",
                            border_style="yellow",
                        )
                    )
            return

        self.logger.info("Starting scratch org snapshot creation")
        snapshot_name = self._generate_snapshot_name()
        description = self._generate_snapshot_description()

        try:
            if self.options["snapshot_id"]:
                snapshot = snapshot_manager.finalize_temp_snapshot(
                    snapshot_name=snapshot_name,
                    description=description,
                    snapshot_id=self.options["snapshot_id"],
                )
            else:
                snapshot = snapshot_manager.update_snapshot_from_org(
                    base_name=snapshot_name,
                    description=description,
                    source_org=self.org_config.org_id,
                    wait=self.options["wait"],
                )
        except ScratchOrgSnapshotError as e:
            self.console.print(
                Panel(
                    f"Failed to create snapshot: {str(e)}",
                    title="Snapshot Creation",
                    border_style="red",
                )
            )
            if self.options["commit_status_context"]:
                self._create_commit_status(snapshot_name, "error")
            raise

        self.return_values["snapshot_id"] = snapshot.get("Id")
        self.return_values["snapshot_name"] = snapshot.get("SnapshotName")
        self.return_values["snapshot_description"] = snapshot.get("Description")
        self.return_values["snapshot_status"] = snapshot.get("Status")

        self._report_result(snapshot)
        set_github_output("SNAPSHOT_ID", snapshot['Id'])

        if self.is_github_job and self.options["create_commit_status"]:
            active = self.return_values["snapshot_status"] == "Active"
            self._create_commit_status(
                snapshot_name=(
                    snapshot_name
                    if active
                    else f"{snapshot_name} ({self.return_values['snapshot_status']})"
                ),
                state="success" if active else "error",
            )
        if self.is_github_job and self.options["github_environment_prefix"]:
            self._create_github_environment(snapshot_name)

    def _should_create_snapshot(self):
        return True

    def _lookup_pull_request(self):
        if self.options.get("pull_request"):
            return self.options["pull_request"]

    def _validate_snapshot_name(self, snapshot_name):
        try:
            SnapshotNameValidator(base_name=snapshot_name)
        except ValueError as e:
            raise ScratchOrgSnapshotError(str(e)) from e

    def _generate_snapshot_name(self, name: Optional[str] = None):
        # Try snapshot_name option
        if not name:
            name = self.options["snapshot_name"]
        # Try branch
        if not name:
            branch = self.project_config.repo_branch
            if branch:
                if branch == self.project_config.project__git__default_branch:
                    name = branch
                elif branch.startswith(
                    self.project_config.project__git__prefix_feature
                ):
                    name = f"f{branch[len(self.project_config.project__git__prefix_feature) :]}"
        # Try commit
        if not name:
            commit = self.project_config.repo_commit
            if commit:
                name = commit[:7]
                
        if not name:
            raise ScratchOrgSnapshotError(
                "Unable to generate snapshot name. Please provide a snapshot name."
            )

        project_code = self.project_config.project_code
        packaged_code = ""
        if self.options["is_packaged"] is True:
            packaged_code = "P"
        elif self.options["is_packaged"] is False:
            packaged_code = "U"
        available_length = 14 - len(packaged_code) - len(project_code)
        name = f"{project_code}{name[:available_length]}{packaged_code}"
        self._validate_snapshot_name(name)
        return name

    def _set_temp_snapshot_name(self, name: str):
        name = f"{name}{self.temp_snapshot_suffix}"
        self._validate_snapshot_name(name)
        self.temp_snapshot_name = name
        return self.temp_snapshot_name

    def _generate_snapshot_description(self, pr_number: Optional[int] = None):
        return (
            " ".join([f"{k}: {v}" for k, v in self.description.items() if v])
        ).strip()[:255]

    def _parse_snapshot_description(self, description: str):
        return dict(item.split(": ") for item in description.split(" ") if ": " in item)

    def _check_snapshot_description(self, description):
        if isinstance(description, str):
            description = self._parse_snapshot_description(description)
        if description != self.description:
            raise ScratchOrgSnapshotFailure(
                f"Snapshot description does not match expected description.\n\n"
                f"Expected: {yaml.dumps(self.description)}\n\nActual: {yaml.dumps(description)}"
            )

    def _create_commit_status(self, snapshot_name, state):
        try:
            description = f"Snapshot: {snapshot_name}"
            self.repo.create_status(
                self.project_config.repo_commit,
                state,
                target_url=os.environ.get("JOB_URL"),
                description=description,
                context=self.options["commit_status_context"],
            )
        except GitHubError as e:
            self.logger.error(f"Failed to create commit status: {str(e)}")
            self.console.print(
                Panel(
                    f"Failed to create commit status: {str(e)}",
                    title="Commit Status",
                    border_style="red",
                )
            )

    def _create_github_environment(self, snapshot_name):
        try:
            environment_name = (
                f"{self.options['github_environment_prefix']}{snapshot_name}"
            )

            # Check if environment already exists
            resp = self.repo._get(f"{self.repo.url}/environments/{environment_name}")
            if resp.status_code == 404:
                self.logger.info(f"Creating new environment: {environment_name}")
                resp = self.repo._put(
                    f"{self.repo.url}/environments/{environment_name}",
                )
                resp.raise_for_status()
                self.logger.info(f"Created new environment: {environment_name}")
            else:
                self.logger.info(f"Environment '{environment_name}' already exists.")

            environment = resp.json()

            self.console.print(
                Panel(
                    f"GitHub Environment '{environment_name}' created/updated successfully!",
                    title="Environment Creation",
                    border_style="green",
                )
            )

        except Exception as e:
            self.logger.error(f"Failed to create/update GitHub Environment: {str(e)}")
            self.console.print(
                Panel(
                    f"Failed to create/update GitHub Environment: {str(e)}",
                    title="Environment Creation",
                    border_style="red",
                )
            )
            raise
        
    def _report_result(self, snapshot, extra: Optional[Dict[str, str]] = None):
        table = Table(title="Snapshot Details", border_style="cyan")
        table.add_column("Field", style="cyan")
        table.add_column("Value", style="magenta")

        for field in [
            "Id",
            "SnapshotName",
            "Status",
            "Description",
            "CreatedDate",
            "ExpirationDate",
        ]:
            value = snapshot.get(field, "N/A")
            if field in ["CreatedDate", "ExpirationDate"]:
                value = self._format_datetime(value)
            table.add_row(field, str(value))

        self.console.print(table)

        # Output to GitHub Actions Job Summary
        summary_file = os.getenv("GITHUB_STEP_SUMMARY")
        if summary_file:
            with open(summary_file, "a") as f:
                f.write(f"## Snapshot Creation Summary\n")
                f.write(f"- **Snapshot ID**: {snapshot.get('Id')}\n")
                f.write(f"- **Snapshot Name**: {snapshot.get('SnapshotName')}\n")
                f.write(f"- **Status**: {snapshot.get('Status')}\n")
                f.write(f"- **Description**: {snapshot.get('Description')}\n")
                f.write(
                    f"- **Created Date**: {self._format_datetime(snapshot.get('CreatedDate'))}\n"
                )
                f.write(
                    f"- **Expiration Date**: {self._format_datetime(snapshot.get('ExpirationDate'))}\n"
                )
                for key, value in (extra or {}).items():
                    f.write(f"- **{key}**: {value}\n")

    def _format_datetime(self, date_string):
        if date_string is None:
            return "N/A"
        dt = parse(date_string)
        return dt.strftime("%Y-%m-%d %H:%M:%S")

    def _format_date(self, date_string):
        if date_string is None:
            return "N/A"
        dt = parse(date_string)
        return dt.strftime("%Y-%m-%d")


class CreateOrgSnapshot(BaseCreateOrgSnapshot):
    salesforce_task = False
    task_docs = """
    Creates a Scratch Org Snapshot using the Dev Hub org.
   
    **Requires** *`target-dev-hub` configured globally or for the project, used as the target Dev Hub org for Scratch Org Snapshots*.
    
    Interacts directly with the OrgSnapshot object in the Salesforce API to fully automate the process of maintaining one active snapshot per snapshot name.
    
    *Snapshot Creation Process*
    
    - **Check for an existing `active` OrgSnapshot** with the same name and recording its ID
    - **Check for an existing `in-progress` OrgSnapshot** with the same name and delete it, maintaining only one in-progress snapshot build
    - **Create a temporary snapshot** under a temporary name with the provided description
    - **Poll for completion** of the snapshot creation process
        - Or pass `--wait False` to return immediately after creating the snapshot setting SNAPSHOT_ID=<id> in GITHUB_OUTPUT and reporting the snapshot details
    
    *On Successful OrgSnapshot Completion*
    
    - Delete the existing snapshot
    - Rename the snapshot to the desired name
    - Report the snapshot details including the ID, status, and expiration date
    """

    temp_snapshot_suffix = "0"

    class Options(BaseCreateScratchOptions, SnapshotNameMixin, NameContextMixin, DescriptionHashMixin):
        pass

    # Peg to API Version 60.0 for OrgSnapshot object
    api_version = "60.0"
    salesforce_task = True

    def _init_options(self, kwargs):
        super()._init_options(kwargs)
        self.temp_snapshot_name = (
            f"{self.options['snapshot_name']}{self.temp_snapshot_suffix}"
        )
        self.options["description"] = self.options.get("description")
        
class GithubCreateOrgSnapshot

class CreateDependenciesSnapshot(BaseCreateScratchOrgSnapshot):
    task_docs = """
    Creates a Scratch Org Snapshot of the frozen dependencies flow, using the hash of the flow in the name
    for cross project dependency lookup by hash.
    """
    task_options = {
        "resolution_strategy": {
            "description": "The resolution strategy to use resolving dynamic dependencies. Defaults to `production`.",
            "required": False,
        },
        "hash": {
            "description": "The hash of the dependencies to use in the snapshot name. Defaults to the hash of the resolved dependencies.",
            "required": True,
        },
        **base_create_scratch_org_snapshot_options,
    }
    
    def _init_options(self, kwargs):
        super()._init_options(kwargs)
        self.options["resolution_strategy"] = self.options.get("resolution_strategy", "production")
        
    def _init_task(self):
        super()._init_task()
        self.resolved, self.resolved_hash = self._get_dependencies()
        self.snapshots = SnapshotManager(self.devhub, self.logger)
        if self._should_create_snapshot() is not True:
            self.options["snapshot_id"] = self.snapshots.existing_active_snapshot_id
        else:
            self.options["snapshot_name"] = self._generate_snapshot_name()
        
    def _generate_snapshot_name(self, name: str | None = None):
        return "DEPS" + self.options["hash"]
    
    def _should_create_snapshot(self):
        self.snapshots.query_existing_active_snapshot(self.options["snapshot_name"])
        if self.snapshots.existing_active_snapshot_id:
            return "Existing active snapshot found"
        return True

class GithubPullRequestSnapshot(BaseGithubTask, BaseDevhubTask):
    task_docs = """
    Creates a Scratch Org Snapshot for a GitHub Pull Request based on build status and conditions.
    
    **Requires** *`target-dev-hub` configured globally or for the project, used as the target Dev Hub org for Scratch Org Snapshots*.
    """

    task_options = {
        "build_success": {
            "description": "Set to True if the build was successful or False for a failure. Defaults to True.",
            "required": True,
        },
        "build_fail_tests": {
            "description": "Whether the build failed due to test failures. Defaults to False",
            "required": True,
        },
        "snapshot_pr": {
            "description": "Whether to create a snapshot for feature branches with PRs",
            "required": False,
        },
        "snapshot_pr_label": {
            "description": "Limit snapshot creation to only PRs with this label",
            "required": False,
        },
        "snapshot_pr_draft": {
            "description": "Whether to create snapshots for draft PRs",
            "required": False,
        },
        "snapshot_fail_pr": {
            "description": "Whether to create snapshots for failed builds on branches with an open PR",
            "required": False,
        },
        "snapshot_fail_pr_label": {
            "description": "Limit failure snapshot creation to only PRs with this label",
            "required": False,
        },
        "snapshot_fail_pr_draft": {
            "description": "Whether to create snapshots for failed draft PR builds",
            "required": False,
        },
        "snapshot_fail_test_only": {
            "description": "Whether to create snapshots only for test failures",
            "required": False,
        },
        **base_create_scratch_org_snapshot_options,
    }
    api_version = "60.0"
    salesforce_task = True
    
    def __init__(self, *args, **kwargs):
        self.pull_request = None
        super().__init__(*args, **kwargs)

    def _init_options(self, kwargs):
        super()._init_options(kwargs)
        self.options["build_success"] = process_bool_arg(
            self.options.get("build_success", True)
        )
        self.options["build_fail_tests"] = process_bool_arg(
            self.options.get("build_fail_tests")
        )
        self.options["commit_status_context"] = self.options.get(
            "commit_status_context"
        )
        self.options["wait"] = process_bool_arg(self.options.get("wait", True))
        self.options["snapshot_id"] = self.options.get("snapshot_id")
        self.options["snapshot_pr"] = process_bool_arg(
            self.options.get("snapshot_pr", True)
        )
        self.options["snapshot_pr_draft"] = process_bool_arg(
            self.options.get("snapshot_pr_draft", False)
        )
        self.options["snapshot_fail_pr"] = process_bool_arg(
            self.options.get("snapshot_fail_pr", True)
        )
        self.options["snapshot_fail_pr_draft"] = process_bool_arg(
            self.options.get("snapshot_fail_pr_draft", False)
        )
        self.options["snapshot_fail_test_only"] = process_bool_arg(
            self.options.get("snapshot_fail_test_only", False)
        )
        self.options["snapshot_pr_label"] = self.options.get("snapshot_pr_label")
        self.options["snapshot_fail_pr_label"] = self.options.get(
            "snapshot_fail_pr_label"
        )

        self.console = Console()

    def _init_task(self):
        super()._init_task()
        self.repo = self.get_repo()
        self.pull_request = self._lookup_pull_request()

    def _lookup_pull_request(self):
        pr = super()._lookup_pull_request()
        if pr:
            res = [self.repo.pull_request(pr)]
        else:
            res = self.repo.pull_requests(
                state = "open",
                head = f"{self.project_config.repo_owner}:{self.project_config.repo_branch}",
            )
        for pr in res:
            self.logger.info(
                f"Checking PR: {pr.number} [{pr.state}] {pr.head.ref} -> {pr.base.ref}"
            )
            if (
                pr.state == "open"
                and pr.head.ref == self.project_config.repo_branch
            ):
                self.logger.info(f"Found PR: {pr.number}")
                return pr

    def _should_create_snapshot(self):
        is_pr = self.pull_request is not None
        self.return_values["has_pr"] = is_pr
        is_draft = self.pull_request.draft if is_pr else False
        self.return_values["pr_is_draft"] = is_draft
        pr_labels = [label["name"] for label in self.pull_request.labels] if is_pr else []
        has_snapshot_label = self.options["snapshot_pr_label"] in pr_labels
        has_snapshot_fail_label = self.options["snapshot_fail_pr_label"] in pr_labels
        self.return_values["pr_has_snapshot_label"] = has_snapshot_label
        self.return_values["pr_has_snapshot_fail_label"] = has_snapshot_fail_label

        if self.options["build_success"] is True:
            if not self.options["snapshot_pr"]:
                self.return_values["skip_reason"] = "snapshot_pr is False"
                return False
            elif not is_pr:
                self.return_values["skip_reason"] = "No pull request on the branch"
                return False
            elif self.options["snapshot_pr_label"] and not has_snapshot_label:
                self.return_values["skip_reason"] = (
                    "Pull request does not have snapshot label"
                )
                return False
            elif is_draft and not self.options["snapshot_pr_draft"]:
                self.return_values["skip_reason"] = (
                    "Pull request is draft and snapshot_pr_draft is False"
                )
                return False
            return True
        else:
            if is_pr:
                return (
                    self.options["snapshot_fail_pr"]
                    and (not is_draft or self.options["snapshot_fail_pr_draft"])
                    and (
                        not self.options["snapshot_fail_pr_label"]
                        or has_snapshot_fail_label
                    )
                    and (
                        not self.options["snapshot_fail_test_only"]
                        or not self.options["build_fail_tests"]
                    )
                )
            else:
                return True

    def _generate_snapshot_name(self, name):
        pr_number = self.pull_request.number if self.pull_request else None
        name = "" 
        name = self.options["snapshot_name"] or name
        if not name:
            name = f"Pr{pr_number}" if pr_number else name
            
        if self.optionsl["build_success"] is False:
            if self.options["build_fail_tests"]:
                name = f"FTest{name}"
            else:
                name = f"Fail{name}"
        return super()._generate_snapshot_name(name)
                
