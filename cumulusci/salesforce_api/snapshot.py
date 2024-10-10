import os
import time
from typing import List, Optional, Union
from datetime import datetime, timedelta
from pydantic import BaseModel, validator
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.markup import escape
from rich.panel import Panel
from rich.progress import (
    Progress,
    SpinnerColumn,
    BarColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table
from rich.text import Text
from simple_salesforce.exceptions import SalesforceResourceNotFound
from cumulusci.core.exceptions import (
    SalesforceException,
    ScratchOrgSnapshotError,
    ScratchOrgSnapshotFailure,
)
from cumulusci.utils.options import Field

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
    base_name: str = Field(..., max_length=15)

    @classmethod
    @validator("base_name")
    def validate_name(cls, name):
        if not name.isalnum():
            raise ValueError("Snapshot name must only contain alphanumeric characters")
        if name[0].isdigit():
            raise ValueError("Snapshot name must start with a letter")
        return name


class SnapshotManager:
    def __init__(self, devhub, logger, temp_suffix: str = "X"):
        self.devhub = devhub
        self.logger = logger
        self.temp_suffix = temp_suffix
        self.existing_active_snapshot: dict | None = None
        self.temporary_snapshot_name = None

    def generate_temp_name(self, base_name: str, max_length: int = 15) -> str:
        temp_name = f"{base_name}{self.temp_suffix}"
        if len(temp_name) > max_length:
            raise ValueError(
                "Base name is too long to generate a temporary snapshot name"
            )
        self.logger.info(f"Generated temporary snapshot name: {temp_name}")
        return temp_name

    def query(
        self,
        snapshot_id: Optional[str] = None,
        snapshot_ids: Optional[List[str]] = None,
        snapshot_name: Optional[str] = None,
        snapshot_names: Optional[List[str]] = None,
        description: Optional[str] = None,
        status: Optional[Union[str, List[str]]] = None,
        limit: Optional[int] = None,
    ):
        query = f"SELECT {', '.join(ORG_SNAPSHOT_FIELDS)} FROM OrgSnapshot"
        where_clauses = []
        params = {}

        if snapshot_id:
            where_clauses.append("Id = :snapshot_id")
            params["snapshot_id"] = snapshot_id

        if snapshot_ids:
            where_clauses.append("Id IN :snapshot_ids")
            params["snapshot_ids"] = tuple(snapshot_ids)

        if snapshot_name:
            where_clauses.append("SnapshotName = :snapshot_name")
            params["snapshot_name"] = snapshot_name

        if snapshot_names:
            where_clauses.append("SnapshotName IN :snapshot_names")
            params["snapshot_names"] = tuple(snapshot_names)

        if description:
            where_clauses.append("Description LIKE :description")
            params["description"] = f"%{description}%"

        if status:
            if isinstance(status, str):
                status = [status]
            where_clauses.append("Status IN :status")
            params["status"] = tuple(status)

        self.logger.info(f"Querying snapshots with filters: {where_clauses}")

        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)

        if limit:
            query += " LIMIT :limit"
            params["limit"] = limit

        return self.devhub.query_all(query, **params)

    def query_existing_active_snapshot(self, snapshot_name: str):
        result = self.query(snapshot_name=snapshot_name, status=["Active"])
        if result["totalSize"] > 0:
            self.existing_active_snapshot = result["records"][0]
            self.logger.info(
                f"Found existing active snapshot: {self.existing_active_snapshot['Id']}"
            )
        else:
            self.logger.info(f"No active snapshot found with name {snapshot_name}")

    def query_and_delete_in_progress_snapshot(self, snapshot_name: str):
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

    def delete_snapshot(self, snapshot_id: str = None):
        if not snapshot_id and self.existing_active_snapshot:
            snapshot_id = self.existing_active_snapshot["Id"]
        if snapshot_id:
            self.logger.info(f"Deleting snapshot: {snapshot_id}")
            self.devhub.OrgSnapshot.delete(snapshot_id)
            self.logger.info(f"Deleted snapshot: {snapshot_id}")

    def rename_snapshot(self, snapshot_id: str, new_name: str):
        self.logger.info(f"Renaming snapshot {snapshot_id} to {new_name}")
        update_body = {"SnapshotName": new_name}
        self.devhub.OrgSnapshot.update(snapshot_id, update_body)
        self.logger.info(f"Snapshot {snapshot_id} renamed to {new_name}")


class SnapshotUX:
    def __init__(self, snapshot_manager):
        self.snapshots = snapshot_manager
        self.console = Console()
        self.layout = Layout()
        self.log_panel = Panel("", title="Log Output", border_style="cyan", expand=True)
        self.progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
        )
        self.overall_progress = self.progress.add_task(
            "[cyan]Overall Progress", total=100
        )
        self.snapshot_info_panel = Panel(
            "", title="Snapshot Info", border_style="yellow"
        )
        self.setup_layout()
        self.log_messages = []
        self.estimated_completion_time = None
        self.is_ci_environment = self.detect_ci_environment()

    def detect_ci_environment(self):
        # Check for common CI environment variables
        return any(
            env_var in os.environ
            for env_var in ["CI", "GITHUB_ACTIONS", "GITLAB_CI", "JENKINS_URL"]
        )

    def setup_layout(self):
        self.layout.split_column(
            Layout(
                Panel(self.progress, title="Snapshot Progress", border_style="green"),
                name="progress",
                ratio=1,
            ),
            Layout(self.snapshot_info_panel, name="info", ratio=1),
            Layout(self.log_panel, name="log", ratio=2),
        )

    def log(self, message, style: str = ""):
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_messages.append(message)

        # Use Text object with markup enabled to render the log panel
        log_text = Text("\n".join(self.log_messages[-15:]))
        log_text.highlight_words(
            [
                timestamp
                for entry in self.log_messages[-15:]
                for timestamp in [entry.split()[0]]
            ],
            "dim",
        )
        log_text.highlight_words(["ERROR", "Error", "Failed", "FAILURE"], "bold red")
        log_text.highlight_words(["WARNING", "Warning"], "bold yellow")
        log_text.highlight_words(
            ["Complete", "complete", "Press any key to continue..."], "bold green"
        )
        self.log_panel.renderable = log_text

        if self.is_ci_environment:
            self.console.print(
                log_entry, markup=True
            )  # Print for CI environments with markup

    def update_snapshot_info(self, snapshot):
        if snapshot:
            table = Table(show_header=False, expand=True)
            table.add_column("Property", style="cyan")
            table.add_column("Value", style="yellow")
            for field in [
                "Id",
                "SnapshotName",
                "Status",
                "SourceOrg",
                "CreatedDate",
                "LastModifiedDate",
            ]:
                table.add_row(field, str(snapshot.get(field, "N/A")))
            self.snapshot_info_panel.renderable = table
        else:
            self.snapshot_info_panel.renderable = "No snapshot data available"

    def create_snapshot(
        self,
        base_name: str,
        description: str,
        source_org: str,
        wait: bool = True,
        update_existing: bool = True,
    ):
        with Live(
            self.layout,
            console=self.console,
            screen=not self.is_ci_environment,
            refresh_per_second=4,
        ) as live:
            self.log(f"Starting snapshot creation for {base_name}")
            self.estimate_completion_time()

            # Check for existing snapshots
            self.progress.update(
                self.overall_progress,
                advance=10,
                description="Checking existing snapshots",
            )
            self.snapshots.query_existing_active_snapshot(base_name)
            if self.snapshots.existing_active_snapshot:
                self.log(
                    f"Found existing active snapshot: {self.snapshots.existing_active_snapshot['Id']}"
                )
                self.update_snapshot_info(self.snapshots.existing_active_snapshot)
            else:
                self.log("No existing active snapshot found")

            temp_name = self.snapshots.generate_temp_name(base_name)
            self.log(f"Generated temporary name: {temp_name}")
            self.snapshots.query_and_delete_in_progress_snapshot(temp_name)
            self.log("Existing snapshot check complete")

            # Create new snapshot
            self.progress.update(
                self.overall_progress, advance=10, description="Creating new snapshot"
            )
            snapshot_id = self.snapshots.create_org_snapshot(
                temp_name, description, source_org
            )
            self.log(f"New snapshot created with ID: {snapshot_id}")

            if not wait:
                self.log("Asynchronous operation requested. Returning snapshot ID.")
                return self.snapshots.devhub.OrgSnapshot.get(snapshot_id)

            # Poll for completion
            self.progress.update(
                self.overall_progress, description="Waiting for snapshot to complete"
            )
            snapshot = self.poll_for_completion(snapshot_id, live)

            # Finalize snapshot
            if snapshot["Status"] == "Active":
                self.make_snapshot_active(
                    snapshot_id=snapshot_id,
                    snapshot_name=base_name,
                    update_existing=update_existing,
                )

            self.progress.update(
                self.overall_progress, advance=10, description="Complete"
            )
            self.log("Snapshot creation complete!")

            # Add a pause here
            self.pause_display(live)
            if not self.is_ci_environment:
                self.snapshots.logger.info("Snapshot Creation Log:")
                self.snapshots.logger.info("  " + "\n  ".join(self.log_messages))
                self.snapshots.logger.info("Snapshot creation complete!")

        return snapshot

    def make_snapshot_active(
        self, snapshot_id: str, snapshot_name: str, update_existing: bool = True
    ):
        # Finalize snapshot creation by deleting the current active snapshot and renaming the new snapshot
        self.progress.update(
            self.overall_progress, advance=10, description="Finalizing snapshot"
        )
        if update_existing:
            if self.snapshots.existing_active_snapshot:
                if (
                    self.snapshots.existing_active_snapshot["SnapshotName"]
                    != snapshot_name
                ):
                    self.snapshots.rename_snapshot(snapshot_id, snapshot_name)
                    self.log(
                        f"Snapshot {snapshot_id} finalized and renamed to {snapshot_name}"
                    )
                elif self.snapshots.existing_active_snapshot["Id"] == snapshot_id:
                    self.log("Snapshot is already active, skipping rename")
                else:
                    self.snapshots.delete_snapshot(
                        self.snapshots.existing_active_snapshot["Id"]
                    )
                    self.log(
                        f"Deleted existing active snapshot with ID {self.snapshots.existing_active_snapshot['Id']}"
                    )
                    self.snapshots.rename_snapshot(snapshot_id, snapshot_name)
                    self.log(
                        f"Snapshot {snapshot_id} finalized and renamed to {snapshot_name}"
                    )

    def pause_display(self, live, timeout=30):
        if self.is_ci_environment:
            return  # Don't pause in CI environment

        self.log(
            f"Process completed. Display will timeout in {timeout} seconds. Press any key to continue...",
            style="bold green",
        )

        start_time = time.time()
        while time.time() - start_time < timeout:
            if self.key_pressed():
                break
            time.sleep(0.1)
            live.refresh()

    def key_pressed(self):
        # This is a simple cross-platform way to check for a keypress
        if os.name == "nt":  # For Windows
            import msvcrt

            return msvcrt.kbhit()
        else:  # For Unix-based systems
            import select
            import sys

            return select.select([sys.stdin], [], [], 0) == ([sys.stdin], [], [])

    def poll_for_completion(
        self,
        snapshot_id: str,
        live,
        timeout: int = 1200,
        initial_poll_interval: int = 10,
    ):
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=timeout)
        poll_interval = initial_poll_interval

        while datetime.now() < end_time:
            try:
                snapshot = self.snapshots.devhub.OrgSnapshot.get(snapshot_id)
                self.update_snapshot_info(snapshot)
            except Exception as e:
                self.log(f"Error retrieving snapshot: {str(e)}")
                raise

            status = snapshot.get("Status")
            self.log(f"Current status: {status}")

            if status == "Active":
                self.progress.update(
                    self.overall_progress, advance=60, description="Snapshot complete"
                )
                return snapshot
            elif status == "Error":
                error_msg = f"Snapshot failed: {snapshot.get('Error')}"
                self.log(f"Error: {error_msg}")
                raise Exception(error_msg)

            elapsed_time = (datetime.now() - start_time).total_seconds()
            if self.estimated_completion_time:
                progress = min(
                    60,
                    int(
                        60
                        * elapsed_time
                        / (self.estimated_completion_time - start_time).total_seconds()
                    ),
                )
                self.progress.update(self.overall_progress, completed=20 + progress)

            live.refresh()
            time.sleep(poll_interval)
            poll_interval = min(poll_interval * 1.5, 30)

        raise TimeoutError(
            f"Snapshot {snapshot_id} did not complete within {timeout} seconds."
        )

    def estimate_completion_time(self):
        recent_snapshots = self.snapshots.query(status=["Active"], limit=5)
        if recent_snapshots["totalSize"] > 0:
            total_time = sum(
                (
                    datetime.strptime(s["LastModifiedDate"], "%Y-%m-%dT%H:%M:%S.%f%z")
                    - datetime.strptime(s["CreatedDate"], "%Y-%m-%dT%H:%M:%S.%f%z")
                ).total_seconds()
                for s in recent_snapshots["records"]
            )
            avg_time = total_time / recent_snapshots["totalSize"]
            self.estimated_completion_time = datetime.now() + timedelta(
                seconds=avg_time
            )
            self.log(
                f"Estimated completion time: {self.estimated_completion_time.strftime('%H:%M:%S')}"
            )
        else:
            self.log("Unable to estimate completion time")

    def finalize_temp_snapshot(
        self,
        snapshot_name: str,
        description: str,
        snapshot_id: str,
        update_existing: bool = True,
    ):
        with Live(
            self.layout, console=self.console, screen=True, refresh_per_second=4
        ) as live:
            self.log(f"Finalizing temporary snapshot {snapshot_id} to {snapshot_name}")

            # Check for existing snapshots
            self.progress.update(
                self.overall_progress,
                advance=10,
                description="Checking existing snapshots",
            )
            self.snapshots.existing_active_snapshot = (
                self.snapshots.devhub.OrgSnapshot.get(snapshot_id)
            )
            self.log("Existing snapshot check complete")

            # Poll for completion
            self.progress.update(
                self.overall_progress, description="Waiting for snapshot to complete"
            )
            snapshot = self.poll_for_completion(snapshot_id, live)

            # Finalize snapshot
            self.progress.update(
                self.overall_progress, advance=30, description="Finalizing snapshot"
            )
            # Finalize snapshot
            if snapshot["Status"] == "Active":
                self.make_snapshot_active(
                    snapshot_id=snapshot_id,
                    snapshot_name=snapshot_name,
                    update_existing=update_existing,
                )

            self.progress.update(self.overall_progress, description="Complete")
            self.log("Snapshot finalization complete!")

            # Add a pause here
            self.pause_display(live)
        return snapshot

    # Passthrough properties and methods
    @property
    def existing_active_snapshot(self):
        return self.snapshots.existing_active_snapshot

    def query(self, *args, **kwargs):
        return self.snapshots.query(*args, **kwargs)

    def query_existing_active_snapshot(self, *args, **kwargs):
        return self.snapshots.query_existing_active_snapshot(*args, **kwargs)

    def delete_snapshot(self, *args, **kwargs):
        return self.snapshots.delete_snapshot(*args, **kwargs)
