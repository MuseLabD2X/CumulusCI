import time
from typing import Optional
from cumulusci.core.exceptions import (
    SalesforceException,
    ScratchOrgSnapshotError,
    ScratchOrgSnapshotFailure,
)

from cumulusci.utils.options import (
    Field,
)
from pydantic import BaseModel, validator
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
