import json
import os
import yaml
from collections import defaultdict
from datetime import datetime
from logging import getLogger
from pathlib import Path

from cumulusci.cli.runtime import click

from cumulusci.core.github import set_github_output
from cumulusci.core.exceptions import FlowNotFoundError
from cumulusci.core.utils import format_duration
from cumulusci.utils import document_flow, flow_ref_title_and_intro
from cumulusci.utils.hashing import hash_obj
from cumulusci.utils.serialization import json_dumps
from cumulusci.utils.yaml.render import yaml_dump
from cumulusci.utils.yaml.safer_loader import load_yaml_data

from .runtime import pass_runtime
from .ui import CliTable
from .utils import group_items

from rich.box import Box
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Span, Text
from rich.tree import Tree
from rich.logging import RichHandler


def format_deploy_hash(deploy):
    if not deploy.hash:
        return "No hash"
    if len(deploy.hash) < 16:
        return deploy.hash
    return f"{deploy.hash[:8]}...{deploy.hash[-8:]}"


def format_deploys(deploys):
    formatted_deploys = []
    if deploys.repo:
        for deploy in deploys.repo:
            formatted_deploys.append(
                {
                    "type": "repo",
                    "path": deploy.path,
                    "size": deploy.size,
                    "hash": format_deploy_hash(deploy),
                }
            )
    if deploys.github:
        for deploy in deploys.github:
            formatted_deploys.append(
                {
                    "type": "github",
                    "repo": deploy.repo,
                    "commit": deploy.commit[:7],
                    "subfolder": deploy.subfolder,
                    "size": deploy.size,
                    "hash": format_deploy_hash(deploy),
                }
            )
    if deploys.zip_url:
        for deploy in deploys.zip_url:
            formatted_deploys.append(
                {
                    "type": "zip_url",
                    "url": deploy.url,
                    "subfolder": deploy.subfolder,
                    "size": deploy.size,
                    "hash": format_deploy_hash(deploy),
                }
            )
    return formatted_deploys


def report_predictions(flow_name, predictions, org_config, return_tree: bool = False):
    if not return_tree:
        console = Console()

    flow_tracker = {
        "deploys": [],
        "package_installs": [],
        "transforms": [],
    }
    flow_tracker_hash = None
    flow_snapshot_hash = None

    flow_tree = Tree(
        f"[bold green]Flow Predictions for {flow_name} on org {org_config.name}"
    )

    snapshot_hashes = org_config.history.get_snapshot_hash(return_data=True)
    starting_snapshot_hash = hash_obj(snapshot_hashes) if snapshot_hashes else None

    for step in predictions:
        hash_content_tree = None
        hash_content = None
        if step.tracker:
            hash_content = step.tracker.get_org_tracker_hash(return_data=True)

            if hash_content:
                # Update the flow tracker and hashes
                flow_tracker["deploys"].extend(hash_content.get("deploys", []))
                flow_tracker["package_installs"].extend(
                    hash_content.get("package_installs", [])
                )
                flow_tracker["transforms"].extend(hash_content.get("transforms", []))
                flow_tracker_hash = hash_obj(flow_tracker)
                flow_snapshot_hash = hash_obj(
                    {
                        "org_shape": snapshot_hashes["org_shape"],
                        "tracker": flow_tracker,
                    }
                )
            else:
                step_tree = flow_tree.add(f"[bold]{step.path}:[/bold]")

        if step.skip:
            step_tree = flow_tree.add(
                f"([bold grey53]{flow_snapshot_hash}) {step.path} (skipped)"
            )
            flow_tree.add(step_tree)
            continue

        step_tree = flow_tree.add(
            f"[bold]([green]{flow_snapshot_hash}[/green]) {step.path}:[/bold] {step.task_config.get('name') or step.task_name or step.task_class.__name__}"
        )

        # Report the step's deploys
        if step.tracker and step.tracker.deploys.has_deploys:
            deploys_tree = step_tree.add("[bold green]Deploys")

            # Deploys from the repo
            if step.tracker.deploys.repo:
                deploy_repo_subtree = deploys_tree.add(f"[italic]From repo")
                for deploy in step.tracker.deploys.repo:
                    deploy_repo_subtree.add(
                        f"[cyan][bold]{deploy.path}[/bold][/cyan]\n"
                        f"Size: [bold]{deploy.size}[/bold] bytes\n"
                        f"Hash: [bold]{format_deploy_hash(deploy)}[/bold]\n"
                    )

            # Deploys from external GitHub repos
            if step.tracker.deploys.github:
                deploy_github_subtree = deploys_tree.add(
                    f"[italic]From external GitHub repo"
                )
                for deploy in step.tracker.deploys.github:
                    subfolder = (
                        f"Subfolder: [bold][blue]{deploy.subfolder}[/blue][/bold]\n"
                        if deploy.subfolder
                        else ""
                    )
                    deploy_github_subtree.add(
                        f"[cyan][bold]{deploy.repo.replace('https://github.com/', 'gh:')}[/bold]@{deploy.commit[:7]}[/cyan]\n"
                        f"{subfolder}"
                        f"Hash: [bold]{format_deploy_hash(deploy)}[/bold]\n"
                        f"Size: [bold]{deploy.size}[/bold] bytes\n"
                    )

            # Deploys from external zip URLs
            if step.tracker.deploys.zip_url:
                deploy_zip_url_subtree = deploys_tree.add(
                    f"[italic]From external zip URL"
                )
                subfolder = (
                    f"Subfolder: [bold][blue]{deploy.subfolder}[/blue][/bold]\n"
                    if deploy.subfolder
                    else ""
                )
                for deploy in step.tracker.deploys.zip_url:
                    deploy_zip_url_subtree.add(
                        f"[cyan][bold]{deploy.url}[/bold][/cyan]\n"
                        f"{subfolder}"
                        f"Hash: [bold]{format_deploy_hash(deploy)}[/bold]\n"
                        f"Size: [bold]{deploy.size}[/bold] bytes\n"
                    )

        # Report the step's package installs
        if step.tracker and step.tracker.package_installs:
            installs_tree = step_tree.add("[bold green]Package Installs")
            for package_install in step.tracker.package_installs:
                package_name = []
                if package_install.namespace:
                    package_name.append(f"({package_install.namespace})")
                if package_install.name:
                    package_name.append(package_install.name)
                if package_install.package_id:
                    package_name.append(f":{package_install.package_id}")
                package_name = " ".join(package_name)
                installs_tree.add(
                    f"{package_name}@[bold]{package_install.version or package_install.version_id}[/bold]"
                )

        # Report the step's hashes
        if step.tracker and hash_content:
            hash_content_tree = step_tree.add(
                "[green][bold]Tracker Hash Contents[/bold][/green]"
            )
            if hash_content["deploys"]:
                hash_deploys_subtree = hash_content_tree.add(
                    f"[green][bold]Deploys[/bold][/green]",
                )
                for deploy in hash_content["deploys"]:
                    hash_deploys_subtree.add(deploy)
            if hash_content["package_installs"]:
                hash_package_installs_subtree = hash_content_tree.add(
                    f"[green][bold]Package Installs[/bold][/green]"
                )
                for package_install in hash_content["package_installs"]:
                    hash_package_installs_subtree.add(package_install)
            if hash_content["transforms"]:
                hash_transforms_subtree = hash_content_tree.add(
                    f"[green][bold]Transforms[/bold][/green]"
                )
                for transform in hash_content["transforms"]:
                    hash_transforms_subtree.add(transform)
            step_hashes_content = (
                f"Step Tracker Hash:       [green][bold]{hash_obj(hash_content)}[/bold][/green]\n"
                f"Flow Step Tracker Hash:  [green][bold]{flow_tracker_hash}[/bold][/green]\n"
                f"Flow Step Snapshot Hash: [green][bold]{flow_snapshot_hash}[/bold][/green]"
            )
            step_tree.add(Panel(step_hashes_content, title="Step Hashes"))
        else:
            step_tree.add(
                Panel(
                    "No tracker hash content", title="Step Hashes", style="bold grey53"
                )
            )

    if return_tree:
        return flow_tree

    console.print()
    console.print(flow_tree)

    org_hashes = (
        f"Org Shape:     [green][bold]{snapshot_hashes['org_shape']}\n[/bold][/green]"
        f"Flow Tracker:  [green][bold]{flow_tracker_hash}[/bold][/green]\n"
        f"Flow Snapshot: [green][bold]{flow_snapshot_hash}[/bold][/green]"
    )
    if starting_snapshot_hash:
        f"Pre-Flow Org Tracker:   [bold]{starting_snapshot_hash}[/bold]\n"
        "  [yellow][bold]NOTE:[/bold][italic] The target org's history shows changes made by the flow[/italic][/yellow]\n"
    console.print(
        Panel(
            org_hashes,
            title=f"Predicted Flow Hashes for {flow_name}",
            style="bold green",
        )
    )
    console.print(Text("End of Flow Prediction", style="bold"))


@click.group("flow", help="Commands for finding and running flows for a project")
def flow():
    pass


@flow.command(name="doc", help="Exports RST format documentation for all flows")
@click.option(
    "--project", "project", is_flag=True, help="Include project-specific flows only"
)
@click.option(
    "--load-yml",
    help="If set, loads the specified yml file into the the project config as additional config",
)
@pass_runtime(require_project=False, require_keychain=True)
def flow_doc(runtime, project=False, load_yml=None):
    flow_info_path = Path(__file__, "..", "..", "..", "docs", "flows.yml").resolve()
    with open(flow_info_path, "r", encoding="utf-8") as f:
        flow_info = load_yaml_data(f)
    click.echo(flow_ref_title_and_intro(flow_info["intro_blurb"]))
    flow_info_groups = list(flow_info["groups"].keys())

    universal_flows = runtime.universal_config.list_flows()
    if project:
        flows = [
            flow
            for flow in runtime.project_config.list_flows()
            if flow not in universal_flows
        ]
    else:
        flows = universal_flows
    flows_by_group = group_items(flows)
    flow_groups = sorted(
        flows_by_group.keys(),
        key=lambda group: (
            flow_info_groups.index(group) if group in flow_info_groups else 100
        ),
    )

    for group in flow_groups:
        click.echo(f"{group}\n{'-' * len(group)}")
        if group in flow_info["groups"]:
            click.echo(flow_info["groups"][group]["description"])

        for flow in sorted(flows_by_group[group]):
            flow_name, flow_description = flow
            try:
                flow_coordinator = runtime.get_flow(flow_name)
            except FlowNotFoundError as e:
                raise click.UsageError(str(e))

            additional_info = None
            if flow_name in flow_info.get("flows", {}):
                additional_info = flow_info["flows"][flow_name]["rst_text"]

            click.echo(
                document_flow(
                    flow_name,
                    flow_description,
                    flow_coordinator,
                    additional_info=additional_info,
                )
            )
            click.echo("")


@flow.command(name="list", help="List available flows for the current context")
@click.option("--plain", is_flag=True, help="Print the table using plain ascii.")
@click.option("--json", "print_json", is_flag=True, help="Print a json string")
@click.option(
    "--load-yml",
    help="If set, loads the specified yml file into the the project config as additional config",
)
@pass_runtime(require_project=False)
def flow_list(runtime, plain, print_json, load_yml=None):
    plain = plain or runtime.universal_config.cli__plain_output
    flows = runtime.get_available_flows()
    if print_json:
        click.echo(json.dumps(flows))
        return None

    flow_groups = group_items(flows)
    for group, flows in flow_groups.items():
        data = [["Flow", "Description"]]
        data.extend(sorted(flows))
        table = CliTable(
            data,
            group,
        )
        table.echo(plain)

    click.echo(
        "Use "
        + click.style("cci flow info <flow_name>", bold=True)
        + " to get more information about a flow."
    )


@flow.command(name="info", help="Displays information for a flow")
@click.argument("flow_name")
@click.option(
    "--skip", help="Specify a comma separated list of task and flow names to skip."
)
@click.option(
    "--skip-from",
    help="Specify a task or flow name to skip and all steps that follow it.",
)
@click.option(
    "--start-from",
    help="Specify a task or flow name to start from. All prior steps will be skippped.",
)
@click.option(
    "--load-yml",
    help="If set, loads the specified yml file into the the project config as additional config",
)
@pass_runtime(require_keychain=True)
def flow_info(
    runtime,
    flow_name,
    skip=None,
    skip_from=None,
    start_from=None,
    load_yml=None,
):
    if skip:
        skip = skip.split(",")

    try:
        coordinator = runtime.get_flow(
            flow_name,
            skip=skip,
            skip_from=skip_from,
            start_from=start_from,
        )
        output = coordinator.get_summary(verbose=True)
        click.echo(output)
    except FlowNotFoundError as e:
        raise click.UsageError(str(e))


@flow.command(name="run", help="Runs a flow")
@click.argument("flow_name")
@click.option(
    "--org",
    help="Specify the target org.  By default, runs against the current default org",
)
@click.option(
    "--delete-org",
    is_flag=True,
    help="If set, deletes the scratch org after the flow completes",
)
@click.option(
    "--debug", is_flag=True, help="Drops into pdb, the Python debugger, on an exception"
)
@click.option(
    "-o",
    nargs=2,
    multiple=True,
    help="Pass task specific options for the task as '-o taskname__option value'.  You can specify more than one option by using -o more than once.",
)
@click.option(
    "--no-prompt",
    is_flag=True,
    help="Disables all prompts.  Set for non-interactive mode use such as calling from scripts or CI systems",
)
@click.option(
    "--predict",
    is_flag=True,
    help="Predict the changes that will be made by the flow without making them",
)
@click.option(
    "--use-snapshots",
    is_flag=True,
    help="Override the flow and org's `use_snapshots` option. If True, first run the flow in predict mode to get the predicted snapshots, then look for an active snapshot to start from, and finally run the rest of the flow.",
)
@click.option(
    "--rich",
    is_flag=True,
    help="Use the rich library for output formatting",
)
@click.option(
    "--skip", help="Specify a comma separated list of task and flow names to skip."
)
@click.option(
    "--skip-from",
    help="Specify a task or flow name to skip and all steps that follow it.",
)
@click.option(
    "--start-from",
    help="Specify a task or flow name to start from. All prior steps will be skippped.",
)
@click.option(
    "--load-yml",
    help="If set, loads the specified yml file into the the project config as additional config",
)
@pass_runtime(require_keychain=True)
def flow_run(
    runtime,
    flow_name,
    org,
    delete_org,
    debug,
    o,
    no_prompt,
    predict=None,
    use_snapshots=None,
    rich=None,
    skip=None,
    skip_from=None,
    start_from=None,
    load_yml=None,
):
    if skip:
        skip = skip.split(",")

    console = Console()

    # Get necessary configs
    org, org_config = runtime.get_org(org, check_expired=use_snapshots or predict)
    if delete_org and not org_config.scratch:
        raise click.UsageError("--delete-org can only be used with a scratch org")

    # Parse command line options
    options = defaultdict(dict)
    if o:
        for key, value in o:
            if "__" in key:
                task_name, option_name = key.split("__")
                options[task_name][option_name] = value
            else:
                raise click.UsageError(
                    "-o option for flows should contain __ to split task name from option name."
                )

    if use_snapshots:
        intro = [
            "The flow will first be run in predict mode to predict hashes of all steps that make changes to the org.\n",
            "[bold]Then:[/bold]\n",
            "  - Query the default DevHub for active snapshots with matching hashes",
            "  - [green][b]If a snapshot is found[/b][/green], start the org from that snapshot",
            "    - Start the flow from after the steps matching the snapshot's hash",
            "  - [bold]If no snapshot is found[/bold], run the flow from the beginning\n",
        ]
        console.print(
            Panel(
                "\n".join(intro),
                title="Flow Run with Snapshots",
                border_style="magenta bold",
            )
        )

    # Create the flow and handle initialization exceptions
    coordinator = None
    try:
        get_flow_args = {}
        if use_snapshots:
            logger = getLogger("cumulusci.flow")
            logger.handlers.clear()
            logger.addHandler(
                RichHandler(
                    console=console,
                    show_level=False,
                    show_path=False,
                )
            )
            get_flow_args["logger"] = logger
        coordinator = runtime.get_flow(
            flow_name,
            options=options,
            skip=skip,
            skip_from=skip_from,
            start_from=start_from,
            **get_flow_args,
        )
        if use_snapshots:
            console.print(
                Panel(
                    (
                        f"Running flow {flow_name} in predict-only mode without an org\n\n"
                        "Using snapshot predictions to find matching snapshots for flow layers.\n"
                        "The flow will first be run in predict mode to calculate hashes, the query\n"
                        "the DevHub to look for active scratch org snapshots to start the org from."
                    )
                    title="Predicting Flow Hashes",
                    border_style="bold magenta",
                )
            )

        continue_from_path = None
        start_time = datetime.now()
        action = "Ran"
        if predict or use_snapshots:
            predictions = coordinator.predict(org_config)
            if not use_snapshots:
                report_predictions(flow_name, predictions, org_config)
            else:
                predictions_tree = report_predictions(
                    flow_name, predictions, org_config, return_tree=True
                )
                console.print(
                    Panel(
                        predictions_tree,
                        title="Flow Predictions",
                        border_style="bold magenta",
                    )
                )
            action = "Predicted"
        if use_snapshots:
            console.print(
                Panel(
                    "Accessing the default DevHub to look for active snapshots matching predicted hashes",
                    title="Looking for Active Snapshots",
                    border_style="bold magenta",
                )
            )
            org_config = coordinator.predict_snapshot(org_config, predictions)
            if not org_config.use_snapshot_hashes:
                console.print(
                    Panel(
                        f"No active snapshots found. Running the flow from the beginning is a normal {org} scratch org.",
                        title="No Snapshots Found",
                        border_style="bold magenta",
                    )
                )
            else:
                matches = [
                    "Found the following matching snapshots in step order:\n",
                ]
                for snapshot in org_config.snapshot_hashes:
                    matches.append(
                        f"- [b]{snapshot['path']} ([green]{snapshot['snapshot_hash']}[/green])[/b]"
                    )
                    matches.append(
                        f"  SnapshotName: [green][b]{snapshot['snapshot']['SnapshotName']}[/b][/green]",
                    )
                    matches.append(f"  Created: {snapshot['snapshot']['CreatedDate']}")
                    matches.append(
                        f"  Description: {snapshot['snapshot']['Description']}\n"
                    )

                continue_from_path = snapshot["path"]
                matches.append("")
                matches.append(
                    f"[green][b]Starting from {snapshot['path']}...[/b][/green]"
                )
                console.print(
                    Panel(
                        "\n".join(matches),
                        title="Found Snapshots",
                        border_style="bold magenta",
                    )
                )

        if not predict:
            action = "Ran"
            coordinator.run(org_config, continue_from_path=continue_from_path)
        duration = datetime.now() - start_time
        click.echo(f"{action} {flow_name} in {format_duration(duration)}")
        if not predict:
            console.print(
                Panel(
                    f"Flow {flow_name} completed successfully in {format_duration(duration)}",
                    title="Flow Complete",
                    border_style="bold green",
                )
            )
            org_config.add_action_to_history(coordinator.action)
    except Exception as e:
        if coordinator and coordinator.action:
            org_config.add_action_to_history(coordinator.action)
        runtime.alert(f"Flow error: {flow_name}")
        raise
    finally:
        # Delete the scratch org if --delete-org was set
        if delete_org:
            try:
                org_config.delete_org()
            except Exception as e:
                click.echo(
                    "Scratch org deletion failed.  Ignoring the error below to complete the flow:"
                )
                click.echo(str(e))

    runtime.alert(f"Flow Complete: {flow_name}")


@flow.command(name="freeze", help="Freeze a flow into a flattened list of static steps")
@click.argument("flow_name")
@click.option(
    "--org",
    help="Specify the target org.  By default, runs against the current default org",
)
@click.option(
    "--debug", is_flag=True, help="Drops into pdb, the Python debugger, on an exception"
)
@click.option(
    "-o",
    nargs=2,
    multiple=True,
    help="Pass task specific options for the task as '-o taskname__option value'.  You can specify more than one option by using -o more than once.",
)
@click.option(
    "--no-prompt",
    is_flag=True,
    help="Disables all prompts.  Set for non-interactive mode use such as calling from scripts or CI systems",
)
@pass_runtime(require_keychain=True)
def flow_freeze(runtime, flow_name, org, debug, o, no_prompt=True):

    # Get necessary configs
    org, org_config = runtime.get_org(org)

    # Parse command line options
    options = defaultdict(dict)
    if o:
        for key, value in o:
            if "__" in key:
                task_name, option_name = key.split("__")
                options[task_name][option_name] = value
            else:
                raise click.UsageError(
                    "-o option for flows should contain __ to split task name from option name."
                )

    # Create the flow and handle initialization exceptions
    try:
        coordinator = runtime.get_flow(flow_name, options=options)
        start_time = datetime.now()
        steps = {}
        for step in coordinator.freeze(org_config):
            stepnum = len(steps)
            steps[stepnum] = step

        steps_hash = hash_obj(steps)
        duration = datetime.now() - start_time
        click.echo(f"Froze {flow_name} in {format_duration(duration)}")
        frozen_name = f"{flow_name}__{steps_hash}"
        filename = f"{frozen_name}.yml"
        frozen_flow = coordinator.flow_config.config
        frozen_flow["description"] = (
            f"Frozen version of {flow_name} with hash {steps_hash}"
        )
        frozen_flow["steps"] = steps
        with open(filename, "w") as f:
            yaml.dump({"flows": {frozen_name: frozen_flow}}, f)
        set_github_output("FLOW_FILENAME", filename)
        click.echo(f"Frozen flow saved to {filename}")
    except Exception:
        runtime.alert(f"Flow error: {flow_name}")
        raise

    runtime.alert(f"Flow Complete: {flow_name}")
