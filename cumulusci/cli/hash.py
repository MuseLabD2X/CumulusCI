import click
import hashlib
import json
import os
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from cumulusci.core.dependencies.resolvers import (
    get_static_dependencies,
    hash_static_dependencies,
)
from cumulusci.core.utils import process_list_arg
from cumulusci.core.github import set_github_output
from cumulusci.core.org_history import ActionScratchDefReference
from cumulusci.utils.hashing import hash_obj
from cumulusci.utils.serialization import decode_value, json_dumps
from cumulusci.utils.yaml.render import yaml_dump
from pydantic import BaseModel
from .runtime import pass_runtime


@click.group(
    "hash",
    help="Commands for hashing parts of the project's CumulusCI configuration and state",
)
def hash_group():
    pass


# Commands for group: hash


@hash_group.command(
    name="config",
    help="Hashes all or part of the project's merged CumulusCI configuration",
)
@pass_runtime(require_project=True, require_keychain=False)  # maybe not needed...
@click.option(
    "--locators",
    "locators",
    help="A comma separated list of CumulusCI config locators to specify the top level of config key(s) to hash. Example: project__package,flows__ci_beta",
)
def hash_config(
    runtime,
    locators,
):
    locators_str = "for {}".format(locators) if locators else ""
    locators = process_list_arg(locators)
    config = runtime.project_config.config
    if locators:
        config = {loc: runtime.project_config.lookup(loc) for loc in locators}
    config_hash = hash_obj(config)
    click.echo(f"Hash of CumulusCI Config{locators_str}:")
    click.echo(config_hash)
    output_name = "HASH_CONFIG"
    if locators:
        output_name + "__" + "__AND__".join(locators)
    set_github_output(output_name, config_hash)


@hash_group.command(
    name="flow",
    help="Hashes a flow's configuration, either dynamic or frozen as a flat list of static steps",
)
@pass_runtime(require_project=True, require_keychain=False)  # maybe not needed...
@click.argument("flow_name")
@click.option(
    "--freeze",
    is_flag=True,
    help="Freeze the flow configuration as a flat list of static steps",
)
def hash_flow(
    runtime,
    flow_name,
    freeze,
):
    flow = runtime.get_flow(flow_name)

    steps = flow.steps
    if freeze:
        steps = flow.freeze(org_config=None)
    config_hash = hash_obj(steps)
    click.echo(f"Hash of flow {flow_name}:")
    click.echo(config_hash)
    output_name = "HASH_FLOW__" + flow_name
    if freeze:
        output_name + "__FROZEN"
    set_github_output(output_name, config_hash)


@hash_group.command(
    name="dependencies",
    help="Resolve and hash the project's current dependencies",
)
@click.option(
    "--resolution-strategy",
    help="The resolution strategy to use. Defaults to production.",
    default="production",
)
@click.option(
    "--org",
    help="Optional scratch org config name (ex: dev) to use for calculating org shape hashes. Does not trigger org creation. If omitted, org shape hashes are not calculated.",
)
@click.option(
    "--json",
    "print_json",
    is_flag=True,
    help="Output the hashes and content as JSON",
)
@click.option(
    "--indent",
    type=int,
    default=None,
    help="Indentation level for JSON output",
)
@pass_runtime(require_keychain=True)
def hash_dependencies(runtime, resolution_strategy, org, print_json, indent):
    org_config = None
    dependencies_hash = None
    org_shape_hash = None
    snapshot = None
    snapshot_hash = None
    if org:
        org_config = runtime.keychain.get_org(org)

    console = Console()
    if org_config:
        if not org_config.scratch:
            raise click.ClickException(f"Org {org} is not a scratch org")

        console.print(Panel(f"Calculating org shape hash for {org}..."))
        scratch_config = ActionScratchDefReference(path=org_config.config_file)
        org_shape_hash = scratch_config.get_snapshot_shape_hash()
        console.print(f"Hash of Org Shape for {org}:")
        console.print(Text(org_shape_hash, style="bold"))

    console.print(
        Panel(f"Calculating static dependency hashes for {resolution_strategy}...")
    )
    resolved = get_static_dependencies(
        runtime.project_config,
        resolution_strategy=resolution_strategy,
    )
    dependencies = []
    table = Table(title=f"Resolved Depedenencies", show_lines=True)
    table.add_column("Type")
    table.add_column("Package or URL")
    table.add_column("Version or Ref")
    table.add_column("Details")

    for dependency in resolved:
        dep_type = dependency.__class__.__name__
        dep_type = dep_type.replace("Dependency", "")

        package_or_url_fields = [
            "github",
            "zip_url",
            "package_name",
            "namespace",
            "package",
        ]
        package_or_url = []
        for field in package_or_url_fields:
            if hasattr(dependency, field):
                value = getattr(dependency, field)
                if value not in package_or_url:
                    package_or_url.append(value)

        version_or_ref_fields = [
            "version",
            "version_id",
            "ref",
        ]
        version_or_ref = []
        for field in version_or_ref_fields:
            if hasattr(dependency, field):
                value = getattr(dependency, field)
                if field == "ref":
                    value = value[:7]
                version_or_ref.append(value)

        details = {
            key: value
            for key, value in dependency.dict(
                exclude_defaults=True,
            ).items()
            if key not in package_or_url_fields + version_or_ref_fields
        }

        table.add_row(
            dep_type,
            "\n".join(package_or_url) or "N/A",
            "\n".join(version_or_ref) or "N/A",
            yaml_dump(details, indent=2) if details else "N/A",
        )
        dependencies.append(decode_value(dependency.dict()))

    console.print(table)
    console.print()

    dependencies_hash = hash_obj(dependencies)
    console.print(Text(f"Resolved Dependencies Successfully", style="bold green"))

    console.print(Panel(f"Calculating tracker hash of dependencies..."))
    tracker_hash, hash_content = hash_static_dependencies(
        project_config=runtime.project_config, dependencies=resolved
    )

    if org_shape_hash:
        snapshot = {
            "org_shape": org_shape_hash,
            "tracker": hash_content,
        }
        snapshot_hash = hash_obj(snapshot)

    if print_json:
        data = {
            "hashes": {
                "dependencies": dependencies_hash,
                "org_shape": org_shape_hash,
                "snapshot": snapshot_hash,
                "tracker": tracker_hash,
            },
            "content": snapshot,
        }
        click.echo(json_dumps(data, indent=indent))
        return

    table = Table(
        title=f"Org Hashes for {org} dependencies, resolution strategy: {resolution_strategy}",
        show_lines=True,
        title_justify="left",
    )
    table.add_column("Type")
    table.add_column("Hash")
    table.add_column("Content")

    for dep_type, content in hash_content.items():
        dep_hash = hash_obj(content)
        if isinstance(content, list):
            content = yaml_dump(content, indent=4)
        table.add_row(dep_type, dep_hash, content)

    console.print()
    console.print(table)
    console.print()

    console.print(Text("Static Dependency Hashes:"))
    console.print(Text(f"  Dependencies: {deps_hash}", style="bold"))
    console.print(Text(f"  Tracker: {tracker_hash}", style="bold"))
    if org_shape_hash:
        snapshot_hash = hash_obj(
            dict(
                org_shape=org_shape_hash,
                tracker=tracker_hash,
            )
        )
        console.print(Text(f"  Org Shape: {org_shape_hash}", style="bold"))
        console.print(Text(f"  Snapshot Hash: {snapshot_hash}", style="bold"))
    console.print()
