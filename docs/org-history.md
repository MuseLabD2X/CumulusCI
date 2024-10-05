# Org History Tracking in CumulusCI

## Overview

Org History Tracking is a powerful feature in CumulusCI that provides comprehensive observability and auditing capabilities for all actions performed against Salesforce orgs. This feature allows you to maintain a detailed record of tasks, flows, and other operations, enabling better troubleshooting, compliance, and optimization of your Salesforce development processes.

CumulusCI has always excelled at performing dynamic work behind the scenes, streamlining many aspects of Salesforce development. However, understanding the specifics of these automated processes has traditionally been challenging. The Org History Tracking feature changes this by providing visibility into both the inputs you provide and the calculated actions that CumulusCI performs against your orgs.

## Enabling Org History Tracking

To enable Org History Tracking for a scratch org, add the `track_history` option to your org's configuration in the `cumulusci.yml` file:

```yaml
orgs:
    scratch:
        dev: # or other org name
            # ... other configuration options ...
            track_history: True
```

You can also enable or disable history tracking for an org using the CLI:

```
cci history enable [ORGNAME]
cci history disable [ORGNAME]
```

## Data Tracked

Org History Tracking captures a wide range of data for each action performed.

Org History is part of the CumulusCI OrgConfig and is stored encrypted in the CumulusCI keychain. No Org History data is sent externally.

The top-level OrgActionResult models include:

### BaseOrgActionResult

-   `hash_action`: A unique hash for the action instance
-   `hash_config`: A unique hash representing the action instance's configuration
-   `duration`: The duration of the action
-   `status`: The status of the action (success, failure, error)
-   `log`: The log output of the action
-   `exception`: The exception message if the action failed

### TaskOrgAction (inherits from BaseOrgActionResult)

-   `name`: The name of the task
-   `description`: The description of the task
-   `group`: The group of the task
-   `class_path`: The class of the task
-   `options`: The options passed to the task
-   `parsed_options`: The options after being parsed by the task
-   `files`: File references used by the task
-   `directories`: Directory references used by the task
-   `commands`: Commands executed by the task
-   `deploys`: Metadata deployments executed by the task
-   `retrieves`: Metadata retrievals executed by the task
-   `transforms`: Metadata transformations executed by the task
-   `package_installs`: Package installs executed by the task
-   `return_values`: The return values of the task

TaskOrgAction provides detailed tracking of various operations, including:

-   File and directory references
-   Metadata API interactions (retrieve/deploy/transform)
-   Package installations
-   Command executions

Each of these references is tracked and hashed for later comparison, providing a comprehensive view of the task's operations.

### FlowOrgAction (inherits from BaseOrgActionResult)

-   `name`: The name of the flow
-   `description`: The description of the flow
-   `group`: The group of the flow
-   `config_steps`: The flow configuration
-   `steps`: The details and results from all steps in the flow

### Other Action Types

-   `OrgCreateAction`: Tracks org creation details
-   `OrgConnectAction`: Tracks org connection details
-   `OrgDeleteAction`: Tracks org deletion details
-   `OrgImportAction`: Tracks org import details

## Hashes

Org History makes extensive use of hashes which represent a unique set of data as a short 8 character string, similar to how a commit sha works in git. There are multiple hash types calculated, each serving their own purpose:

-   **Action**: A unique identifier for a particular instance of OrgAction on an instance of an org. Mostly used in `cci history info` to show the full details of an action
-   **Config**: A unique identifier of the CumulusCI configuration of an OrgAction or set of OrgActions
    -   **Dynamic**: The dynamic configuration from cumulusci.yml or cci option flags. Re-running will recalculate options, potentially resulting a different Config Static but the same Config Dynamic hash.
    -   **Static**: A unique identifier of the resolved, static configuration as executed previously. Re-running should produce the same result.
-   **Dependencies**: A unique identifier of the CumulusCI dependencies installed/configured in the org
    -   **Dynamic**: A unique identifier of the dynamic dependencies and resolution strategies used to resolve dependencies for an OrgAction or set of OrgActions. Re-running from dynamic dependencies re-runs the dependency resolution, potentially finding newer/different versions.
    -   **Static**: A unique identifier of the resolved static dependencies for an OrgAction or set of OrgActions. Re-runing static configuration recreates the exact same set of dependencies (package versions and unmanaged github or zip_url metadata)
-   **Tracker**: A unique identifier of the tracked operations including: deploys (repo, github, zip_url), retrieves, transforms (including diff of metadata xml changes), package installs/upgrades, command executions, and references to resources (files, directories, and urls).
    -   **Static**: A unique indentifer of the actual contents as executed. For example, the Tracker Static hash of a deploy only hashes the base64 encoded metadata package_zip being deployed, regardless of the path it came from. This is useful for identifying different OrgActions that wound up deploying the same metadata.
    -   **Dynamic**: A unique identifier of the dynamic configuration that went into the tracked operations. For a deploy of local repo files, the Tracker Dynamic hash would use the path to directory of metadata deployed rather than the actual contents deployed. This is useful for checking if the Tracker Static hash has changed by recalculating it using a different local repo commit.

## Best Practices

1. **Regular Review**: Periodically review your org's history to understand patterns and identify potential issues.
2. **Compliance**: Use the detailed history for audit trails and compliance reporting.
3. **Troubleshooting**: When issues arise, use the history to understand recent changes and their impacts.
4. **Optimization**: Analyze action durations and patterns to optimize your development and deployment processes.
5. **Documentation**: Use the history as a basis for automatically generating documentation of org changes over time.

## Limitations and Considerations

-   History tracking may have a small performance impact and increase storage requirements.
-   Sensitive information in command outputs is automatically redacted, but always review before sharing histories.
-   History is stored with the org's configuration and is not automatically synced across different development environments.

## Understanding Task Options

CumulusCI tasks often perform dynamic calculations based on the options you provide. The Org History Tracking feature gives you visibility into both the original options you passed to a task (`options`) and the calculated options that the task actually used (`parsed_options`). This can be particularly useful for understanding how CumulusCI interprets and processes your inputs.

For example, if your project is using CumulusCI's dynamic dependencies to install a product from another GitHub repository, like NPSP:

```
cci task run update_dependencies
```

The history will show track two sets of options:

-   `options`: the original options you provided, in this case none so the default project dependencies specified under `project -> dependencies` in the cumulusci.yml file are used
-   `parsed_options`: the dynamically resolved options the task calculated and used for excution. In this case, that's the results of the dyanmic resolution of NPSP and all its latest dependencies.

This can help you understand how CumulusCI is interpreting your inputs and what additional default values or calculations it's applying.

## Future Enhancements (Proposed)

-   A mechanism for custom code to hook into OrgAction events
-   Advanced search and filtering capabilities for large histories
-   Integrate source tracking via Salesforce's SourceMember object into org history to track changes made in the org directly
-   Visual timeline representation of org history
