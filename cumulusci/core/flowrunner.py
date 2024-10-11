""" FlowRunner contains the logic for actually running a flow.

Flows are an integral part of CCI, they actually *do the thing*. We've been getting
along quite nicely with BaseFlow, which turns a flow definition into a callable
object that runs the flow in one fell swoop. We named it BaseFlow thinking that,
like tasks, specific flows might subclass it to extend behavior. In practice,
unlike BaseTask, subclasses ended up representing variations in how the flow
should actually be executed. We added callback hooks like pre_task and post_task
for host systems embedding cci, like web apps, to inspect the flow in progress.

BaseFlow suited us well.

FlowRunner is a v2 API for flows in CCI. There are two objects of interest:

- FlowCoordinator: takes a flow_config & runtime options to create a set of StepSpecs
  - Meant to replace the public API of BaseFlow, including override hooks.
  - Precomputes a flat list of steps, instead of running Flow recursively.
- TaskRunner: encapsulates the actual task running, result providing logic.

Upon initialization, FlowRunner:

- Creates a logger
- Validates that there are no cycles in the given flow_config
- Validates that the flow_config is using new-style-steps
- Collects a list of StepSpec objects that define what the flow will do.

Upon running the flow, FlowRunner:

- Refreshes the org credentials
- Runs each StepSpec in order
- * Logs the task or skip
- * Updates any ^^ task option values with return_values references
- * Creates a TaskRunner to run the task and get the result
- * Re-raise any fatal exceptions from the task, if not ignore_failure.
- * collects StepResults into the flow.

TaskRunner:

- Imports the actual task module.
- Constructs an instance of the BaseTask subclass.
- Runs/calls the task instance.
- Returns results or exception into an immutable StepResult

Option values/overrides can be passed in at a number of levels, in increasing order of priority:

- Task default (i.e. `.tasks__TASKNAME__options`)
- Flow definition task options (i.e. `.flows__FLOWNAME__steps__STEPNUM__options`)
- Flow definition subflow options (i.e. `.flows__FLOWNAME__steps__STEPNUM__options__TASKNAME`)
    see `dev_org_namespaced` for an example
- Flow runtime (i.e. on the commandline)

"""

import copy
import logging
from collections import defaultdict
from operator import attrgetter
from typing import (
    TYPE_CHECKING,
    Any,
    DefaultDict,
    Dict,
    List,
    NamedTuple,
    Optional,
    Tuple,
    Type,
    Union,
)

from jinja2.sandbox import ImmutableSandboxedEnvironment

from cumulusci.core.config import FlowConfig, TaskConfig
from cumulusci.core.config.org_config import OrgConfig
from cumulusci.core.org_history import (
    ActionScratchDefReference,
    OrgActionStatus,
    FlowOrgAction,
    FlowActionStep,
    FlowActionTracker,
    TaskActionTracker,
    TaskOrgAction,
)

from cumulusci.core.config.project_config import BaseProjectConfig
from cumulusci.core.exceptions import (
    CumulusCIFailure,
    FlowConfigError,
    FlowInfiniteLoopError,
    TaskImportError,
)
from cumulusci.salesforce_api.snapshot import SnapshotManager
from cumulusci.core.sfdx import get_devhub_api
from cumulusci.utils import cd
from cumulusci.utils.hashing import hash_obj
from cumulusci.utils.version_strings import StepVersion

if TYPE_CHECKING:
    from cumulusci.core.tasks import BaseTask


RETURN_VALUE_OPTION_PREFIX = "^^"

jinja2_env = ImmutableSandboxedEnvironment()


class StepSpec:
    """simple namespace to describe what the flowrunner should do each step"""

    __slots__ = (
        "step_num",
        "task_name",
        "task_config",
        "task_class",
        "project_config",
        "allow_failure",
        "path",
        "skip",
        "skip_steps",
        "skip_from",
        "start_from",
        "when",
    )

    step_num: StepVersion
    task_name: str
    task_config: dict
    task_class: Optional[
        Type["BaseTask"]
    ]  # None means this step was skipped by setting task: None
    project_config: BaseProjectConfig
    allow_failure: bool
    path: str
    skip: bool
    skip_steps: List[str]
    skip_from: str
    start_from: str
    when: Optional[str]

    def __init__(
        self,
        step_num: StepVersion,
        task_name: str,
        task_config: dict,
        task_class: Optional[Type["BaseTask"]],
        project_config: BaseProjectConfig,
        allow_failure: bool = False,
        from_flow: Optional[str] = None,
        skip: bool = False,
        skip_from: Optional[str] = None,
        skip_steps: Optional[List[str]] = None,
        start_from: Optional[str] = None,
        when: Optional[str] = None,
    ):
        if not isinstance(step_num, StepVersion):
            step_num = StepVersion(step_num)
        self.step_num = step_num
        self.task_name = task_name
        self.task_config = task_config
        self.task_class = task_class
        self.project_config = project_config
        self.allow_failure = allow_failure
        self.skip = skip
        self.skip_from = skip_from
        self.skip_steps = skip_steps or []
        self.start_from = start_from
        self.when = when

        # Store the dotted path to this step.
        # This is not guaranteed to be unique, because multiple steps
        # in the same flow can reference the same task name with different options.
        # It's here to support the ^^flow_name.task_name.attr_name syntax
        # for referencing previous task return values in options.
        if from_flow:
            self.path = ".".join([from_flow, task_name])
        else:
            self.path = task_name

    def __repr__(self):
        skipstr = ""
        if self.skip:
            skipstr = "!SKIP! "
        return (
            f"<{skipstr}StepSpec {self.step_num}:{self.task_name} {self.task_config}>"
        )


class StepPrediction(NamedTuple):
    step_num: StepVersion
    task_name: str
    task_config: dict
    task_class: Optional[
        Type["BaseTask"]
    ]  # None means this step was skipped by setting task: None
    allow_failure: bool
    path: str
    skip: bool
    skip_steps: List[str]
    skip_from: str
    start_from: str
    when: Optional[str]
    tracker: TaskActionTracker

    @classmethod
    def from_step(cls, step: StepSpec, tracker: TaskActionTracker):
        return cls(
            step.step_num,
            step.task_name,
            step.task_config,
            step.task_class,
            step.allow_failure,
            step.path,
            step.skip,
            step.skip_steps,
            step.skip_from,
            step.start_from,
            step.when,
            tracker,
        )


class StepResult(NamedTuple):
    step_num: StepVersion
    task_name: str
    path: str
    result: Any
    return_values: Any
    exception: Optional[Exception]
    action: TaskOrgAction | FlowOrgAction | None


class FlowCallback:
    """A subclass of FlowCallback allows code running a flow
    to inject callback methods to run during the flow. Anything you
    would like the FlowCallback to have access to can be passed to the
    constructor. This is typically used to pass a Django model or model id
    when running a flow inside of a web app.

    Example subclass of FlowCallback:

        class CustomFlowCallback(FlowCallback):
            def __init__(self, model):
                self.model = model

            def post_task(self, step, result):
                # do something to record state on self.model

    Once a subclass is defined, you can instantiate it, and
    pass it as the value for the 'callbacks' keyword argument
    when instantiating a FlowCoordinator.

    Example running a flow with custom callbacks:

        custom_callbacks = CustomFlowCallbacks(model_instance)
        flow_coordinator = FlowCoordinator(
            project_config,
            flow_config,
            name=flow_name,
            options=options,
            callbacks=custom_callbacks,
        )
        flow_coordinator.run(org_config)


    """

    def pre_flow(self, coordinator: "FlowCoordinator"):
        """This is passed an instance of FlowCoordinator,
        that pertains to the flow which is about to run."""
        pass

    def post_flow(self, coordinator: "FlowCoordinator"):
        """This is passed an instance of FlowCoordinator,
        that pertains to the flow just finished running.
        This step executes whether or not the flow completed
        successfully."""
        pass

    def pre_task(self, step: StepSpec):
        """This is passed an instance StepSpec, that
        pertains to the task which is about to run."""
        pass

    def post_task(self, step: StepSpec, result: StepResult):
        """This method is called after a task has executed.

        :param step: Instance of StepSpec that relates to the task which executed
        :param result: Instance of the StepResult class that was run. Attributes of
        interest include, `result.result`, `result.return_values`, and `result.exception`
        """
        pass


class TaskRunner:
    """TaskRunner encapsulates the job of instantiating and running a task."""

    step: StepSpec
    org_config: Optional[OrgConfig]
    flow: Optional["FlowCoordinator"]

    def __init__(
        self,
        step: StepSpec,
        org_config: Optional[OrgConfig],
        flow: Optional["FlowCoordinator"] = None,
    ):
        self.step = step
        self.org_config = org_config
        self.flow = flow

    @classmethod
    def from_flow(cls, flow: "FlowCoordinator", step: StepSpec) -> "TaskRunner":
        return cls(step, flow.org_config, flow=flow)

    def _init_task(self, **options) -> "BaseTask":
        """
        Initialize a task.

        :return: BaseTask
        """

        # Resolve ^^task_name.return_value style option syntax
        task_config = self.step.task_config.copy()
        task_config["options"] = task_config.get("options", {}).copy()
        assert self.flow
        self.flow.resolve_return_value_options(task_config["options"])

        task_config["options"].update(options)

        assert self.step.task_class

        return self.step.task_class(
            self.step.project_config,
            TaskConfig(task_config),
            org_config=self.org_config,
            name=self.step.task_name,
            stepnum=self.step.step_num,
            flow=self.flow,
        )

    def predict_step(self, **options) -> "StepPrediction":
        """
        Predict a step.

        :return: StepResult
        """
        task = self._init_task(**options)
        tracker = task(predict=True)
        return StepPrediction.from_step(
            self.step,
            tracker,
        )

    def run_step(self, **options) -> StepResult:
        """
        Run a step.

        :return: StepResult
        """
        task = self._init_task(**options)
        self._log_options(task)
        exc = None
        try:
            task()
        except Exception as e:
            self.flow.logger.error(f"Exception in task {self.step.path}")
            exc = e
        return StepResult(
            self.step.step_num,
            self.step.task_name,
            self.step.path,
            task.result,
            task.return_values,
            exc,
            FlowActionStep(task=task.action),
        )

    def _log_options(self, task: "BaseTask"):
        if not task.task_options:
            task.logger.info("No task options present")
            return
        task.logger.info("Options:")
        for key, info in task.task_options.items():
            value = task.options.get(key)
            if value is not None:
                if type(value) is not list:
                    value = self._obfuscate_if_sensitive(value, info)
                    task.logger.info(f"  {key}: {value}")
                else:
                    task.logger.info(f"  {key}:")
                    for v in value:
                        v = self._obfuscate_if_sensitive(v, info)
                        task.logger.info(f"    - {v}")

    def _obfuscate_if_sensitive(self, value: str, info: dict) -> str:
        if info.get("sensitive"):
            value = 8 * "*"
        return value


class FlowCoordinator:
    org_config: Optional[OrgConfig]
    steps: List[StepSpec]
    callbacks: FlowCallback
    logger: logging.Logger
    skip: List[str]
    skip_from: List[str]
    flow_config: FlowConfig
    runtime_options: dict
    name: Optional[str]
    results: List[StepResult]
    tracker: FlowActionTracker
    action: FlowOrgAction | None
    predict_org_config: Optional[OrgConfig] | None
    use_snapshots: bool = False
    force_use_snapshots: bool = False

    def __init__(
        self,
        project_config: BaseProjectConfig,
        flow_config: FlowConfig,
        name: Optional[str] = None,
        options: Optional[dict] = None,
        skip: Optional[List[str]] = None,
        skip_from: Optional[List[str]] = None,
        start_from: Optional[str] = None,
        callbacks: Optional[FlowCallback] = None,
        predict_org_config: Optional[OrgConfig] = None,
        use_snapshots: Optional[bool] = None,
        force_use_snapshots: Optional[bool] = None,
        logger: Optional[logging.Logger] = None,
    ):
        self.project_config = project_config
        self.flow_config = flow_config
        self.name = name
        self.org_config = None
        self.predict_org_config = predict_org_config

        if not callbacks:
            callbacks = FlowCallback()
        self.callbacks = callbacks

        self.runtime_options = options or {}

        self.skip = skip or []
        self.skip_from = skip_from or None
        self.start_from = start_from
        self.skip_beginning = start_from is not None
        self.skip_remaining = False
        self.results = []

        self.logger = self._init_logger(logger)
        self.steps = self._init_steps()

        self.tracker = FlowActionTracker(
            name=self.name,
            description=self.flow_config.description,
            group=self.flow_config.group,
            config_steps={
                StepVersion(str(key)): step
                for key, step in self.flow_config.config.get("steps").items()
            },
            steps=[],
            repo=self.project_config.repo_url,
            branch=self.project_config.repo_branch,
            commit=self.project_config.repo_commit,
        )
        self.action = None
        self.force_use_snapshots = force_use_snapshots
        self.use_snapshots = all(
            [
                use_snapshots,
                self.flow_config.use_snapshots,
            ]
        )
        if not self.use_snapshots and force_use_snapshots:
            self.logger.warning(
                "force_use_snapshots is set to True but the flow is set to False, overriding flow `use_snapshots` setting."
            )
            self.use_snapshots = True

    @classmethod
    def from_steps(
        cls,
        project_config: BaseProjectConfig,
        steps: List[StepSpec],
        name: Optional[str] = None,
        callbacks: Optional[FlowCallback] = None,
    ):
        instance = cls(
            project_config,
            flow_config=FlowConfig({"steps": {}}),
            name=name,
            callbacks=callbacks,
        )
        instance.steps = steps
        return instance

    def _rule(self, fill="=", length=60, new_line=False):
        self.logger.info(f"{fill * length}")
        if new_line:
            self.logger.info("")

    def get_summary(self, verbose=False):
        """Returns an output string that contains the description of the flow
        and its steps."""
        lines = []
        if "description" in self.flow_config.config:
            lines.append(f"Description: {self.flow_config.config['description']}")

        step_lines = self.get_flow_steps(verbose=verbose)
        if step_lines:
            lines.append("\nFlow Steps")
        lines.extend(step_lines)

        return "\n".join(lines)

    def get_flow_steps(
        self, for_docs: bool = False, verbose: bool = False
    ) -> List[str]:
        """Returns a list of flow steps (tasks and sub-flows) for the given flow.
        For docs, indicates whether or not we want to use the string for use in a code-block
        of an rst file. If True, will omit output of source information."""
        lines = []
        previous_parts = []
        previous_source = None
        for step in self.steps:
            parts = step.path.split(".")
            steps = str(step.step_num).split("/")
            if len(parts) > len(steps):
                # Sub-step generated during freeze process; skip it
                continue
            task_name = parts.pop()

            i = -1
            new_source = (
                f" [from {step.project_config.source}]"
                if step.project_config.source is not previous_source
                else ""
            )
            options_info = ""
            for i, flow_name in enumerate(parts):
                if not any(":" in part for part in step.path.split(".")[i + 1 :]):
                    source = new_source
                else:
                    source = ""
                if len(previous_parts) < i + 1 or previous_parts[i] != flow_name:
                    if for_docs:
                        source = ""

                    line = f"{'    ' * i}{steps[i]}) flow: {flow_name}{source}"
                    if step.skip:
                        line += " [skip]"
                        line = f"\033[90m{line}\033[0m"  # Gray color
                    lines.append(line)
                    if source:
                        new_source = ""

            padding = "    " * (i + 1) + " " * len(str(steps[i + 1]))
            when = f"{padding}  when: {step.when}" if step.when is not None else ""

            if for_docs:
                new_source = ""

            if step.task_config.get("options"):
                if verbose:
                    options = step.task_config.get("options")
                    options_info = f"{padding}  options:"

                    for option, value in options.items():
                        options_info += f"\n{padding}      {option}: {value}"

            line = f"{'    ' * (i + 1)}{steps[i + 1]}) task: {task_name}{new_source}"
            if step.skip:
                line += " [skip]"
                line = f"\033[90m{line}\033[0m"  # Gray color
            lines.append(line)

            if when:
                lines.append(when)

            if options_info:
                lines.append(options_info)

            previous_parts = parts
            previous_source = step.project_config.source

        return lines

    def freeze(self, org_config) -> List[StepSpec]:
        self.org_config = org_config
        line = f"Initializing flow for freezing: {self.__class__.__name__}"
        if self.name:
            line = f"{line} ({self.name})"
        self._rule()
        self.logger.info(line)
        self.logger.info(self.flow_config.description)
        self._rule(new_line=True)
        steps = []
        for step in self.steps:
            if step.skip:
                continue
            with cd(step.project_config.repo_root):
                task = step.task_class(
                    step.project_config,
                    TaskConfig(step.task_config),
                    name=step.task_name,
                )
                steps.extend(task.freeze(step))
        return steps

    def run(
        self,
        org_config: OrgConfig,
        predictions: Optional[List[StepPrediction]] = None,
        continue_from_path: Optional[str] = None,
    ):
        self.org_config = org_config
        line = f"Initializing flow: {self.__class__.__name__}"
        if self.name:
            line = f"{line} ({self.name})"
        self._rule()
        self.logger.info(line)
        self.logger.info(self.flow_config.description)
        self._rule(new_line=True)

        # Handle snapshot lookup
        if self.use_snapshots and predictions is None:
            self._init_org(predict=True)
            self._rule(fill="-")
            self.logger.info("Running prediction to look for active snapshots to use")
            self._rule(fill="-", new_line=True)
            self.org_config.use_snapshot = True
            predictions = self.predict(org_config)
            self.org_config = self.predict_snapshot(org_config, predictions)
        else:
            self.org_config = org_config

        self._init_org()

        self._rule(fill="-")
        self.logger.info("Organization:")
        self.logger.info(f"  Username: {org_config.username}")
        self.logger.info(f"    Org Id: {org_config.org_id}")
        self.logger.info(f"  Instance: {org_config.instance_name}")
        self._rule(fill="-", new_line=True)

        # Give pre_flow callback a chance to alter the steps
        # based on the state of the org before we display the steps.
        self.callbacks.pre_flow(self)

        self._rule(fill="-")
        self.logger.info("Steps:")
        for line in self.get_summary().splitlines():
            self.logger.info(line)
        self._rule(fill="-", new_line=True)

        self.logger.info("Starting execution")
        self._rule(new_line=True)

        try:
            for step in self.steps:
                if continue_from_path is not None:
                    if step.path == continue_from_path:
                        continue_from_path = None
                    self.logger.info(
                        f"Skipping step {step.path} due to continue_from_path={continue_from_path}"
                    )
                    continue
                self._run_step(step)
            flow_name = f"'{self.name}' " if self.name else ""
            self.logger.info(
                f"Completed flow {flow_name}on org {org_config.name} successfully!"
            )
            self._record_result()
        except Exception as e:
            self._record_result(e)
            raise e from e
        finally:
            self.callbacks.post_flow(self)

    def predict_snapshot(
        self,
        org_config: OrgConfig,
        predictions: List[StepPrediction],
        devhub_api=None,
    ):
        """Uses a flow's predictions to determine the best snapshot to use for a scratch org"""
        predicted_snapshots = []
        tracker = {
            "deploys": [],
            "package_installs": [],
            "transforms": [],
        }
        snapshot_hashes = []
        scratch_config = ActionScratchDefReference(path=self.org_config.config_file)

        snapshot_hash_content = {
            "org_shape": scratch_config.get_snapshot_shape_hash(),
            "tracker": tracker,
        }

        for prediction in predictions:
            if not prediction or not prediction.tracker:
                continue
            step_hash_content = prediction.tracker.get_org_tracker_hash(
                return_data=True
            )
            if not step_hash_content:
                continue
            tracker["deploys"].extend(step_hash_content["deploys"])
            tracker["package_installs"].extend(step_hash_content["package_installs"])
            tracker["transforms"].extend(step_hash_content["transforms"])

            snapshot_hashes.append(
                {
                    "path": prediction.path,
                    "tracker_hash": hash_obj(step_hash_content),
                    "snapshot_hash": hash_obj(snapshot_hash_content),
                    "content": step_hash_content,
                }
            )

        if snapshot_hashes:
            self.logger.info(
                "Identified {} snapshot hashes to check for in the DevHub".format(
                    len(snapshot_hashes)
                )
            )
            self.logger.info(
                "Getting access token for the DevHub to check for snapshots..."
            )
            if not devhub_api:
                devhub_api = get_devhub_api(
                    project_config=self.project_config, api_version="61.0"
                )  # API Version for GA of OrgSnapshots
            snapshots = SnapshotManager(devhub_api, logger=self.logger)
            description_where = [
                f"%hash:{step['snapshot_hash']}%" for step in snapshot_hashes
            ]
            description_where = tuple(description_where)
            res = snapshots.query(
                description_where=description_where,
                status="Active",
            )

            # Query active snapshots with matching hashes and build active_snapshots dict
            active_snapshots = {}
            if res["records"]:
                for snapshot in res["records"]:
                    for part in snapshot["Description"].split(" "):
                        if part.startswith("hash:"):
                            active_snapshots[part.split(":")[1]] = snapshot

            self._rule(fill="-")
            self.logger.info(f"Found {len(active_snapshots)} active snapshots to use")
            self._rule(fill="-")
            self.logger.info(f"Determining best snapshot...")

            # Check the step hashes against the active snapshots and set the snapshot name for the first snapshot found
            for snapshot in snapshot_hashes:
                if snapshot["snapshot_hash"] in active_snapshots:
                    active = active_snapshots[snapshot["snapshot_hash"]]
                    # Merge the hash info into the snapshot info
                    predicted_snapshot = dict(snapshot.items())
                    predicted_snapshot["snapshot"] = active
                    predicted_snapshots.append(predicted_snapshot)

                else:
                    self.logger.info(
                        f"No active snapshot found for {snapshot['path']} hash {snapshot['snapshot_hash']}"
                    )

            if predicted_snapshots:
                self._rule(fill="-")
                self.logger.info("Predicted Snapshot Matches")
                self._rule(fill="-", new_line=True)
                for predicted in predicted_snapshots:
                    self.logger.info(
                        f"Predicted snapshot for {predicted['path']} with hash {predicted['snapshot_hash']}\n"
                        f"  Snapshot: {predicted['snapshot']['SnapshotName']} ({predicted['snapshot']['Id']})\n"
                        f"  Description: {predicted['snapshot']['Description']}\n\n"
                    )
                self._rule(fill="-", new_line=True)

                self.logger.info(
                    f"Using snapshot {predicted['snapshot']['SnapshotName']} ({predicted['snapshot']['Id']}) to create org"
                )
                org_config.snapshot_hashes = predicted_snapshots
                org_config.use_snapshot_hashes = True
            else:
                self.logger.warning(
                    "No snapshots found to use. Skipping snapshot and creating a new scratch org."
                )

        else:
            self.logger.warning(
                "No snapshots found to use. Skipping snapshot and creating a new scratch org."
            )
        return org_config

    def predict(self, org_config: OrgConfig) -> List[StepPrediction]:
        line = f"Initializing flow for prediction only: {self.__class__.__name__}"
        if self.name:
            line = f"{line} ({self.name})"
        self._rule()
        self.logger.info(line)
        self.logger.info(self.flow_config.description)
        self._rule(new_line=True)

        # Use the passed org_config for prediction or construct one from the org config
        if self.predict_org_config is None:
            self.predict_org_config = OrgConfig(
                name=org_config.config_name,
                config={
                    "active": False,
                    "config_name": org_config.config_name,
                    "config_file": org_config.config_file,
                    "created": False,
                    "days": org_config.days,
                    "is_sandbox": org_config.is_sandbox,
                    "namespace": org_config.namespace,
                    "namespaced": org_config.namespaced,
                    "org_type": org_config.org_type,
                    "scratch": org_config.scratch,
                    "scratch_org_type": org_config.scratch_org_type,
                },
            )
        self.predict_org_config._installed_packages = defaultdict(list)
        # self.predict_org_config.installed_packages = lambda x: x._installed_packages
        # Prevent token refresh
        self.predict_org_config.refresh_oauth_token = lambda x: None

        self.org_config = self.predict_org_config
        self._init_org(predict=True)

        # Give pre_flow callback a chance to alter the steps
        # based on the state of the org before we display the steps.
        self.callbacks.pre_flow(self)

        self._rule(fill="-")
        self.logger.info("Steps:")
        for line in self.get_summary().splitlines():
            self.logger.info(line)
        self._rule(fill="-", new_line=True)

        self.logger.info("Starting prediction")
        self._rule(new_line=True)

        predictions = []
        installed = defaultdict(list)
        try:
            for step in self.steps:
                prediction = self._run_step(step, predict=True)
                if prediction and prediction.tracker:
                    if prediction.tracker.package_installs:
                        for package_install in prediction.tracker.package_installs:
                            version_info = package_install.to_version_info()
                            # Add to the defaultdict using different keys
                            if package_install.package_id:
                                installed[package_install.package_id].append(
                                    version_info
                                )
                            if package_install.namespace:
                                installed[package_install.namespace].append(
                                    version_info
                                )
                                installed[
                                    f"{package_install.namespace}@{package_install.version}"
                                ].append(version_info)
                predictions.append(prediction)
                self.org_config._installed_packages = installed.copy()
            flow_name = f"'{self.name}' " if self.name else ""
            self.logger.info(
                f"Completed flow {flow_name} prediction for org {org_config.name} successfully!"
            )
            return predictions
        except Exception as e:
            raise e from e
        finally:
            self.callbacks.post_flow(self)

    def _run_step(self, step: StepSpec, predict: bool = False):
        if step.skip:
            self._rule(fill="*")
            self.logger.info(f"Skipping task: {step.task_name}")
            self._rule(fill="*", new_line=True)
            if predict:
                return StepPrediction.from_step(
                    step,
                    tracker=None,
                )
            return

        if step.when:
            jinja2_context = {
                "project_config": step.project_config,
                "org_config": self.org_config,
            }
            expr = jinja2_env.compile_expression(step.when)
            value = expr(**jinja2_context)
            if not value:
                self.logger.info(
                    f"Skipping task {step.task_name} (skipped unless {step.when})"
                )
                if predict:
                    step.skip = True
                    return StepPrediction.from_step(
                        step,
                        tracker=None,
                    )
                return

        self._rule(fill="-")
        if predict:
            self.logger.info(f"Predicting task: {step.task_name}")
        else:
            self.logger.info(f"Running task: {step.task_name}")
        self._rule(fill="-", new_line=True)

        self.callbacks.pre_task(step)
        if predict:
            prediction = TaskRunner.from_flow(self, step).predict_step()
            if not prediction:
                return StepPrediction.from_step(
                    step,
                    tracker=None,
                )
            return prediction
        else:
            result = TaskRunner.from_flow(self, step).run_step()
            self.callbacks.post_task(step, result)
            self.results.append(
                result
            )  # add even a failed result to the result set for the post flow

        if result.exception and not step.allow_failure:
            raise result.exception  # PY3: raise an exception type we control *from* this exception instead?

    def _init_logger(self, logger: logging.Logger = None) -> logging.Logger:
        """
        Returns a logging.Logger-like object to use for the duration of the flow. Tasks will receive this logger
        and getChild(class_name) to get a child logger.

        :return: logging.Logger
        """
        logger = logger or logging.getLogger("cumulusci.flows")
        return logger.getChild(self.__class__.__name__)

    def _init_steps(self) -> List[StepSpec]:
        """
        Given the flow config and everything else, create a list of steps to run, sorted by step number.

        :return: List[StepSpec]
        """
        self._check_old_yaml_format()
        self._check_infinite_flows(self.flow_config)

        steps = []

        for number, step_config in self.flow_config.steps.items():
            specs = self._visit_step(number, step_config, self.project_config)
            steps.extend(specs)

        return sorted(steps, key=attrgetter("step_num"))

    def _visit_step(
        self,
        number: Union[str, int],
        step_config: dict,
        project_config: BaseProjectConfig,
        visited_steps: Optional[List[StepSpec]] = None,
        parent_options: Optional[dict] = None,
        parent_ui_options: Optional[dict] = None,
        from_flow: Optional[str] = None,
    ) -> List[StepSpec]:
        """
        for each step (as defined in the flow YAML), _visit_step is called with only
        the first two parameters. this takes care of validating the step, collating the
        option overrides, and if it is a task, creating a StepSpec for it.

        If it is a flow, we recursively call _visit_step with the rest of the parameters of context.

        :param number: StepVersion representation of the current step number
        :param step_config: the current step's config (dict from YAML)
        :param visited_steps: used when called recursively for nested steps, becomes the return value
        :param parent_options: used when called recursively for nested steps, options from parent flow
        :param parent_ui_options: used when called recursively for nested steps, UI options from parent flow
        :param from_flow: used when called recursively for nested steps, name of parent flow
        :return: List[StepSpec] a list of all resolved steps including/under the one passed in
        """
        step_number = StepVersion(str(number))

        if visited_steps is None:
            visited_steps = []
        if parent_options is None:
            parent_options = {}
        if parent_ui_options is None:
            parent_ui_options = {}

        # This should never happen because of cleanup
        # in core/utils/cleanup_old_flow_step_replace_syntax()
        assert step_config.keys() != {"task", "flow"}

        # Skips
        # - either in YAML (with the None string for task or flow on a step) or in the skip list on a flow step
        # - or by providing a skip list to the FlowRunner at initialization.
        task_or_flow = step_config.get("task", step_config.get("flow"))
        if task_or_flow and task_or_flow == self.start_from:
            self.skip_beginning = False
        if (
            task_or_flow == "None"
            or task_or_flow in self.skip
            or task_or_flow == self.skip_from
            or self.skip_remaining
            or self.skip_beginning
        ):
            if task_or_flow == self.skip_from:
                self.skip_remaining = True
            visited_steps.append(
                StepSpec(
                    step_num=step_number,
                    task_name=step_config.get("task", step_config.get("flow")),
                    task_config=step_config.get("options", {}),
                    task_class=None,
                    project_config=project_config,
                    from_flow=from_flow,
                    skip=True,  # someday we could use different vals for why skipped
                )
            )
            return visited_steps

        if "task" in step_config:
            name = step_config["task"]

            # get the base task_config from the project config, as a dict for easier manipulation.
            # will raise if the task doesn't exist / is invalid
            task_config = project_config.get_task(name)
            task_config_dict: dict = copy.deepcopy(task_config.config)
            if "options" not in task_config_dict:
                task_config_dict["options"] = {}

            # merge the options together, from task_config all the way down through parent_options
            step_overrides = copy.deepcopy(parent_options.get(name, {}))
            step_overrides.update(step_config.get("options", {}))
            task_config_dict["options"].update(step_overrides)

            # merge UI options from task config and parent flow
            if "ui_options" not in task_config_dict:
                task_config_dict["ui_options"] = {}
            step_ui_overrides = copy.deepcopy(parent_ui_options.get(name, {}))
            step_ui_overrides.update(step_config.get("ui_options", {}))
            task_config_dict["ui_options"].update(step_ui_overrides)

            # merge checks from task config and flow step
            if "checks" not in task_config_dict:
                task_config_dict["checks"] = []
            task_config_dict["checks"].extend(step_config.get("checks", []))

            # merge runtime options
            if name in self.runtime_options:
                task_config_dict["options"].update(self.runtime_options[name])

            # get implementation class. raise/fail if it doesn't exist, because why continue
            try:
                task_class = task_config.get_class()
            except (ImportError, AttributeError, TaskImportError) as e:
                raise FlowConfigError(f"Task named {name} has bad classpath, {e}")

            visited_steps.append(
                StepSpec(
                    step_num=step_number,
                    task_name=name,
                    task_config=task_config_dict,
                    task_class=task_class,
                    project_config=task_config.project_config,
                    allow_failure=step_config.get("ignore_failure", False),
                    from_flow=from_flow,
                    when=step_config.get("when"),
                )
            )
            return visited_steps

        if "flow" in step_config:
            name = step_config["flow"]
            if from_flow:
                path = ".".join([from_flow, name])
            else:
                path = name
            step_options = step_config.get("options", {})
            step_ui_options = step_config.get("ui_options", {})
            flow_config = project_config.get_flow(name)
            for sub_number, sub_stepconf in flow_config.steps.items():
                # append the flow number to the child number, since its a LooseVersion.
                # e.g. if we're in step 2.3 which references a flow with steps 1-5, it
                #   simply ends up as five steps: 2.3.1, 2.3.2, 2.3.3, 2.3.4, 2.3.5
                # TODO: how does this work with nested flowveride? what does defining step 2.3.2 later do?
                num = f"{number}/{sub_number}"
                self._visit_step(
                    number=num,
                    step_config=sub_stepconf,
                    project_config=flow_config.project_config,
                    visited_steps=visited_steps,
                    parent_options=step_options,
                    parent_ui_options=step_ui_options,
                    from_flow=path,
                )
        return visited_steps

    def _check_old_yaml_format(self):
        if self.flow_config.steps is None:
            if "tasks" in self.flow_config.config:
                raise FlowConfigError(
                    'Old flow syntax detected.  Please change from "tasks" to "steps" in the flow definition.'
                )
            else:
                raise FlowConfigError("No steps found in the flow definition")

    def _check_infinite_flows(self, flow_config, flow_stack=None):
        """
        Recursively loop through the flow_config and check if there are any cycles.

        :param flow_config: FlowConfig to traverse to find cycles/infinite loops
        :param flow_stack: list of flow signatures already visited
        :return: None
        """
        if not flow_stack:
            flow_stack = []
        project_config = flow_config.project_config

        for step in flow_config.steps.values():
            if "flow" in step:
                next_flow_name = step["flow"]
                if next_flow_name == "None":
                    continue

                next_flow_config = project_config.get_flow(next_flow_name)
                signature = (
                    hash(next_flow_config.project_config.source),
                    next_flow_config.name,
                )

                if signature in flow_stack:
                    raise FlowInfiniteLoopError(
                        f"Infinite flows detected with flow {next_flow_name}"
                    )
                flow_stack.append(signature)
                self._check_infinite_flows(next_flow_config, flow_stack)
                flow_stack.pop()

    def _init_org(self, predict: bool = False):
        """Test and refresh credentials to the org specified."""
        if (
            self.org_config
            and not self.org_config.use_snapshots
            and self.force_use_snapshots
        ):
            if not self.use_snapshots:
                self.logger.info(
                    "Overriding org config use_snapshots setting to True for this flow because of `force_use_snapshots`."
                )
                self.use_snapshots = True
        if self.org_config and self.use_snapshots and not self.org_config.scratch:
            raise FlowConfigError(
                "The use_snapshots option is only available for scratch orgs."
            )
        if self.org_config and self.use_snapshots and self.org_config.active:
            raise FlowConfigError(
                "The use_snapshots option is not available for active orgs. Use `cci org scratch_delete <org_name>` to delete the org and create a new snapshot."
            )
        # attempt to refresh the token, this can throw...
        if predict:
            self.logger.info(
                f"Skipping org credentials verification and refresh for prediction."
            )
            return
        with self.org_config.save_if_changed():
            self.logger.info(
                f"Verifying and refreshing credentials for the specified org: {self.org_config.name}."
            )
            self.org_config.refresh_oauth_token(self.project_config.keychain)

    def resolve_return_value_options(self, options):
        """Handle dynamic option value lookups in the format ^^task_name.attr"""
        for key, value in options.items():
            if isinstance(value, str) and value.startswith(RETURN_VALUE_OPTION_PREFIX):
                path, name = value[len(RETURN_VALUE_OPTION_PREFIX) :].rsplit(".", 1)
                result = self._find_result_by_path(path)
                options[key] = result.return_values.get(name)

    def _find_result_by_path(self, path):
        for result in self.results:
            if result.path[-len(path) :] == path:
                return result
        raise NameError(f"Path not found: {path}")

    def _record_result(self, exception=None) -> None:
        data = self.tracker.dict()
        if exception:
            if isinstance(exception, CumulusCIFailure):
                status = OrgActionStatus.FAILURE
            else:
                status = OrgActionStatus.ERROR
        else:
            status = OrgActionStatus.SUCCESS
        data["action_type"] = "Flow"
        data["log"] = ""
        data["exception"] = str(exception)
        data["status"] = status
        data["steps"] = [result.action for result in self.results if result.action]
        self.action = FlowOrgAction.parse_obj(data)


class PreflightFlowCoordinator(FlowCoordinator):
    """Coordinates running preflight checks instead of the actual flow steps."""

    preflight_results: DefaultDict[Optional[str], List[dict]]
    _task_caches: Dict[BaseProjectConfig, "TaskCache"]

    def run(self, org_config: OrgConfig):
        self.org_config = org_config
        self.callbacks.pre_flow(self)

        self._init_org()
        self._rule(fill="-")
        self.logger.info("Organization:")
        self.logger.info(f"  Username: {org_config.username}")
        self.logger.info(f"    Org Id: {org_config.org_id}")
        self._rule(fill="-", new_line=True)

        self.logger.info("Running preflight checks...")
        self._rule(new_line=True)

        self.preflight_results = defaultdict(list)
        # Expose for test access
        self._task_caches = {self.project_config: TaskCache(self, self.project_config)}
        try:
            # flow-level checks
            jinja2_context = {
                "tasks": self._task_caches[self.project_config],
                "project_config": self.project_config,
                "org_config": self.org_config,
            }
            for check in self.flow_config.checks or []:
                result = self.evaluate_check(check, jinja2_context)
                if result:
                    self.preflight_results[None].append(result)

            # Step-level checks
            for step in self.steps:
                jinja2_context["project_config"] = step.project_config
                # Create a cache for this project config, if not present
                # and not equal to the root project config.
                # This accommodates cross-project preflight checks.
                if step.project_config not in self._task_caches:
                    self._task_caches[step.project_config] = TaskCache(
                        self, step.project_config
                    )
                jinja2_context["tasks"] = self._task_caches[step.project_config]
                for check in step.task_config.get("checks", []):
                    result = self.evaluate_check(check, jinja2_context)
                    if result:
                        self.preflight_results[str(step.step_num)].append(result)
        finally:
            self.callbacks.post_flow(self)

    def evaluate_check(
        self, check: dict, jinja2_context: Dict[str, Any]
    ) -> Optional[dict]:
        self.logger.info(f"Evaluating check: {check['when']}")
        expr = jinja2_env.compile_expression(check["when"])
        value = bool(expr(**jinja2_context))
        self.logger.info(f"Check result: {value}")
        if value:
            return {"status": check["action"], "message": check.get("message")}


class TaskCache:
    """Provides access to named tasks and caches their results.

    This is intended for use in a jinja2 expression context
    so that multiple expressions evaluated in the same context
    can avoid running a task more than once with the same options.
    """

    project_config: BaseProjectConfig
    results: Dict[Tuple[str, Tuple[Any]], Any]

    def __init__(self, flow: FlowCoordinator, project_config: BaseProjectConfig):
        self.flow = flow
        # Cross-project flows may include preflight checks
        # that depend on their local context.
        self.project_config = project_config
        self.results = {}

    def __getattr__(self, task_name: str):
        return CachedTaskRunner(self, task_name)


class CachedTaskRunner:
    """Runs a task and caches the result in a TaskCache"""

    cache: TaskCache
    task_name: str

    def __init__(self, cache: TaskCache, task_name: str):
        self.cache = cache
        self.task_name = task_name

    def __call__(self, **options: dict) -> Any:
        cache_key = (self.task_name, tuple(sorted(options.items())))
        if cache_key in self.cache.results:
            return self.cache.results[cache_key].return_values

        task_config = self.cache.project_config.tasks[self.task_name]
        task_class = TaskConfig(
            {**task_config, "project_config": self.cache.project_config}
        ).get_class()
        step = StepSpec(
            step_num=StepVersion("1"),
            task_name=self.task_name,
            task_config=task_config,
            task_class=task_class,
            project_config=self.cache.project_config,
        )
        self.cache.flow.callbacks.pre_task(step)
        result = TaskRunner(step, self.cache.flow.org_config, self.cache.flow).run_step(
            **options
        )
        self.cache.flow.callbacks.post_task(step, result)

        self.cache.results[cache_key] = result
        return result.return_values


def flow_from_org_actions(
    name: str,
    description: str,
    group: str,
    project_config: BaseProjectConfig,
    org_actions: List[Union[TaskOrgAction, FlowOrgAction]],
) -> FlowCoordinator:

    steps = []
    step_counter = 1

    flow_step_name = None

    for org_action in org_actions:
        if isinstance(org_action, TaskOrgAction):
            flow_step_name = org_action.name
            # Handle single task
            steps.append(
                stepspec_from_task_action(project_config, org_action, step_counter)
            )
            step_counter += 1
        elif isinstance(org_action, FlowOrgAction):
            flow_step_name = f"{org_action.name}@{org_action.hash_action}"
            # Handle flow (multiple tasks)
            for step_action in org_action.steps:
                steps.append(
                    stepspec_from_task_action(
                        project_config,
                        step_action.task,
                        step_counter,
                        from_flow=flow_step_name,
                    )
                )
                step_counter += 1

    return FlowCoordinator.from_steps(
        project_config,
        steps,
        name="Composite Replay Flow",
    )


def stepspec_from_task_action(
    project_config: BaseProjectConfig,
    task_action: TaskOrgAction,
    step_num: int,
    from_flow: Optional[str] = None,
) -> StepSpec:
    task_config = {
        "class_path": task_action.class_path,
        "options": task_action.options.copy(),
    }

    # Handle special case of mapping resolved dependencies back to runnable ones
    runnable_dependencies = task_action.get_runnable_dependencies(project_config)
    if runnable_dependencies:
        task_config["options"] = task_config.get("options", {})
        task_config["options"]["dependencies"] = runnable_dependencies

    try:
        task_class = project_config.get_task(task_action.name).get_class()
    except (ImportError, AttributeError, TaskImportError) as e:
        raise FlowConfigError(f"Task named {task_action.name} has bad classpath, {e}")

    return StepSpec(
        step_num=StepVersion(str(step_num)),
        task_name=task_action.name,
        task_config=task_config,
        task_class=task_class,
        project_config=project_config,
        allow_failure=task_config.get("ignore_failure", False),
        from_flow=from_flow,
        when=task_config.get("when"),
    )
