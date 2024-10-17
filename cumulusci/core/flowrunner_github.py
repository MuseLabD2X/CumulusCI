import os
from logging import getLogger
from cumulusci.core.exceptions import CumulusCIFailure
from cumulusci.core.flowrunner import FlowCallback, FlowCoordinator

class GitHubSummaryCallback(FlowCallback):
    def __init__(self):
        self.logger = getLogger(__name__)
        self.step_summary_file = os.environ.get("GITHUB_STEP_SUMMARY")
        self.logger.info(f"Step Summary File: {self.step_summary_file}")
        self.job_summary_file = os.environ.get("GITHUB_JOB_SUMMARY")
        self.logger.info(f"Job Summary File: {self.job_summary_file}")

    def _append_to_summary(self, content, summary_type="step"):
        file_path = self.step_summary_file if summary_type == "step" else self.job_summary_file
        if file_path:
            with open(file_path, "a") as f:
                self.logger.info(f"Appending to {file_path}: {content}")
                f.write(content + "\n")

    def pre_flow(self, coordinator: FlowCoordinator):
        self.coordinator = coordinator
        flow_header = f"# 🔄 Flow: {coordinator.name or 'Unnamed Flow'}\n"
        self._append_to_summary(flow_header, "job")

    def post_flow(self, coordinator: FlowCoordinator):
        self._generate_job_summary(coordinator)

    def pre_task(self, step):
        self._append_to_summary(f"\n## 🔹 Task: {step.task_name}", "step")

    def post_task(self, step, result):
        status_emoji = "✅" if result.exception is None else "❌"
        self._append_to_summary(f"{status_emoji} {step.task_name} - {result.result}", "step")

    def _generate_job_summary(self, coordinator: FlowCoordinator):
        overall_status = "✅ Success" if coordinator.action and coordinator.action.status == "success" else "❌ Failure"
        self._append_to_summary(f"\n## Overall Status: {overall_status}", "job")

        self._add_org_info()
        self._add_action_summary()
        self._add_error_summary()

    def _add_org_info(self):
        org_config = self.coordinator.org_config
        if self.coordinator.requires_org:
            self._append_to_summary("\n## 🌐 Org Information", "job")
            self._append_to_summary(f"- **Username**: {org_config.username}", "job")
            self._append_to_summary(f"- **Org ID**: {org_config.org_id}", "job")
            self._append_to_summary(f"- **Instance**: {org_config.instance_name}", "job")
        elif org_config is not None:
            self._append_to_summary("\n## 🌐 Org Information", "job")
            self._append_to_summary("- **Scratch Org Profile**: {org_config.name}", "job")
            self._append_to_summary("- **Config File**: {org_config.config_file}", "job")

    def _add_action_summary(self):
        self._append_to_summary("\n## 📊 Action Summary", "job")
        for result in self.coordinator.results:
            status_emoji = "✅" if result.exception is None else "❌"
            self._append_to_summary(f"- {status_emoji} **{result.task_name}**", "job")
            if result.return_values:
                self._append_to_summary("  ```", "job")
                for line in str(result.return_values).split('\n'):
                    self._append_to_summary(f"  {line}", "job")
                self._append_to_summary("  ```", "job")

    def _add_error_summary(self):
        failures = []
        errors = []

        for result in self.coordinator.results:
            if result.exception:
                if isinstance(result.exception, CumulusCIFailure):
                    failures.append((result.task_name, result.exception))
                else:
                    errors.append((result.task_name, result.exception))

        if failures:
            self._append_to_summary("\n## ❗ Failures", "job")
            for task_name, exception in failures:
                self._append_to_summary(f"- **{task_name}**: {str(exception)}", "job")

        if errors:
            self._append_to_summary("\n## 🚨 Errors", "job")
            for task_name, exception in errors:
                self._append_to_summary(f"- **{task_name}**: {str(exception)}", "job")