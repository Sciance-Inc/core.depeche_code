#! /usr/bin/python3

# dbt_parser.py
#
# Project name: DepecheCode
# Author: Hugo Juhel
#
# description:
"""
    Airflow's DAG parser and builder for DBT
"""

#############################################################################
#                                 Packages                                  #
#############################################################################

import json
from pathlib import Path

from airflow.utils.task_group import TaskGroup
from airflow.utils.decorators import apply_defaults

from depechecode.airflow_plugin.operators.dbt import (
    RunOperator,
    TestOperator,
)
from depechecode.logger import MixinLogable

#############################################################################
#                                 Packages                                  #
#############################################################################


class DBTAutoDag(MixinLogable):
    """
    An automatic parser for a DBT manifest.
    The class extracts the between models dependencies and convert them to DepecheCode's dbt operators.

    A utility class that parses out a dbt project and creates the respective task groups
    Args:
        dag: The Airflow DAG
        dbt_global_cli_flags: Any global flags for the dbt CLI
        dbt_project_dir: The directory containing the dbt_project.yml
        dbt_profiles_dir: The directory containing the profiles.yml
        dbt_target: The dbt target profile (e.g. dev, prod)
        dbt_run_group_name: Optional override for the task group name.
        dbt_test_group_name: Optional override for the task group name.
    """

    _PATH_TO_TARGET = "target/manifest.json"

    @apply_defaults
    def __init__(
        self,
        dag,
        working_dir: str,
        profiles_dir: str = None,
        target: str = None,
        requirements_file_path: str = None,
        run_group_name="dbt_run",
        test_group_name="dbt_test",
        *args,
        **kwargs,
    ):

        super().__init__(logger_name="DBTDagParser")

        self._dag = dag
        self._working_dir = working_dir
        self._profiles_dir = profiles_dir
        self._target = target
        self._requirements_file_path = requirements_file_path

        self._args = args
        self._kwargs = kwargs

        # Prepare groups to gather tasks.
        self._run_group: TaskGroup = None  # type: ignore

    def load_dbt_manifest(self):
        """
        Read the DBT manifest.
        Returns: A JSON object containing the dbt manifest content.
        """

        try:
            path = Path(self._working_dir) / Path(self._PATH_TO_TARGET)
            with open(path) as f:
                file_content = json.load(f)
        except BaseException as error:
            raise IOError(
                f"Failed to read the DBT s project manifest : {path}"  # type: ignore
            ) from error

        return file_content

    def __call__(self):
        """
        Parse out the content of the DBT manifest and build the Task
        Returns: None
        """

        manifest = self.load_dbt_manifest()
        tasks = {}
        with TaskGroup(group_id="run_group") as tg1:

            # Create the tasks for each model
            for node_name in manifest["nodes"].keys():
                if node_name.split(".")[0] == "model":

                    # Make the run nodes
                    tasks[node_name] = RunOperator(
                        task_group=self._run_group,
                        task_id=node_name,
                        model=node_name.split(".")[-1],
                        cwd=self._working_dir,
                        requirements_file=self._requirements_file_path,
                        profiles_dir=self._profiles_dir,
                        target=self._target,
                        dag=self._dag,
                    )

                    # # Make the test nodes
                    # node_test = node_name.replace("model", "test")
                    # tasks[node_test] = TestOperator(
                    #     task_group=self._test_group,
                    #     task_id=node_test,
                    #     model=node_test.split(".")[-1],
                    #     cwd=self._working_dir,
                    #     requirements_file=self._requirements_file_path,
                    #     profiles_dir=self._profiles_dir,
                    #     target=self._target,
                    #     dag=self._dag,
                    # )

            # Add upstream and downstream dependencies for each run task
            for node_name in manifest["nodes"].keys():
                if node_name.split(".")[0] == "model":
                    for upstream_node in manifest["nodes"][node_name]["depends_on"][
                        "nodes"
                    ]:
                        upstream_node_type = upstream_node.split(".")[0]
                        if upstream_node_type == "model":
                            tasks[upstream_node] >> tasks[node_name]

                    self.info(f"Created node {node_name}")

            self._run_group = tg1

    @property
    def run_group(self):
        return self._run_group

    # @property
    # def test_group(self):
    #     return self._test_group
