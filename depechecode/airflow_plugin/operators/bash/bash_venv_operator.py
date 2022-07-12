#! /usr/bin/python3

# bash_venv_operator.py
#
# Project name: DepecheCode
# Author: Hugo Juhel
#
# description:
"""
    Implements a Python virtual-env aware Bash.
    A Python environment is created on the fly to run the bash command.
    The Operator is used to wrap a Python CLI program and executes it in a specific environement.

    This is mainly (only) done to run DBT without having to relays on the unstable Python API : https://docs.getdbt.com/docs/running-a-dbt-project/dbt-api
"""

#############################################################################
#                                 Packages                                  #
#############################################################################

import os
from pathlib import Path
from airflow.operators.bash import BashOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.operator_helpers import context_to_airflow_vars
from depechecode.virtual_env import TempVenv

#############################################################################
#                                  Script                                   #
#############################################################################


class VenvBashOperator(BashOperator):
    """
    The operator is a Python virtual-env aware bash.
    The operator insert the virtual environement"s path into the Path variables.
    """

    @apply_defaults
    def __init__(
        self,
        task_id: str,
        bash_command: str,
        requirements_file_path: str = None,
        python_bin: str = None,
        cwd: str = None,
        *args,
        **kwargs,
    ) -> None:
        """
        Return a new VenvBashOperator

        Args:
            bash_command (str): The bash command to executes in the Python aware bash.
            requirements_file_path (str, optional): A requirements.txt file (no-hashes) to use to bootrsrap the environement. Defaults to None.
            python_bin (str, optional): The base Python binary to use for the environement. Defaults to None.
        """

        super(VenvBashOperator, self).__init__(
            bash_command=bash_command, cwd=cwd, task_id=task_id, *args, **kwargs
        )

        if cwd and requirements_file_path:
            requirements_file_path = str(Path(cwd) / Path(requirements_file_path))

        self._manager_factory = lambda: TempVenv(
            requirements_file_path=requirements_file_path, python_bin=python_bin
        )

        self._bin_path = None

    def get_env(self, context):
        """
        Builds the set of environment variables to be exposed for the bash command.
        Intercept the definition of the path and insert the path to the Venv.
        Called by the parent, in the execute mmethod
        """

        # Fetch all the environement to be sure to get the path and override it with the environment given at the Operator instanciation if any
        override = self.env or {}
        env = {**os.environ.copy(), **override}

        # Add the newyly created virtual python to the path
        virtual_env_path = self._bin_path
        if virtual_env_path is None:
            raise ValueError("bin path is only available for an active manager.")
        env["PATH"] = virtual_env_path + os.pathsep + env["PATH"]

        # Add the contextual variables from Airflow
        airflow_context_vars = context_to_airflow_vars(context, in_env_var_format=True)
        self.log.debug(
            "Exporting the following env vars:\n%s",
            "\n".join(f"{k}={v}" for k, v in airflow_context_vars.items()),
        )
        env.update(airflow_context_vars)
        return env

    def execute(self, context):
        """
        Execute a bash command in a dedicated Python Virtual Environement.
        """

        # Create the virtual enviroment before executing the Task.
        with self._manager_factory() as mng:
            self._bin_path = mng.bin_path
            super().execute(context)
