#! /usr/bin/python3

# compile.py
#
# Project name: DepecheCode
# Author: Hugo Juhel
#
# description:
"""
    Prepare a clean DBT folder environement
"""

#############################################################################
#                                 Packages                                  #
#############################################################################

from abc import ABCMeta
from typing import List
from airflow.utils.decorators import apply_defaults
from depechecode.virtual_env import TempVenv, execute_in_subprocess
from depechecode.logger import get_module_logger

#############################################################################
#                                  Script                                   #
#############################################################################

_LOGGER = get_module_logger("Compile")


class _Compile:
    """
    Prepare a clean environement for dbt commands to run
    """

    def format_dbt_command_args(self, *args) -> List[str]:
        """
        Format the DBT command to be executed agains the venv.

        Args:
            profiles_dir (str, optional): The profiles's directory to use to execute the command with. Defaults to None.
            target (str, optional): The name of the dbt profile's target to use. Defaults to None.

        Returns:
            str: The parsed command
        """

        args_: List[str] = list(args)  # type: ignore
        if self._profiles_dir:
            args_.extend(["--profiles-dir", self._profiles_dir])

        if self._target:
            args_.extend(["--target", self._target])

        return args_

    def __init__(
        self,
        requirements_file_path: str = None,
        python_bin: str = None,
        profiles_dir: str = None,
        target: str = None,
        *args,
        **kwargs
    ):
        """
        Prepare a new cleanup environment for the parser.

        Args:
            requirements_file_path (str): [description]
            python_bin (str, optional): [description]. Defaults to None.
            cwd ([type], optional): [description]. Defaults to None.
        """
        self._profiles_dir = profiles_dir
        self._target = target
        self._manager = TempVenv(
            requirements_file_path=requirements_file_path, python_bin=python_bin
        )

    def __enter__(self):
        """
        Run the dbt clean, dbt deps & dbt compile commands in a fresh environement
        """

        self._manager.__enter__()
        clean_command = self.format_dbt_command_args("dbt", "clean")
        deps_command = self.format_dbt_command_args("dbt", "deps")
        compile_command = self.format_dbt_command_args("dbt", "compile")

        for c in [clean_command, deps_command, compile_command]:
            execute_in_subprocess(c)

    def __exit__(self, exc_type, exc_val, exc_tb):

        return self._manager.__exit__(exc_type, exc_val, exc_tb)
