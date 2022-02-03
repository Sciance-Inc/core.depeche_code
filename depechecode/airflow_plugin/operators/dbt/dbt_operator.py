#! /usr/bin/python3

# dbt_operator.py
#
# Project name: DepecheCode
# Author: Hugo Juhel
#
# description:
"""
    Implements various DBT executor. One for each verb.
    The Operator wraps a CLI call to DBT

    This is mainly (only) done to run DBT without having to relays on the unstable Python API : https://docs.getdbt.com/docs/running-a-dbt-project/dbt-api
"""

#############################################################################
#                                 Packages                                  #
#############################################################################

from abc import ABCMeta
from typing import List
from airflow.utils.decorators import apply_defaults
from depechecode.airflow_plugin.operators.bash import VenvBashOperator
from depechecode.logger import get_module_logger

#############################################################################
#                                  Script                                   #
#############################################################################

_LOGGER = get_module_logger("DBTOperator")


class _DBTOperator(VenvBashOperator, metaclass=ABCMeta):
    """
    The operator executes a call to a DBT verb through the DBT's CLI.
    The CLI is executed in a Pytjon-virtual-env aware bash
    """

    @staticmethod
    def format_dbt_command_args(
        *args, profiles_dir: str = None, target: str = None
    ) -> str:
        """
        Format the DBT command to be executed agains the venv.

        Args:
            profiles_dir (str, optional): The profiles's directory to use to execute the command with. Defaults to None.
            target (str, optional): The name of the dbt profile's target to use. Defaults to None.

        Returns:
            str: The parsed command
        """

        args_: List[str] = list(args)  # type: ignore
        if profiles_dir:
            args_.extend(["--profiles-dir", profiles_dir])

        if target:
            args_.extend(["--target", target])

        return " ".join(args_)

    @apply_defaults
    def __init__(
        self,
        dbt_command_args: List[str],
        requirements_file_path: str = None,
        python_bin: str = None,
        profiles_dir: str = None,
        target: str = None,
        *args,
        **kwargs,
    ) -> None:
        """
        Return a new _DBTOperator

        Args:
            dbt_command_args (List[str]): [description]
            requirements_file_path (str, optional): [description]. Defaults to None.
            python_bin (str, optional): [description]. Defaults to None.
            working_dir (str, optional): [description]. Defaults to None.
            profiles_dir (str, optional): [description]. Defaults to None.
            target (str, optional): [description]. Defaults to None.
        """

        cmd = _DBTOperator.format_dbt_command_args(
            *dbt_command_args, profiles_dir=profiles_dir, target=target
        )

        _LOGGER.info(f"Formatted DBT command : '{cmd}'")

        super(_DBTOperator, self).__init__(
            *args,
            bash_command=cmd,
            requirements_file_path=requirements_file_path,
            python_bin=python_bin,
            **kwargs,
        )


class _DBTModelLessOperator(_DBTOperator):
    _VERB: str

    @apply_defaults
    def __init__(self, profiles_dir: str = None, target: str = None, *args, **kwargs):
        """
        Return a new operator executing a call to DBT using the _VERB attribute.
        The command schould'nt be model dependant.
        """

        super(_DBTModelLessOperator, self).__init__(
            dbt_command_args=["dbt", self._VERB],
            profiles_dir=profiles_dir,
            target=target,
            *args,
            **kwargs,
        )


class _DBTModelOperator(_DBTOperator):
    _VERB: str

    @apply_defaults
    def __init__(
        self, model: str, profiles_dir: str = None, target: str = None, *args, **kwargs
    ):
        """
        Return a new operator executing a call to DBT using the _VERB attribute.
        The command schould be model dependant, and the model to use should be indicated through the model attribute.
        """

        super(_DBTModelOperator, self).__init__(
            dbt_command_args=["dbt", self._VERB, "--models", model],
            profiles_dir=profiles_dir,
            target=target,
            *args,
            **kwargs,
        )


class DebugOperator(_DBTModelLessOperator):
    _VERB = "debug"


class CleanOperator(_DBTModelLessOperator):
    _VERB = "clean"


class CompileOperator(_DBTModelLessOperator):
    _VERB = "compile"


class DepsOperator(_DBTModelLessOperator):
    _VERB = "deps"


class SeedOperator(_DBTModelLessOperator):
    _VERB = "seed"


class RunOperator(_DBTModelOperator):
    _VERB = "run"


class TestOperator(_DBTModelOperator):
    _VERB = "test"
