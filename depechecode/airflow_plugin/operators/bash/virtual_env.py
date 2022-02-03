#! /usr/bin/python3

# virtual_env.py
#
# Project name: DepecheCode
# Author: Hugo Juhel
#
# description:
"""
    Implement helpers to create and manage virtual environements.
"""

#############################################################################
#                                 Packages                                  #
#############################################################################

import sys
import shlex
import subprocess
from tempfile import TemporaryDirectory
from typing import List, Optional
from pathlib import Path

from depechecode.logger import get_module_logger

#############################################################################
#                                  Script                                   #
#############################################################################

_LOGGER = get_module_logger()


def _execute_in_subprocess(cmd: List[str]):
    """
    Execute a process and stream output to logger

    Args:
        cmd (List[str]): The array of arguments.
    """

    _LOGGER.info(f"Executing cmd: {' '.join(shlex.quote(c) for c in cmd)}")
    with subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=0, close_fds=True
    ) as proc:
        _LOGGER.info("Output:")
        if proc.stdout:
            with proc.stdout:
                for line in iter(proc.stdout.readline, b""):
                    _LOGGER.info(f"{line.decode().rstrip()}")

        exit_code = proc.wait()
    if exit_code != 0:
        raise subprocess.CalledProcessError(exit_code, cmd)


def _generate_virtualenv_cmd(tmp_dir, python_bin, system_site_packages):
    """
    Build the virtualenv creation command.

    Args:
        tmp_dir (str): The tempdir in which the environement will be store.
        python_bin (str): The python version to be used in the environement.
        system_site_packages (bool): Whether to include system_site_packages in the environement. Default to False.

    Returns:
        List[str]: The command to be executed, as an array of strings.
    """

    cmd = [sys.executable, "-m", "virtualenv", tmp_dir]
    if system_site_packages:
        cmd.append("--system-site-packages")
    if python_bin is not None:
        cmd.append(f"--python={python_bin}")
    return cmd


def _generate_pip_install_cmd_from_file(tmp_dir, requirements_file_path) -> List[str]:
    """
    Build the Pip command to install from the path.

    Args:
        tmp_dir (str): The tempdir in which the environement will be store.
        requirements_file_path (str): The path to the requirements file to use for the environement. Defaults to None.

    Returns:
        List[str]: THe command to be executed.
    """

    cmd = [f"{tmp_dir}/bin/pip", "install", "-r"]
    return cmd + [requirements_file_path]


def _prepare_virtualenv(
    venv_directory: str,
    python_bin: str = None,
    system_site_packages: bool = False,
    requirements_file_path: Optional[str] = None,
) -> str:
    """

    Args:
        venv_directory (str): Where to create and store the venv.
        python_bin (str): Path to the python executable. Uses the default python if False. Defaults to None.
        system_site_packages (bool): Whether to include system_site_packages in the environement. Default to False.
        requirements_file_path (Optional[str], optional): The path to the requirements file to use for the environement. Defaults to None.

    Returns:
        str: The path to the newly created environement.
    """

    virtualenv_cmd = _generate_virtualenv_cmd(
        venv_directory, python_bin, system_site_packages
    )
    _execute_in_subprocess(virtualenv_cmd)

    pip_cmd = None
    if requirements_file_path:
        pip_cmd = _generate_pip_install_cmd_from_file(
            venv_directory, requirements_file_path
        )

    if pip_cmd:
        _execute_in_subprocess(pip_cmd)

    _LOGGER.info(f"Successfully created the Virtual environement to '{venv_directory}'")

    return venv_directory


class TempVenv(TemporaryDirectory):
    """
    Context manager for the 'on-the-fly' creation of a Python virtual env.
    """

    def __init__(
        self,
        *,
        requirements_file_path=None,
        python_bin: str = None,
        suffix: str = None,
        prefix: str = "airflow_venv",
        dir=None,
    ):
        """
        Create the context manager.

        Args:
            requirements_file_path ([type], optional): An optional path to a set of dependencies to be installed. Defaults to None.
            python_bin (str, optional): An optional base Python binary for the Venv. Defaults to None.
            suffix (str, optional): Same as tempfile.TemporaryDirectory. Defaults to None.
            prefix (str, optional): Same as tempfile.TemporaryDirectory. Defaults to "airflow_venv".
            dir ([type], optional): Same as tempfile.TemporaryDirectory. Defaults to None.
        """

        super().__init__(
            suffix=suffix,
            prefix=prefix,
            dir=dir,
        )  # type: ignore

        self._requirements_file_path = requirements_file_path
        self._python_bin = python_bin

    @property
    def bin_path(self) -> str:
        """
        Get the path to the python binary of the venv.

        Returns:
            str: The path to the venv
        """

        return str(Path(self.name) / "bin")

    def __enter__(self):
        """
        Create and configure the virtual env.
        """

        _prepare_virtualenv(
            venv_directory=self.name,
            python_bin=self._python_bin,
            requirements_file_path=self._requirements_file_path,
        )

        return None
