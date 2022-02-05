from pathlib import Path
import shutil
from yaml import safe_load
from dataclasses import dataclass
from typing import Dict
from tempfile import TemporaryDirectory
from git import Git  # type: ignore
from azure.devops.v6_0.git.models import GitVersionDescriptor
import os
from airflow.models import DagRun  # type: ignore
from airflow.utils.state import State


from depechecode.virtual_env import TempVenv, execute_in_subprocess
from depechecode.logger import get_module_logger

_LOGGER = get_module_logger()
_DAGBAG = (
    Path(os.environ.get("AIRFLOW_HOME", "/opt/airflow"))
    / "dags/depechecode_deployments/"
)
_DAGBAG.mkdir(parents=True, exist_ok=True)


@dataclass
class _Deployment:
    """
    Reprensents the deployment config to be applied by DepecheCode.
    """

    branch_name: str
    repo_name: str
    configuration: Dict
    dag_id: str


def get_deployment_config(repo, client):
    """
    Check if the depeche code file is present either in main/master or develop

    Args:
        repo: The repository to inspect.
    """

    def _(branch):
        """Check if one of the items contains the depeche_code.yml flag"""

        # Try to fetch the content of a branch that might not exists
        version = GitVersionDescriptor(branch)
        try:
            items = client.get_items(
                repo.id, recursion_level="full", version_descriptor=version
            )
        except BaseException:
            return None

        # Parse the content of the depeche code file if present (only consider the first one)
        for item in items:
            if item.path.endswith(("depechecode.yaml", "depechecode.yml")):
                blob = client.get_blob_content(repo.id, item.object_id)
                data = safe_load(next(blob))

                return data

        return None

    for branche in ("main", "develop", "master"):
        data = _(branche)
        if data:
            # By convention the DAG_Id is a base name suffixed by the branch
            dag_id = data["deployment"]["DAG_id_base_name"] + branche
            yield _Deployment(
                branche, repo_name=repo.name, configuration=data, dag_id=dag_id
            )


def apply_dbt_deployment(deployment: _Deployment):
    """
    Create a DBT deployment from the deplyoment specifications

    Args:
        deployment (_Deployment): The deployment specifications to use for the deploy.
    """
    _LOGGER.info(
        f"\U0001F4E6 : Creating a deployment for {deployment.repo_name}/{deployment.branch_name}..."
    )
    with TemporaryDirectory() as artifact:

        artifact = Path(artifact)

        for repo in deployment.configuration["deployment"]["composed_of"]:
            Git(artifact).clone(repo, branch=deployment.branch_name)

        # Extract the entrypoint and prepare the entrypoint to extract the commands
        entrypoint = deployment.configuration["deployment"]["entrypoint"]
        cwd = artifact / entrypoint

        # Create the requirements.txt files from the Poetry context
        cmd = [
            "poetry",
            "export",
            "--without-hashes",
            "-o",
            "depechecode/requirements.txt",
        ]

        _LOGGER.debug("Creating the requirements file")
        execute_in_subprocess(cmd, cwd=cwd)

        # Create a new virtual environment in which install dependencies
        with TempVenv(
            requirements_file_path="depechecode/requirements.txt", cwd=cwd
        ) as venv:

            # Execute the dbt deps && dbt compile in the entrypoint : The DBT seed must have been executed once.
            for verb in ["clean", "deps", "compile"]:
                cmd = [
                    "dbt",
                    verb,
                    "--profiles-dir",
                    "depechecode/profiles",
                    "--target",
                    deployment.branch_name,
                ]
                execute_in_subprocess(cmd, cwd=cwd, env=venv.get_prepended_env)

        runs = len(DagRun.find(dag_id=deployment.dag_id, state=State.RUNNING)) + len(
            DagRun.find(dag_id=deployment.dag_id, state=State.QUEUED)
        )
        if runs > 0:
            _LOGGER.info(
                f"\U0001F525 : ...abording the deployment of '{deployment.dag_id}' has the DAG is already running (looking at you side-effect !)."
            )
            return

        # Move the whole folder at once :
        dst = _DAGBAG / Path(deployment.repo_name) / Path(deployment.branch_name)
        shutil.move(str(artifact), str(dst))

        _LOGGER.info(
            f"\U0001F389 : ... '{deployment.dag_id}' has been automagically deployed."
        )


if __name__ == "__main__":
    from azure.devops.connection import Connection
    from msrest.authentication import BasicAuthentication

    credentials = BasicAuthentication("", "xab75ubrov6uv2l6gcrpbdmdp3elneaami6jfsmknjs4y4doxo2a")  # type: ignore
    connection = Connection(
        base_url="https://dev.azure.com/Centre-Expertise-IA", creds=credentials
    )
    git_client = connection.clients.get_git_client()
    get_repos_response = git_client.get_repositories("COTRA-CE")

    for repo in get_repos_response:
        _LOGGER.debug(f"Gazing into {repo.name}, scrutinizingly.")
        for deployment in get_deployment_config(repo, git_client):
            apply_dbt_deployment(deployment)
