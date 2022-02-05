#! /usr/bin/python3

# dbt_deployment_operator.py
#
# Project name: DepecheCode
# Author: Hugo Juhel
#
# description:
"""
    Implements an Operator to deploy a DBT project hosted with Azure.
"""

#############################################################################
#                                 Packages                                  #
#############################################################################

from airflow.utils.decorators import apply_defaults
from airflow.models.baseoperator import BaseOperator
from depechecode.logger import get_module_logger
from azure.devops.connection import Connection
from msrest.authentication import BasicAuthentication

from depechecode.airflow_plugin.operators.deployment.serendipity.azure_interactor import (
    get_deployment_config,
    apply_dbt_deployment,
)

#############################################################################
#                                  Script                                   #
#############################################################################

_LOGGER = get_module_logger("DBTDeploymentOperator")


class AzureDBTDeploymentOperator(BaseOperator):
    """
    Fetch repo from Azure and apply DBT operators.
    """

    @apply_defaults
    def __init__(
        self,
        ado_PAT: str = None,
        dag_bag_path: str = None,
        organization_url: str = None,
        project_name: str = None,
        *args,
        **kwargs,
    ) -> None:
        """
        Create the operator.

        Args:
            git_PAT (str, optional): The Personal Access Token to use to fetch repos from github. Defaults to None.
            dag_bag_path (str, optional): The path to the Airflow's dag bag. Defaults to None.
            organisation_url (str, optional): The URL to the Azure DevOps of your organisation
            project_name: (str, optional): The project's name friendly name.
        """

        super(AzureDBTDeploymentOperator, self).__init__(
            *args,
            **kwargs,
        )

        self._ado_PAT = ado_PAT
        self._dag_bag_path = dag_bag_path
        self._organization_url = organization_url
        self._project_name = project_name

    def execute(self, context):
        """
        Fetch and build new DAGs from Azure
        """

        # Fill in with your personal access token and org URL
        # personal_access_token = "xab75ubrov6uv2l6gcrpbdmdp3elneaami6jfsmknjs4y4doxo2a"
        # organization_url = "https://dev.azure.com/Centre-Expertise-IA"

        # Create the git client to use
        credentials = BasicAuthentication("", self._ado_PAT)  # type: ignore
        connection = Connection(base_url=self._organization_url, creds=credentials)
        git_client = connection.clients.get_git_client()
        get_repos_response = git_client.get_repositories(self._project_name)

        for repo in get_repos_response:
            _LOGGER.debug(f"\U0001F440 : gazing into {repo.name}, scrutinizingly.")
            for deployment in get_deployment_config(repo, git_client):
                apply_dbt_deployment(deployment)
