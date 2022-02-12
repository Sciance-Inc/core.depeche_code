#! /usr/bin/python3

# dbt_deployment_operator.py
#
# Project name: DepecheCode
# Author: Hugo Juhel
#
# description:
"""
    Implements an Operator to deploy a DBT project hosted on S3 / Minio.
"""

#############################################################################
#                                 Packages                                  #
#############################################################################

import os
from typing import Any
from tempfile import TemporaryDirectory
from pathlib import Path
from shutil import unpack_archive, rmtree, copytree
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.decorators import apply_defaults
from airflow.models.baseoperator import BaseOperator
from airflow.models import DagRun  # type: ignore
from airflow.utils.state import State
from depechecode.logger import get_module_logger


#############################################################################
#                                  Script                                   #
#############################################################################

_LOGGER = get_module_logger("DBTDeploymentOperator")
_DAGBAG = Path(os.environ.get("AIRFLOW_HOME", "/opt/airflow")) / "dags/depechecode/"
_DAGBAG.mkdir(parents=True, exist_ok=True)
_DAGBAG = str(_DAGBAG)


class S3DBTDeploymentOperator(BaseOperator):
    """
    Fetch artifacts from s3, packaged with DepecheCode and deploy them..
    """

    @apply_defaults
    def __init__(
        self,
        dag_bag: str = _DAGBAG,
        deployments_connection_id: str = "minio_svc",
        deployments_bucket: str = "deployments",
        *args,
        **kwargs,
    ) -> None:

        super(S3DBTDeploymentOperator, self).__init__(
            *args,
            **kwargs,
        )

        self._dag_bag = Path(dag_bag)
        self._deployments_connection_id = deployments_connection_id
        self._deployments_bucket = deployments_bucket

    def execute(self, context: Any):
        """
        Execute the deployment
        """

        # A tempdir to store extracted archive
        with TemporaryDirectory() as staging:

            # A tempdir to store downloaded artifacts
            with TemporaryDirectory() as dll:

                s3 = S3Hook(self._deployments_connection_id)

                # Fetch each deployments targets
                for key in s3.list_keys(bucket_name=self._deployments_bucket):

                    # Split the name, to get the extention
                    frm = Path(key).suffix[1:]
                    deployment_name = Path(key).stem

                    archive_path = s3.download_file(
                        key=key, bucket_name=self._deployments_bucket, local_path=dll
                    )

                    # The path the artifact has been download
                    from_ = Path(dll) / archive_path
                    # The staging dir we want to extract the artifact to
                    to_ = Path(staging) / deployment_name

                    # unpack the archive
                    unpack_archive(from_, to_, format=frm)

                    # Move the Folder
                    runs = len(
                        DagRun.find(dag_id=deployment_name, state=State.RUNNING)
                    ) + len(DagRun.find(dag_id=deployment_name, state=State.QUEUED))

                    if runs > 0:
                        _LOGGER.info(
                            f"\U0001F525 : ...abording the deployment of '{deployment_name}' as the DAG is already running (looking at you DBT-side-effect !)."
                        )
                        continue

                    # Move the whole folder at once :
                    # TODO : add support for other python bin, migrates to 3.8 and uses copy tree
                    dst = self._dag_bag / Path(deployment_name)
                    rmtree(dst, ignore_errors=True)
                    _LOGGER.info(f"Moving deployment to : {str(dst)}")
                    copytree(to_, str(dst))

                    _LOGGER.info(
                        f"\U00002728 \U0001F370 \U00002728 : ... '{deployment_name}' has been automagically deployed."
                    )
