#! /usr/bin/python3

# __init__.py
#
# Project name: DepecheCode
# Author: Hugo Juhel
#
# description:
"""
    Airflow's plugin entrypoint
    
    Register the depechecode plugins
"""

#############################################################################
#                                 Packages                                  #
#############################################################################

from airflow.plugins_manager import AirflowPlugin
from depechecode.airflow_plugin.operators.dbt import (
    DebugOperator,
    CleanOperator,
    CompileOperator,
    DepsOperator,
    SeedOperator,
    RunOperator,
    TestOperator,
)

from depechecode.airflow_plugin.operators.deployment import AzureDBTDeploymentOperator

#############################################################################
#                                  Script                                   #
#############################################################################


class DepecheCodePlugin(AirflowPlugin):
    """
    Namespace for the Depeche Code plugins.
    """

    name = "DepecheCode"
    operattors = [
        DebugOperator,
        CleanOperator,
        CompileOperator,
        DepsOperator,
        SeedOperator,
        RunOperator,
        TestOperator,
        AzureDBTDeploymentOperator,
    ]
