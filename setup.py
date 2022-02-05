# -*- coding: utf-8 -*-
from setuptools import setup

packages = [
    "depechecode",
    "depechecode.airflow_plugin",
    "depechecode.airflow_plugin.operators.bash",
    "depechecode.airflow_plugin.operators.dbt",
    "depechecode.dag_builder",
    "depechecode.wip",
]

package_data = {"": ["*"]}

install_requires = [
    "SQLAlchemy>=1.4.31,<2.0.0",
    "apache-airflow>=2.2.3,<3.0.0",
    "pyodbc>=4.0.32,<5.0.0",
    "sshtunnel>=0.4.0,<0.5.0",
    "virtualenv>=20.13.0,<21.0.0",
]

entry_points = {
    "airflow.plugins": ["DepecheCode = " "depechecode.airflow_plugin:DepecheCodePlugin"]
}

setup_kwargs = {
    "name": "depechecode",
    "version": "0.0.1",
    "description": "",
    "long_description": None,
    "author": "hugo juhel",
    "author_email": "juhel.hugo@stratemia.com",
    "maintainer": None,
    "maintainer_email": None,
    "url": None,
    "packages": packages,
    "package_data": package_data,
    "install_requires": install_requires,
    "entry_points": entry_points,
    "python_requires": ">=3.7,<4.0",
}


setup(**setup_kwargs)
