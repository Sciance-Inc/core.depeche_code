# DepecheCode

 _All I ever wanted_

_All I ever needed_

_Is here in my DAG_

_Bash commands are very unnecessary_

_They can only do harm_


# Objectives
__DepecheCode__ aims at making DBT dags deployable on Airflow. The library provides a DBT's dag parser and some dedicated operators to run DBT. T

# Build
> Airflow requires a setupable packages that can be extract from poetry

```bash
poetry build
tar -xvf dist/*.tar.gz --wildcards --no-anchored '*/setup.py' --strip=1
rm -R dist
```