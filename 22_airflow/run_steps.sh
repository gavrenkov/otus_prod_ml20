#!/usr/bin/env bash
# init

export AIRFLOW_HOME=$PWD

# install from pypi using pip
pip install apache-airflow
# initialize the database
airflow initdb
# start the web server, default port is 8080
airflow webserver -p 8080 & airflow scheduler &
# start the scheduler


airflow backfill -s 2020-09-20 -e 2020-10-18 example_2
