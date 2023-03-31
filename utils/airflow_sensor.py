import os
import requests
import boto3
import json

from typing import Tuple, Any
import logging
from datetime import timedelta, datetime
from time import sleep
from croniter import croniter

from flytekit.extend import Interface
from flytekit import PythonInstanceTask, task


AIRFLOW_API_URL = {
    'dev': 'http://localhost:8080/api/v1',
    'beta': 'http://localhost:8080/api/v1',
    'prod': 'http://localhost:8080/api/v1',
}

AIRFLOW_API_SECRET_ID = {
    'dev': 'dev-airflow-api-creds',
    'beta': 'beta-airflow-api-creds',
    'prod': 'prod-airflow-api-creds'
}


logging.basicConfig(level=logging.INFO)
run_id_template = "scheduled__{dt}T{tm}+00:00"

def get_domain() -> str:
    res = os.getenv("FLYTE_INTERNAL_TASK_DOMAIN", "dev")
    return 'dev' if res.startswith('dev') else 'prod' if res.startswith('prod') else res

@task
def format_run_id(dt: str, tm: str) -> str:
    res = run_id_template.format(dt=dt, tm=tm)
    logging.info(f'format_run_id() returns: {res}')
    return res

@task
def get_most_recent_run_id(cron_schedule: str) -> str:
    # Must call iter.get_prev() twice due to how Airflow run_id uses the previous schedule, not the most recent.
    iter = croniter(cron_schedule)
    iter.get_prev(datetime)
    dt = iter.get_prev(datetime)
    logging.info(f'dt: {dt.strftime("%Y-%m-%d")} & tm: {dt.strftime("%H:%M:%S")}')
    res = format_run_id(dt=dt.strftime("%Y-%m-%d"), tm=dt.strftime("%H:%M:%S"))
    return res


class WaitForAirflowDag(PythonInstanceTask):
    _DAG_ID: str = "dag_id"
    _RUN_ID: str = "run_id"
    _TASK_ID: str = "task_id"
    _DAG_SUCCESS: bool = "dag_success"

    def __init__(
        self,
        name: str,
        poll_interval: timedelta = timedelta(seconds=10),
        **kwargs,
    ):
        inputs = {
            self._DAG_ID: str, 
            self._RUN_ID: str, 
            self._TASK_ID: str
        }
        outputs = {
            self._DAG_SUCCESS: bool
        }
        super(WaitForAirflowDag, self).__init__(
            task_type="airflow-dag-sensor",
            name=name,
            task_config=None,
            interface=Interface(inputs=inputs, outputs=outputs),
            **kwargs,
        )
        self._poll_interval = poll_interval

    def execute(self, **kwargs) -> Any:
        dag_id = kwargs[self._DAG_ID]
        run_id = kwargs[self._RUN_ID]
        task_id = kwargs[self._TASK_ID]
        retries = self._metadata.retries
        while retries > 0:
            logging.info(f"Sensing Airflow dag_id [{dag_id}], run_id [{run_id}], and task_id [{task_id}] ...")
            logging.info(f'retries left: {retries}')
            if is_dagrun_complete(dag_id=dag_id, run_id=run_id, task_id=task_id):
                logging.info(f"Sensed dag success!")
                return True
            timestr = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            logging.warning(f"Sensed dag NOT succeeded yet. Going into sleep for {self._poll_interval.seconds} seconds at {timestr}")
            retries -= 1
            sleep(self._poll_interval.seconds)

        raise RuntimeError(f'WaitForAirflowDag sensor {self.name} has reached max retries of {self._metadata.retries} at poll_interval of {self._poll_interval.seconds} seconds.')


def get_airflow_api_credentials() -> Tuple[str, str]:
    airflow_api_secret_id = AIRFLOW_API_SECRET_ID[get_domain()]
    logging.info(f'airflow_api_secret_id: {airflow_api_secret_id}')
    secrets_manager_client = boto3.client("secretsmanager", region_name="us-east-1")
    response = secrets_manager_client.get_secret_value(SecretId=airflow_api_secret_id)
    secrets_dict = json.loads(response["SecretString"])

    un = secrets_dict['username']
    pwd = secrets_dict['password']
    return un, pwd


def is_dagrun_complete_1(dag_id: str, run_id:str, task_id: str = None) -> bool:
    return False


def is_dagrun_complete(dag_id: str, run_id:str, task_id: str = None) -> bool:
    """
    Check if a given DAG run is complete.
    """
    un, pwd = get_airflow_api_credentials()

    airflow_api_url = AIRFLOW_API_URL[get_domain()]
    print(f'airflow_api_url: {airflow_api_url}')

    if task_id:
        endpoint = f'dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}'
    else:
        endpoint = f'dags/{dag_id}/dagRuns/{run_id}'

    logging.info(f'endpoint: {endpoint}')

    response = requests.get(f'{airflow_api_url}/{endpoint}', auth=(un, pwd),  timeout=10)

    if response.status_code != 200:
        # Request failed for some reason
        msg = f"""Request failed with status code {response.status_code}.
Check whether the dag_id [{dag_id}], run_id [{run_id}], and/or task_id [{task_id}] if provided exists.
"""
        raise Exception(msg)

    dagrun = response.json()
    logging.info(dagrun)

    state = dagrun['state']

    return state == 'success'
