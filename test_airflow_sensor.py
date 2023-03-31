import logging
from datetime import timedelta, datetime

from utils.airflow_sensor import WaitForAirflowDag, format_run_id, get_most_recent_run_id

from flytekit import task, workflow
from flytekit import TaskMetadata, task, workflow


logging.basicConfig(level=logging.INFO)

@task
def get_wf_run_dt() -> str:
    return datetime.now().strftime("%Y-%m-%d")
    # return '2023-03-21'


@task
def downstream_task() -> None:
    print(f'airflow dag succeeded.')


@workflow
def wf() -> None:
    sensor = WaitForAirflowDag(
        name="wait-for-dag-1",
        metadata=TaskMetadata(retries=5),
        poll_interval=timedelta(seconds=60),
    )

    t1 = sensor(
        dag_id='dag-1', 
        run_id=get_most_recent_run_id(cron_schedule='*/30 * * * *'),
        task_id='task-2'
    )
    t2 = downstream_task()
    t1 >> t2


if __name__ == "__main__":
    dt = datetime.now().strftime("%Y-%m-%d")
    run_id = format_run_id.format(dt=dt, tm='07:00:00')
    print(run_id)

