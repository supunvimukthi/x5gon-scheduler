import os, sys
sys.path.insert(0, os.path.abspath("."))
import time
import json
import utils.ScheduleStatus as ScheduleStatus

from datetime import datetime

from utils.DAG import create_dag
from utils.database import create_new_job, update_job_status, update_error, update_retry_count, get_values, get_pending_tasks, \
    insert_values, get_dag_data

in_memory_dags = {}


def get_dag(dag_id):
    if dag_id not in in_memory_dags:
        dag_data = get_dag_data(dag_id)
        in_memory_dags[dag_id] = create_dag(dag_data)

    return in_memory_dags[dag_id]


# limitations: have to define all parameters in request mapping and response mapping
def run():
    start_time = time.perf_counter()
    while True:
        # Run scheduler every 10 second for now
        if time.perf_counter() < (start_time + 10):
            continue

        start_time = time.perf_counter()
        print('running once...')
        # get all pending tasks
        pending_tasks = get_pending_tasks()
        for pending_task in pending_tasks:
            try:
                # get the relevant dag or construct it
                DAG = get_dag(pending_task[6])

                # get http operator object related to the pending task
                current_task = DAG.nodes[pending_task[1]]['http_operator']

                # retrieving data related to the http operator
                required_keys = list(current_task.request_mapping.keys())
                required_data = {}
                if len(required_keys) > 0:
                    required_data = get_values(pending_task[0], required_keys)

                # execute the http request
                result = current_task.execute(required_data)
                if result:
                    # saving data needed for the task from the result dictionary
                    values = []
                    for key in current_task.response_mapping.keys():
                        if key in result:
                            values.append((
                                pending_task[0],
                                current_task.response_mapping[key],
                                json.dumps({'value': result[key]}),
                                pending_task[1],
                                datetime.now().timestamp()
                            ))

                    insert_values(values)

                    # accessing all downstream task to this task
                    for next_task_id in DAG.successors(pending_task[1]):
                        delay_time = DAG.nodes[next_task_id]['http_operator'].delay
                        # scheduling the task
                        create_new_job(pending_task[0], next_task_id, pending_task[6], delay_time)

                    # update the current task as completed
                    update_job_status(pending_task[0], pending_task[1], ScheduleStatus.COMPLETED)
                else:
                    # update status as skipped since operator did not satisfy the condition
                    update_job_status(pending_task[0], pending_task[1], ScheduleStatus.SKIPPED)
            except Exception as e:
                print(e)
                # if retry count is exceeding the max retry count update status as failed
                if pending_task[4] == current_task.max_retry_count:
                    update_error(pending_task[0], pending_task[1], str(e))
                # increase the retry count and keep status as pending to try again
                else:
                    update_retry_count(pending_task[0], pending_task[1], pending_task[4] + 1)
                    update_job_status(pending_task[0], pending_task[1], ScheduleStatus.PENDING)


run()
