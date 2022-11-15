import psycopg2
from datetime import timedelta, datetime

from utils.config import db_config
import utils.ScheduleStatus as ScheduleStatus

CREATE_TABLE_SQLS = (
        """
        CREATE TABLE IF NOT EXISTS schedules (
             job_id UUID,
             task_id VARCHAR(255),
             status VARCHAR(255) NOT NULL,
             trigger_time INTEGER NOT NULL,
             retry_count INTEGER,
             error VARCHAR(1000),
             dag_id UUID,
             PRIMARY KEY (job_id, task_id)
             )
        """,
        """ CREATE TABLE IF NOT EXISTS runner_data (
                job_id UUID,
                key VARCHAR(255) NOT NULL,
                value JSON NOT NULL,
                created_by VARCHAR(255),
                created_at INTEGER,
                PRIMARY KEY (job_id, key)
                )
        """,
        """ CREATE TABLE IF NOT EXISTS dag (
                dag_id UUID,
                data JSON NOT NULL,
                username VARCHAR(255) NOT NULL,
                created_at INTEGER,
                PRIMARY KEY (dag_id)
                )
        """,
        """ CREATE TABLE IF NOT EXISTS users (
                username VARCHAR(255) NOT NULL,
                full_name VARCHAR(255) NOT NULL,
                hashed_password VARCHAR(255) NOT NULL,
                email VARCHAR(255),
                PRIMARY KEY (username)
                )
        """)

NEW_JOB_SQL = """INSERT INTO schedules(job_id, task_id, status, trigger_time, retry_count, dag_id)
             VALUES(%s, %s, %s, %s, %s, %s);"""

UPDATE_JOB_STATUS_SQL = """UPDATE schedules
                SET status = %s
                WHERE job_id = %s AND task_id = %s;"""

UPDATE_RETRY_COUNT_SQL = """UPDATE schedules SET retry_count = %s, trigger_time = %s
                        WHERE job_id = %s AND task_id = %s;"""

UPDATE_ERROR_SQL = """UPDATE schedules SET error = %s, status = %s
                        WHERE job_id = %s AND task_id = %s;"""

GET_PENDING_TASKS_SQL = """SELECT * FROM schedules WHERE status = %s AND trigger_time < %s;"""

INSERT_KEY_VALUES_SQL = """INSERT INTO 
    runner_data (job_id, key, value, created_by, created_at)
    VALUES (%s, %s, %s, %s, %s);"""

RETRIEVE_KEY_VALUES_SQL = """SELECT * FROM runner_data WHERE job_id = %s and key IN %s;"""

RETRIEVE_DAG_SQL = """SELECT data, username FROM dag WHERE dag_id = %s;"""

RETRIEVE_USER_SQL = """SELECT * FROM users WHERE username = %s;"""

NEW_DAG_SQL = """INSERT INTO dag(dag_id, data, username, created_at)
             VALUES(%s, %s, %s, %s);"""

NEW_USER_SQL = """INSERT INTO users(username, full_name, hashed_password, email)
             VALUES(%s, %s, %s, %s);"""

params = db_config()


def create_new_job(job_id, task_id, dag_id, delay_time=10):
    current_time = datetime.now()
    trigger_time = current_time + timedelta(seconds=delay_time)
    conn = psycopg2.connect(**params)
    cur = conn.cursor()
    cur.execute(NEW_JOB_SQL, (job_id, task_id, ScheduleStatus.PENDING, trigger_time.timestamp(), 0, dag_id))
    cur.close()
    conn.commit()
    conn.close()


def update_job_status(job_id, task_id, status):
    conn = psycopg2.connect(**params)
    cur = conn.cursor()
    cur.execute(UPDATE_JOB_STATUS_SQL, (status, job_id, task_id))
    cur.close()
    conn.commit()
    conn.close()


def update_retry_count(job_id, task_id, retry_count, delay = 5):
    trigger_time = datetime.now() + timedelta(seconds=delay)
    conn = psycopg2.connect(**params)
    cur = conn.cursor()
    cur.execute(UPDATE_RETRY_COUNT_SQL, (retry_count, trigger_time.timestamp(), job_id, task_id))
    cur.close()
    conn.commit()
    conn.close()


def update_error(job_id, task_id, error):
    conn = psycopg2.connect(**params)
    cur = conn.cursor()
    cur.execute(UPDATE_ERROR_SQL, (error, ScheduleStatus.FAILED, job_id, task_id))
    cur.close()
    conn.commit()
    conn.close()


# TODO: update status of the task to 'RUNNING' in an atomic transaction by locking the query rows
def get_pending_tasks():
    conn = psycopg2.connect(**params)
    cur = conn.cursor()
    cur.execute(GET_PENDING_TASKS_SQL, (ScheduleStatus.PENDING, datetime.now().timestamp()))
    results = cur.fetchall()
    cur.close()
    conn.close()
    return results


def insert_values(values):
    conn = psycopg2.connect(**params)
    cur = conn.cursor()
    cur.executemany(INSERT_KEY_VALUES_SQL, values)
    cur.close()
    conn.commit()
    conn.close()


def get_values(job_id, keys):
    conn = psycopg2.connect(**params)
    cur = conn.cursor()
    cur.execute(RETRIEVE_KEY_VALUES_SQL, (job_id, tuple(keys)))
    results = cur.fetchall()
    cur.close()
    conn.close()

    result_dict = {res[1]: res[2]['value'] for res in results}
    return result_dict


def get_dag_data(dag_id):
    conn = psycopg2.connect(**params)
    cur = conn.cursor()
    cur.execute(RETRIEVE_DAG_SQL, [dag_id])
    results = cur.fetchone()
    cur.close()
    conn.close()
    return results


def create_new_dag(dag_id, data, username):
    conn = psycopg2.connect(**params)
    cur = conn.cursor()
    cur.execute(NEW_DAG_SQL, (dag_id, data, username, datetime.now().timestamp()))
    cur.close()
    conn.commit()
    conn.close()


def create_user(username, full_name, hashed_password, email):
    conn = psycopg2.connect(**params)
    cur = conn.cursor()
    cur.execute(NEW_USER_SQL, (username, full_name, hashed_password, email))
    cur.close()
    conn.commit()
    conn.close()


def get_user(username):
    conn = psycopg2.connect(**params)
    cur = conn.cursor()
    cur.execute(RETRIEVE_USER_SQL, [username])
    results = cur.fetchone()
    cur.close()
    conn.close()
    if results:
        user = {
            "username": results[0],
            "full_name": results[1],
            "hashed_password": results[2],
            "email": results[3],
        }
        return user
    return None


def create_tables():
    conn = psycopg2.connect(**params)
    cur = conn.cursor()
    # create table one by one
    for command in CREATE_TABLE_SQLS:
        cur.execute(command)
    cur.close()
    conn.commit()
    conn.close()

# create_tables()

