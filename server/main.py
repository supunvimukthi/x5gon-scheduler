import json

from fastapi import FastAPI
from pydantic import BaseModel
import networkx as nx
import random
from typing import Optional
import uuid
from datetime import datetime

from utils.database import create_new_job, insert_values, create_new_dag, get_dag_data
from utils.DAG import create_dag as create_dag_object

app = FastAPI()


class URL_Data(BaseModel):
    resource_url: str


class JobData(BaseModel):
    data: Optional[dict]
    triggered_by: str
    next_job: Optional[str]
    dag_id: str
    job_id: Optional[uuid.UUID or str]


class DagData(BaseModel):
    dag_json: dict
    dag_id: Optional[uuid.UUID or str]


class S3_Object(BaseModel):
    AWS_key_id: str
    AWS_secret_access_key: str
    AWS_region_name: str
    source_url: str
    destination_bucket: str
    destination_path: str
    filename: str


class SubmitTextData(BaseModel):
    AWS_key_id: str
    AWS_secret_access_key: str
    AWS_region_name: str
    s3_url: str


class GetTextData(BaseModel):
    job_id: uuid.UUID


class WikifyTextData(BaseModel):
    WIKIFIER_token_id: str
    texts: list


class DB_Data(BaseModel):
    db_host: str
    db_user: str
    db_pass: str
    record: object


class ElasticData(BaseModel):
    record: Optional[str]


def isError():
    digit = random.randint(0, 9)
    return digit == 9


def get_file_data(url):
    domains = ['mit', 'cet', 'llc', 'git']
    file_types = ['pdf', 'video']
    mime_types = {
        'pdf': 'pdf',
        'video': 'mp4'
    }
    domain = domains[random.randint(0, len(domains) - 1)]
    file_type = 'pdf'
    return {
        "domain": domain,
        "filetype": file_type,
        "mime_type": mime_types[file_type],
        "filename": 'test_{}_{}.{}'.format(url, random.randint(0, 10), mime_types[file_type])
    }


@app.get("/")
def read_root():
    return {"Hello": "World!"}


@app.post("/process/decode_url")
def decode_url(data: URL_Data):
    is_error = isError()
    response = {
        "is_error": is_error
    }

    if is_error:
        response['error_msg'] = "error occurred in decoding url"
    else:
        response.update(get_file_data(data.resource_url))

    return response


@app.post("/process/copy_to_s3")
def copy_to_s3(data: S3_Object):
    is_error = isError()
    response = {
        "is_error": is_error
    }

    if is_error:
        response['error_msg'] = "error occurred in copying to s3"
    else:
        response['s3_url'] = "https//:s3-image-{}-{}.file".format(data.filename, random.randint(0, 10000))

    return response


@app.post("/process/submit_text_extraction_job")
def submit_text_extraction_job(data: SubmitTextData):
    is_error = isError()
    response = {
        "is_error": is_error
    }

    if is_error:
        response['error_msg'] = "error occurred in submitting text extraction"
    else:
        response['job_id'] = "job-{}-{}".format(data.s3_url, random.randint(0, 10000))

    return response


@app.post("/process/get_text_extraction")
def get_text_extraction(data: GetTextData):
    is_error = isError()
    response = {
        "is_error": is_error
    }

    if is_error:
        response['error_msg'] = "error occurred in getting text extraction"
    else:
        response['text_extracted_text'] = data.job_id.split("-")
        response['text_extracted_full_text'] = data.job_id

    return response


@app.post("/process/wikify_text")
def wikify_text(data: WikifyTextData):
    is_error = isError()
    response = {
        "is_error": is_error
    }

    if is_error:
        response['error_msg'] = "error occurred in Wikifier"
    else:
        response['page_rank_topics'] = ["page rank topics", data.texts]
        response['cosine_rank_topics'] = ["cosine rank topics", data.texts]

    return response


@app.post("/process/push_to_X5DB")
def push_to_X5DB(data: DB_Data):
    is_error = isError()
    response = {
        "is_error": is_error
    }

    if is_error:
        response['error_msg'] = "error occurred in pushing to db"

    return response


@app.post("/process/push_to_elastic")
def push_to_elastic(data: ElasticData):
    is_error = isError()
    response = {
        "is_error": is_error
    }

    if is_error:
        response['error_msg'] = "error occurred in pushing to elastic"

    return response


@app.post("/process/transcribe")
def transcribe(data: ElasticData):
    is_error = isError()
    response = {
        "is_error": True
    }

    if is_error:
        response['error_msg'] = "error occurred in transcribe service"

    return response


# TODO: Check whether a job exist for the given URL
@app.post("/runner/create_job")
def create_job(data: JobData):
    try:
        job_id = str(uuid.uuid4())
        if data.job_id:
            job_id = data.job_id
        values = []
        if data.data:
            for key in data.data:
                values.append((
                    job_id,
                    key,
                    json.dumps({'value': data.data[key]}),
                    data.triggered_by,
                    datetime.now().timestamp()
                ))
            insert_values(values)

        next_job = data.next_job
        success_message = "Successfully created a job with id {} \ntrigger task: {}".format(job_id, next_job)
        if not data.next_job:
            dag_data = get_dag_data(data.dag_id)
            dag = create_dag_object(dag_data)
            root_node = [i for i in nx.topological_sort(dag)][0]
            next_job = root_node
            success_message = "Successfully created a job with id {} \ntrigger task (fallback to dag root task sine " \
                              "no trigger task was provided): {}".format(job_id, next_job)

        create_new_job(job_id, next_job, data.dag_id)
    except Exception as e:
        return {
            'is_error': True,
            'error_msg': str(e)
        }

    return {
        'is_error': False,
        'message': success_message
    }


@app.post("/runner/create_dag")
def create_dag(data: DagData):
    dag_id = str(uuid.uuid4())
    try:
        if data.dag_id:
            dag_id = data.dag_id
        if data.dag_json:
            # try creating the dag here to check for any errors
            create_dag(json.dumps(data.dag_json))
            create_new_dag(dag_id, json.dumps(data.dag_json))
        else:
            return {
                'is_error': True,
                'error_msg': 'No dag json object provided'
            }

    except Exception as e:
        return {
            'is_error': True,
            'error_msg': str(e)
        }

    return {
        'is_error': False,
        'message': 'Successfully created a dag with id : {}'.format(dag_id)
    }
