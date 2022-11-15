import json
import random
import uuid

from datetime import datetime, timedelta
from typing import Optional, Union

from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
from passlib.context import CryptContext
from pydantic import BaseModel
import networkx as nx

from utils.database import create_new_job, insert_values, create_new_dag, get_dag_data, get_user, create_user
from utils.DAG import create_dag as create_dag_object
from utils.config import server_config

SECRET_KEY = server_config()["secret_key"]
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

app = FastAPI()


class URL_Data(BaseModel):
    resource_url: str


class JobData(BaseModel):
    data: Optional[dict]
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
    job_id: str


class WikifyTextData(BaseModel):
    WIKIFIER_token_id: str
    texts: list


class DB_Data(BaseModel):
    db_host: str
    db_user: str
    db_pass: str
    record: object


class Token(BaseModel):
    access_token: str
    token_type: str


class TokenData(BaseModel):
    username: Union[str, None] = None


class User(BaseModel):
    username: str
    email: Union[str, None] = None
    full_name: Union[str, None] = None
    hashed_password: Union[str, None] = None


class NewUser(BaseModel):
    username: str
    email: str
    full_name: str
    password: str


class ElasticData(BaseModel):
    record: Optional[str]


def get_password_hash(password):
    return pwd_context.hash(password)


def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)


def authenticate_user(username: str, password: str):
    user = get_user(username)
    if not user:
        return False
    user = User(**user)
    if not verify_password(password, user.hashed_password):
        return False
    return user


def create_access_token(data: dict, expires_delta: Union[timedelta, None] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


async def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception
    user = get_user(username)
    if user is None:
        raise credentials_exception
    user = User(**user)
    return user


async def get_current_active_user(current_user: User = Depends(get_current_user)):
    if current_user.disabled:
        raise HTTPException(status_code=400, detail="Inactive user")
    return current_user


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
    return {"status": "ok!"}


@app.post("/token", response_model=Token)
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    user = authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user.username}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}


@app.post("/register")
async def register(user: NewUser):
    try:
        hashed_password = get_password_hash(user.password)
        create_user(user.username, user.full_name, hashed_password, user.email)
    except Exception as e:
        return {
            'is_error': True,
            'message': str(e)
        }

    return {
        'is_error': False,
        'message': "New user: {} created successfully".format(user.username)
    }


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
    response = {
        "is_error": True,
        'error_msg': "error occurred in transcribe service"
    }

    return response


# TODO: Check whether a job exist for the given URL
@app.post("/runner/create_job")
def create_job(data: JobData, current_user: User = Depends(get_current_user)):
    try:
        dag_data, username = get_dag_data(data.dag_id)
        if username != current_user.username:
            raise Exception("Dag does not belong to current user: {}".format(current_user.username))
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
                    current_user.username,
                    datetime.now().timestamp()
                ))
            insert_values(values)

        next_job = data.next_job
        success_message = "Successfully created a job with id {} \ntrigger task: {}".format(job_id, next_job)
        if not data.next_job:
            dag = create_dag_object(dag_data)
            root_node = [i for i in nx.topological_sort(dag)][0]
            next_job = root_node
            success_message = "Successfully created a job with id {} \ntrigger task (fallback to dag root task since " \
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
def create_dag(data: DagData, current_user: User = Depends(get_current_user)):
    dag_id = str(uuid.uuid4())
    try:
        if data.dag_id:
            dag_id = data.dag_id
        if data.dag_json:
            # try creating the dag here to check for any errors
            create_dag_object(json.dumps(data.dag_json))
            create_new_dag(dag_id, json.dumps(data.dag_json), current_user.username)
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
