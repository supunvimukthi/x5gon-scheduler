import networkx as nx
import json

import runner.http_ as http


def response_filter(response):
    return response.json()


base_url = 'http://127.0.0.1:8080/'


decode_url = http.HttpOperator(
    base_url='http://13.234.76.200:8000/api/v1',
    endpoint='uri_decoder',
    method='GET',
    data={},
    headers={},
    response_check=None,
    response_filter=lambda res: res.json(),
    extra_options={},
    log_response=False,
    auth_type=None,
    conditions=None,
    request_mapping={'url': 'url'},
    response_mapping={
        'filename': 'filename',
        'domain': 'domain',
        'filetype': 'filetype',
        'mime_type': 'mime_type'
    },
    max_retry_count=3
)

copy_to_s3 = http.HttpOperator(
    base_url=base_url,
    endpoint='process/copy_to_s3',
    method='POST',
    data={
        'AWS_key_id': 'str',
        'AWS_secret_access_key': 'str',
        'AWS_region_name': 'str',
        'source_url': 'str',
        'destination_bucket': 'str',
        'destination_path': 'str',
    },
    headers={},
    response_check=None,
    response_filter=lambda res: res.json(),
    extra_options={},
    log_response=False,
    auth_type=None,
    conditions=None,
    request_mapping={
        'domain': 'filename'
    },
    response_mapping={'s3_url': 's3_url'},
    max_retry_count=3
)

submit_text_extraction_job = http.HttpOperator(
    base_url=base_url,
    endpoint='process/submit_text_extraction_job',
    method='POST',
    data={
        'AWS_key_id': 'str',
        'AWS_secret_access_key': 'str',
        'AWS_region_name': 'str'
    },
    headers={},
    response_check=None,
    response_filter=lambda res: res.json(),
    extra_options={},
    log_response=False,
    auth_type=None,
    conditions={'filetype': '.pdf'},
    request_mapping={
        'filetype': 'filetype',
        's3_url': 's3_url'
    },
    response_mapping={'job_id': 'job_id'},
    max_retry_count=3
)

get_text_extraction = http.HttpOperator(
    base_url=base_url,
    endpoint='process/get_text_extraction',
    method='POST',
    data={},
    headers={},
    response_check=None,
    response_filter=lambda res: res.json(),
    extra_options={},
    log_response=False,
    auth_type=None,
    conditions=None,
    request_mapping={'job_id': 'job_id'},
    response_mapping={
        'text_extracted_text': 'text_extracted_text',
        'text_extracted_full_text': 'text_extracted_full_text'
    },
    max_retry_count=3
)

wikify_text = http.HttpOperator(
    base_url=base_url,
    endpoint='process/wikify_text',
    method='POST',
    data={'WIKIFIER_token_id': 'token'},
    headers={},
    response_check=None,
    response_filter=lambda res: res.json(),
    extra_options={},
    log_response=False,
    auth_type=None,
    conditions={},
    request_mapping={'text_extracted_text': 'texts'},
    response_mapping={
        'page_rank_topics': 'page_rank_topics',
        'cosine_rank_topics': 'cosine_rank_topics'
    },
    max_retry_count=3
)

push_to_X5DB = http.HttpOperator(
    base_url=base_url,
    endpoint='process/push_to_X5DB',
    method='POST',
    data={
        'db_host': 'str',
        'db_user': 'str',
        'db_pass': 'str'
    },
    headers={},
    response_check=None,
    response_filter=lambda res: res.json(),
    extra_options={},
    log_response=False,
    auth_type=None,
    conditions={},
    request_mapping={
        'page_rank_topics': 'page_rank_topics',
        'cosine_rank_topics': 'cosine_rank_topics',
        'text_extracted_full_text': 'text_extracted_full_text',
        'domain': 'domain',
        'mime_type': 'mime_type'
    },
    response_mapping={},
    max_retry_count=3
)

push_to_elastic = http.HttpOperator(
    base_url=base_url,
    endpoint='process/push_to_elastic',
    method='POST',
    data={},
    headers={},
    response_check=None,
    response_filter=lambda res: res.json(),
    extra_options={},
    log_response=False,
    auth_type=None,
    conditions=None,
    request_mapping={},
    response_mapping={},
    max_retry_count=3
)

transcribe = http.HttpOperator(
    base_url=base_url,
    endpoint='process/transcribe',
    method='POST',
    data={},
    headers={},
    response_check=None,
    response_filter=lambda res: res.json(),
    extra_options={},
    log_response=False,
    auth_type=None,
    conditions={'filetype': '.mp4'},
    request_mapping={'filetype': 'filetype'},
    response_mapping={},
    max_retry_count=3
)

X5gon_DAG = nx.DiGraph()
X5gon_DAG.add_node("decode_url", http_operator=decode_url)
X5gon_DAG.add_node("copy_to_s3", http_operator=copy_to_s3)
X5gon_DAG.add_node("transcribe", http_operator=transcribe)
X5gon_DAG.add_node("submit_text_extraction_job", http_operator=submit_text_extraction_job)
X5gon_DAG.add_node("get_text_extraction", http_operator=get_text_extraction)
X5gon_DAG.add_node("wikify_text", http_operator=wikify_text)
X5gon_DAG.add_node("push_to_X5DB", http_operator=push_to_X5DB)
X5gon_DAG.add_node("push_to_elastic", http_operator=push_to_elastic)

X5gon_DAG.add_edge("decode_url", "copy_to_s3")
X5gon_DAG.add_edge("copy_to_s3", "submit_text_extraction_job")
X5gon_DAG.add_edge("copy_to_s3", "transcribe")
X5gon_DAG.add_edge("submit_text_extraction_job", "get_text_extraction")
X5gon_DAG.add_edge("get_text_extraction", "wikify_text")
X5gon_DAG.add_edge("wikify_text", "push_to_X5DB")
X5gon_DAG.add_edge("push_to_X5DB", "push_to_elastic")


# list(X5gon_DAG.edge_dfs(X5gon_DAG, 'copy_to_s3'))
# nx.descendants(X5gon_DAG, "copy_to_s3")
list(X5gon_DAG.successors('copy_to_s3'))

transcribe.delay
# nx.draw_planar(X5gon_DAG,
#                with_lasbels=True,
#                node_size=1000,
#                node_color="#ffff8f",
#                width=0.8,
#                font_size=14)

from networkx.readwrite import json_graph

data = json_graph.node_link_data(X5gon_DAG)
(decode_url.__dict__)

# ##{
#   "data": {"url": "http://hydro.ijs.si/v015/f9/7gh3dwpzrfpfvxnrl5fkaq4nedrqguh6.mp4"},
#   "triggered_by": "SD",
#   "next_job": "decode_url"
# }

#http://13.234.76.200:8080/docs#/default/create_job_runner_create_job_post


def create_dag(json_data):
    data_object = json_data
    if type(json_data) == str:
        data_object = json.loads(json_data)
    graph = nx.DiGraph()
    for node in data_object['nodes']:
        graph.add_node(node['id'], http_operator=http.HttpOperator(**node['http_operator']))
    for edge in data_object['edges']:
        graph.add_edge(edge['source'], edge['target'])

    return graph

