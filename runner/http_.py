import logging
import json
from typing import Any, Callable, Dict, Optional, Type

from requests.auth import AuthBase, HTTPBasicAuth

from runner.httpHook import HttpHook


class HttpOperator:
    """
    Calls an endpoint on an HTTP system to execute an action


    :param base_url: base_url of where the http endpoint is running
    :param endpoint: The relative part of the full url. (templated)
    :param method: The HTTP method to use, default = "POST"
    :param data: The data to pass. POST-data in POST/PUT and params
        in the URL for a GET request. (templated)
    :param headers: The HTTP headers to be added to the GET request
    :param response_check: A check against the 'requests' response object.
        The callable takes the response object as the first positional argument
        and optionally any number of keyword arguments available in the context dictionary.
        It should return True for 'pass' and False otherwise.
    :param response_filter: A function allowing you to manipulate the response
        text. e.g response_filter=lambda response: json.loads(response.text).
        The callable takes the response object as the first positional argument
        and optionally any number of keyword arguments available in the context dictionary.
    :param extra_options: Extra options for the 'requests' library, see the
        'requests' documentation (options to modify timeout, ssl, etc.)
    :param log_response: Log the response (default: False)
    :param auth_type: The auth type for the service
    """


    def __init__(
        self,
        *,
        base_url: str = None,
        endpoint: Optional[str] = None,
        method: str = 'POST',
        data: Any = {},
        headers: Optional[Dict[str, str]] = None,
        response_check: Optional[Callable[..., bool]] = None,
        response_filter: Optional[Callable[..., Any]] = lambda response: response.json(),
        extra_options: Optional[Dict[str, Any]] = None,
        log_response: bool = False,
        auth_type: Type[AuthBase] = HTTPBasicAuth,
        request_mapping: Dict[str, str] = {},
        response_mapping: Dict[str, str] = {},
        conditions: Optional[Dict[str, str]] = None,
        max_retry_count: int = 3,
        delay: int = 30,
        log=logging.getLogger('Http_Operator')
    ) -> None:
        self.base_url = base_url
        self.endpoint = endpoint
        self.method = method
        self.headers = headers or {}
        self.data = data or {}
        self.response_check = response_check
        self.response_filter = response_filter
        self.extra_options = extra_options or {}
        self.log_response = log_response
        self.auth_type = auth_type or None
        self.conditions = conditions
        self.request_mapping = request_mapping
        self.response_mapping = response_mapping
        self.max_retry_count = max_retry_count
        self.log = log
        self.delay = delay

        self.log.setLevel(level=logging.DEBUG)

    def __iter__(self):
        yield from {
            "base_url": self.base_url,
            "endpoint": self.endpoint,
            "method": self.method,
            "headers": self.headers,
            "data": self.data,
            "response_check": str(self.response_check),
            "response_filter": str(self.response_filter),
            "log_response": self.log_response,
            "auth_type": self.auth_type,
            "conditions": self.conditions,
            "request_mapping": self.request_mapping,
            "response_mapping": self.response_mapping,
            "max_retry_count": self.max_retry_count,
            "delay": self.delay
        }.items()

    def __str__(self):
        return json.dumps(dict(self), ensure_ascii=False)

    def __repr__(self):
        return self.__str__()

    def to_json(self):
        return self.__str__()

    def execute(self, data) -> Any:
        self.data.update(data)

        for key in self.request_mapping:
            if key not in self.data:
                raise Exception("missing field in request : {}".format(key))
            self.data[self.request_mapping[key]] = self.data[key]
            # del self.data[key]

        if self.conditions:
            for condition in self.conditions:
                if self.data[condition] != self.conditions[condition]:
                    return
        http = HttpHook(self.method, base_url=self.base_url, auth_type=self.auth_type)

        self.log.info("Calling HTTP method")

        response = http.run(self.endpoint, self.data, self.headers, self.extra_options)
        if self.log_response:
            self.log.info(response.text)
        if self.response_check:
            if not self.response_check(response):
                raise Exception("Response check returned False.")
        if self.response_filter:
            return self.response_filter(response)
        return response.text
