from typing import Any, Dict, Optional, Union
import logging
import requests
from requests.auth import HTTPBasicAuth


class HttpHook:
    """
    Interact with HTTP servers.

    :param method: the API method to be called
    :param base_url: base_url of where the http endpoint is running Default
        headers can also be specified in the Extra field in json format.
    :param auth_type: The auth type for the service
    """

    def __init__(
        self,
        method: str = 'POST',
        base_url: str = "",
        auth_type: Any = HTTPBasicAuth,
    ) -> None:
        super().__init__()
        self.method = method.upper()
        self.base_url: str = base_url
        self.auth_type: Any = auth_type
        self.log = logging.getLogger('Http_Operator')

        self.log.setLevel(level=logging.DEBUG)

    # headers may be passed through directly or in the "extra" field in the connection
    # definition
    def get_session(self, headers: Optional[Dict[Any, Any]] = None) -> requests.Session:
        """
        Returns http session for use with requests

        :param headers: additional headers to be passed through as a dictionary
        """
        session = requests.Session()

        if not self.base_url:
            raise ValueError('base url is not defined')

        # TODO: handle different authentications
        # if conn.login:
        #     session.auth = self.auth_type(conn.login, conn.password)
        if headers:
            session.headers.update(headers)

        return session

    def run(
        self,
        endpoint: Optional[str] = None,
        data: Optional[Union[Dict[str, Any], str]] = None,
        headers: Optional[Dict[str, Any]] = None,
        extra_options: Optional[Dict[str, Any]] = None,
        **request_kwargs: Any,
    ) -> Any:
        r"""
        Performs the request

        :param endpoint: the endpoint to be called i.e. resource/v1/query?
        :param data: payload to be uploaded or request parameters
        :param headers: additional headers to be passed through as a dictionary
        :param extra_options: additional options to be used when executing the request
            i.e. {'check_response': False} to avoid checking raising exceptions on non
            2XX or 3XX status codes
        :param request_kwargs: Additional kwargs to pass when creating a request.
            For example, ``run(json=obj)`` is passed as ``requests.Request(json=obj)``
        """
        extra_options = extra_options or {}

        session = self.get_session(headers)

        url = self.url_from_endpoint(endpoint)

        if self.method == 'GET':
            # GET uses params
            req = requests.Request(self.method, url, params=data, headers=headers, **request_kwargs)
        elif self.method == 'HEAD':
            # HEAD doesn't use params
            req = requests.Request(self.method, url, headers=headers, **request_kwargs)
        else:
            # Others use data
            req = requests.Request(self.method, url, json=data, headers=headers, **request_kwargs)

        prepped_request = session.prepare_request(req)
        self.log.info("Sending '%s' to url: %s", self.method, url)
        return self.run_and_check(session, prepped_request, extra_options)

    def run_and_check(
        self,
        session: requests.Session,
        prepped_request: requests.PreparedRequest,
        extra_options: Dict[Any, Any],
    ) -> Any:
        """
        Grabs extra options like timeout and actually runs the request,
        checking for the result

        :param session: the session to be used to execute the request
        :param prepped_request: the prepared request generated in run()
        :param extra_options: additional options to be used when executing the request
            i.e. ``{'check_response': False}`` to avoid checking raising exceptions on non 2XX
            or 3XX status codes
        """
        extra_options = extra_options or {}

        settings = session.merge_environment_settings(
            prepped_request.url,
            proxies=extra_options.get("proxies", {}),
            stream=extra_options.get("stream", False),
            verify=extra_options.get("verify"),
            cert=extra_options.get("cert"),
        )

        # Send the request.
        send_kwargs: Dict[str, Any] = {
            "timeout": extra_options.get("timeout"),
            "allow_redirects": extra_options.get("allow_redirects", True),
        }
        send_kwargs.update(settings)

        try:
            response = session.send(prepped_request, **send_kwargs)

            if extra_options.get('check_response', True):
                self.check_response(response)
            return response

        except Exception as ex:
            self.log.warning('Error occurred while calling the http endpoint', ex)
            raise ex

    def check_response(self, response: requests.Response) -> None:
        """
        Checks the status code and raise an exception on non 2XX or 3XX
        status codes

        :param response: A requests response object
        """
        try:
            response.raise_for_status()
            if 'is_error' in response.json() and response.json()['is_error']:
                raise Exception(response.json()['error_msg'])
        except requests.exceptions.HTTPError:
            self.log.error("HTTP error: %s", response.reason)
            self.log.error(response.text)
            raise Exception(str(response.status_code) + ":" + response.reason)

    def url_from_endpoint(self, endpoint: Optional[str]) -> str:
        """Combine base url with endpoint"""
        if self.base_url and not self.base_url.endswith('/') and endpoint and not endpoint.startswith('/'):
            return self.base_url + '/' + endpoint
        return (self.base_url or '') + (endpoint or '')

    def test_connection(self):
        """Test HTTP Connection"""
        try:
            self.run()
            return True, 'Connection successfully tested'
        except Exception as e:
            return False, str(e)
