import io
import os
import socket
import tempfile
import unittest
from unittest import mock

import pipeline_http_request as target


PUBLIC_V4 = "93.184.216.34"
PUBLIC_V6 = "2606:2800:220:1:248:1893:25c8:1946"


class FakeResponse:
    def __init__(self, status=200, headers=None, body=b""):
        self.status = status
        self._headers = list(headers or [])
        self._body = io.BytesIO(body)

    def getheader(self, name):
        for key, value in self._headers:
            if key.lower() == name.lower():
                return value
        return None

    def getheaders(self):
        return list(self._headers)

    def read(self, size=-1):
        return self._body.read(size)


class FakeConnection:
    def __init__(self, response):
        self.response = response
        self.requests = []
        self.closed = False

    def request(self, method, path, body=None, headers=None):
        self.requests.append((method, path, body, dict(headers or {})))

    def getresponse(self):
        return self.response

    def close(self):
        self.closed = True


class ConnectionQueue:
    def __init__(self, responses):
        self.responses = list(responses)
        self.connections = []

    def __call__(self, scheme, host, port, address, context, timeout):
        connection = FakeConnection(self.responses.pop(0))
        connection.target = (scheme, host, port, address, context, timeout)
        self.connections.append(connection)
        return connection


class PipelineHTTPRequestTest(unittest.TestCase):
    def _request(self, method, url, responses, headers=(), body=None, resolver=None):
        output = io.BytesIO()
        queue = ConnectionQueue(responses)
        result = target.request(
            method,
            url,
            headers,
            body,
            output,
            resolver=resolver or (lambda _host, _port: [PUBLIC_V4]),
            connection_factory=queue,
        )
        return result, output.getvalue(), queue

    def test_public_get_post_put_and_patch(self):
        for method in ["GET", "POST", "PUT", "PATCH"]:
            with self.subTest(method=method):
                body = None if method == "GET" else b'{"ok":true}'
                result, output, queue = self._request(
                    method,
                    "https://api.example.com/v1/items",
                    [FakeResponse(body=b"response")],
                    headers=(("Content-Type", "application/json"),),
                    body=body,
                )
                self.assertEqual(200, result[1])
                self.assertEqual(b"response", output)
                request = queue.connections[0].requests[0]
                self.assertEqual(method, request[0])
                self.assertEqual(body, request[2])

    def test_head_is_supported(self):
        result, output, queue = self._request(
            "HEAD",
            "https://api.example.com/health",
            [FakeResponse(headers=[("Content-Length", "0")])],
        )
        self.assertEqual(200, result[1])
        self.assertEqual(b"", output)
        self.assertEqual("HEAD", queue.connections[0].requests[0][0])

    def test_public_plain_http_is_supported(self):
        result, output, queue = self._request(
            "GET",
            "http://public.example.com/data",
            [FakeResponse(body=b"public")],
        )
        self.assertEqual(200, result[1])
        self.assertEqual(b"public", output)
        self.assertEqual("http", queue.connections[0].target[0])

    def test_bearer_header_is_sent_but_never_logged(self):
        secret = "Bearer secret-token-value"
        _result, _output, queue = self._request(
            "POST",
            "https://api.example.com/v1/items",
            [FakeResponse(body=b"ok")],
            headers=(("Authorization", secret),),
            body=b"{}",
        )
        sent_headers = queue.connections[0].requests[0][3]
        self.assertEqual(secret, sent_headers["Authorization"])

    def test_header_env_avoids_secret_in_interface_output(self):
        with mock.patch.dict(os.environ, {"API_AUTH": "Bearer from-env"}):
            spec = target.parse_cli(
                [
                    "POST",
                    "https://api.example.com/",
                    "--header-env",
                    "Authorization=API_AUTH",
                ]
            )
        self.assertEqual((("Authorization", "Bearer from-env"),), spec.headers)

    def test_private_local_link_local_and_metadata_are_rejected(self):
        urls = [
            "http://localhost/",
            "https://127.0.0.1/",
            "https://10.0.0.1/",
            "https://172.16.0.1/",
            "https://192.168.0.1/",
            "http://169.254.169.254/latest/meta-data/",
            "https://[::1]/",
            "https://[fc00::1]/",
            "https://[fe80::1]/",
        ]
        for url in urls:
            with self.subTest(url=url), self.assertRaises(target.RequestError):
                parsed, _scheme, host, port = target.validate_url(url)
                target.resolve_public_addresses(host, port)

    def test_all_dns_answers_must_be_public(self):
        answers = [
            (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", (PUBLIC_V4, 443)),
            (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("10.0.0.8", 443)),
        ]
        with mock.patch("socket.getaddrinfo", return_value=answers):
            with self.assertRaises(target.RequestError):
                target.resolve_public_addresses("api.example.com", 443)

    def test_redirect_to_private_target_is_rejected(self):
        queue = ConnectionQueue(
            [FakeResponse(status=302, headers=[("Location", "http://169.254.169.254/")])]
        )

        def resolver(host, _port):
            if host == "169.254.169.254":
                raise target.RequestError("private redirect")
            return [PUBLIC_V4]

        with self.assertRaises(target.RequestError):
            target.request(
                "GET",
                "https://api.example.com/start",
                (),
                None,
                io.BytesIO(),
                resolver=resolver,
                connection_factory=queue,
            )
        self.assertEqual(1, len(queue.connections))

    def test_cross_origin_redirect_drops_authentication_headers(self):
        queue = ConnectionQueue(
            [
                FakeResponse(
                    status=307,
                    headers=[("Location", "https://other.example.com/next")],
                ),
                FakeResponse(body=b"ok"),
            ]
        )
        target.request(
            "POST",
            "https://api.example.com/start",
            (("Authorization", "Bearer secret"), ("X-Test", "kept")),
            b"{}",
            io.BytesIO(),
            resolver=lambda _host, _port: [PUBLIC_V4],
            connection_factory=queue,
        )
        redirected_headers = queue.connections[1].requests[0][3]
        self.assertNotIn("Authorization", redirected_headers)
        self.assertEqual("kept", redirected_headers["X-Test"])

    def test_303_changes_post_to_get_and_drops_body(self):
        queue = ConnectionQueue(
            [
                FakeResponse(status=303, headers=[("Location", "/result")]),
                FakeResponse(body=b"done"),
            ]
        )
        output = io.BytesIO()
        target.request(
            "POST",
            "https://api.example.com/start",
            (),
            b"request",
            output,
            resolver=lambda _host, _port: [PUBLIC_V4],
            connection_factory=queue,
        )
        redirected = queue.connections[1].requests[0]
        self.assertEqual("GET", redirected[0])
        self.assertIsNone(redirected[2])
        self.assertEqual(b"done", output.getvalue())

    def test_cli_rejects_delete_routing_headers_and_url_credentials(self):
        invalid = [
            ["DELETE", "https://api.example.com/"],
            ["POST", "https://api.example.com/", "--header", "Host: private"],
            ["GET", "https://user:password@api.example.com/"],
            ["POST", "https://api.example.com/", "--unknown", "value"],
        ]
        for argv in invalid:
            with self.subTest(argv=argv), self.assertRaises(target.RequestError):
                spec = target.parse_cli(argv)
                target.validate_url(spec.url)

    def test_body_file_and_stdin_are_mutually_exclusive(self):
        with self.assertRaises(target.RequestError):
            target.parse_cli(
                [
                    "POST",
                    "https://api.example.com/",
                    "--body-file",
                    "body.json",
                    "--body-stdin",
                ]
            )

    def test_read_body_from_file_and_limit(self):
        with tempfile.NamedTemporaryFile() as handle:
            handle.write(b"payload")
            handle.flush()
            spec = target.parse_cli(
                ["POST", "https://api.example.com/", "--body-file", handle.name]
            )
            self.assertEqual(b"payload", target._read_body(spec, io.BytesIO()))

    def test_head_output_redacts_sensitive_response_headers(self):
        stdout = io.StringIO()
        with mock.patch.object(target.sys, "stdout", stdout):
            target._write_head(
                200,
                [("Set-Cookie", "secret=value"), ("Content-Type", "text/plain")],
            )
        value = stdout.getvalue()
        self.assertNotIn("secret=value", value)
        self.assertIn("Set-Cookie: <redacted>", value)


if __name__ == "__main__":
    unittest.main()
