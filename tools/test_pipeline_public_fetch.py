import io
import os
import socket
import unittest
from unittest import mock

import pipeline_public_fetch as target


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

    def request(self, method, path, headers):
        self.requests.append((method, path, headers))

    def getresponse(self):
        return self.response

    def close(self):
        self.closed = True


class ConnectionQueue:
    def __init__(self, responses):
        self.responses = list(responses)
        self.connections = []

    def __call__(self, host, port, address, context, timeout):
        connection = FakeConnection(self.responses.pop(0))
        connection.target = (host, port, address, timeout)
        self.connections.append(connection)
        return connection


class PipelinePublicFetchTest(unittest.TestCase):
    def test_public_https_get_is_allowed(self):
        output = io.BytesIO()
        queue = ConnectionQueue([FakeResponse(body=b"public data")])
        final_url, status, _headers = target.fetch(
            "GET",
            "https://example.com/public-data.csv",
            output,
            resolver=lambda _host, _port: [PUBLIC_V4],
            connection_factory=queue,
        )
        self.assertEqual("https://example.com/public-data.csv", final_url)
        self.assertEqual(200, status)
        self.assertEqual(b"public data", output.getvalue())
        method, _path, headers = queue.connections[0].requests[0]
        self.assertEqual("GET", method)
        self.assertNotIn("Authorization", headers)
        self.assertNotIn("Cookie", headers)

    def test_head_is_allowed_without_body(self):
        queue = ConnectionQueue(
            [FakeResponse(headers=[("Content-Length", "123")], body=b"ignored")]
        )
        _url, status, headers = target.fetch(
            "HEAD",
            "https://example.com/data",
            io.BytesIO(),
            resolver=lambda _host, _port: [PUBLIC_V6],
            connection_factory=queue,
        )
        self.assertEqual(200, status)
        self.assertEqual([("Content-Length", "123")], headers)
        self.assertEqual("HEAD", queue.connections[0].requests[0][0])

    def test_non_https_and_local_targets_are_rejected(self):
        rejected = [
            "http://169.254.169.254/",
            "https://127.0.0.1/",
            "https://localhost/",
            "https://10.0.0.1/",
            "https://[::1]/",
            "file:///etc/passwd",
            "ftp://example.com/file",
            "https://vpce-123.execute-api.ap-northeast-1.amazonaws.com/",
        ]
        for url in rejected:
            with self.subTest(url=url), self.assertRaises(target.FetchError):
                parsed, host, port = target.validate_url(url)
                target.resolve_public_addresses(host, port)

    def test_all_non_public_ip_classes_are_rejected(self):
        rejected = [
            "127.0.0.1",
            "10.0.0.1",
            "172.16.0.1",
            "192.168.0.1",
            "169.254.169.254",
            "100.64.0.1",
            "0.0.0.0",
            "::1",
            "fc00::1",
            "fe80::1",
        ]
        for address in rejected:
            with self.subTest(address=address), self.assertRaises(target.FetchError):
                target._require_global_ip(address)

    def test_credentials_in_url_are_rejected(self):
        rejected = [
            "https://user:password@example.com/data",
            "https://example.com/data?access_token=secret",
            "https://example.com/data?X-Amz-Signature=secret",
        ]
        for url in rejected:
            with self.subTest(url=url), self.assertRaises(target.FetchError):
                target.validate_url(url)

    def test_dns_result_must_be_entirely_public(self):
        answers = [
            (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", (PUBLIC_V4, 443)),
            (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("10.0.0.8", 443)),
        ]
        with mock.patch("socket.getaddrinfo", return_value=answers):
            with self.assertRaises(target.FetchError):
                target.resolve_public_addresses("example.com", 443)

    def test_redirect_to_private_ip_is_rejected_before_second_connection(self):
        queue = ConnectionQueue(
            [FakeResponse(status=302, headers=[("Location", "https://127.0.0.1/")])]
        )

        def resolver(host, _port):
            if host == "127.0.0.1":
                raise target.FetchError("private redirect")
            return [PUBLIC_V4]

        with self.assertRaises(target.FetchError):
            target.fetch(
                "GET",
                "https://example.com/start",
                io.BytesIO(),
                resolver=resolver,
                connection_factory=queue,
            )
        self.assertEqual(1, len(queue.connections))

    def test_public_redirect_is_revalidated(self):
        queue = ConnectionQueue(
            [
                FakeResponse(status=302, headers=[("Location", "https://data.example.org/a")]),
                FakeResponse(body=b"ok"),
            ]
        )
        resolved = []

        def resolver(host, _port):
            resolved.append(host)
            return [PUBLIC_V4]

        output = io.BytesIO()
        final_url, _status, _headers = target.fetch(
            "GET",
            "https://example.com/start",
            output,
            resolver=resolver,
            connection_factory=queue,
        )
        self.assertEqual(["example.com", "data.example.org"], resolved)
        self.assertEqual("https://data.example.org/a", final_url)
        self.assertEqual(b"ok", output.getvalue())

    def test_cli_rejects_method_header_upload_and_data_options(self):
        rejected = [
            ["--request", "POST", "https://example.com/"],
            ["-X", "POST", "https://example.com/"],
            ["-d", "value", "https://example.com/"],
            ["--data-binary", "@file", "https://example.com/"],
            ["-F", "file=@x", "https://example.com/"],
            ["-T", "file", "https://example.com/"],
            ["-H", "Authorization: secret", "https://example.com/"],
            ["-H", "Cookie: secret", "https://example.com/"],
            ["--proxy", "https://proxy.example", "https://example.com/"],
            ["--netrc", "https://example.com/"],
            ["--cert", "client.pem", "https://example.com/"],
            ["--user", "user:password", "https://example.com/"],
            ["--unix-socket", "/tmp/socket", "https://example.com/"],
            ["--upload-file", "file", "https://example.com/"],
        ]
        for argv in rejected:
            with self.subTest(argv=argv), self.assertRaises(target.FetchError):
                target.parse_cli(argv)

    def test_cli_output_name_is_only_a_basename(self):
        spec = target.parse_cli(["https://example.com/data", "public-data.csv"])
        self.assertEqual("public-data.csv", spec.output_name)
        for name in ["../data", "/tmp/data", "subdir/data", "-d", "Authorization:secret"]:
            with self.subTest(name=name), self.assertRaises(target.FetchError):
                target.parse_cli(["https://example.com/data", name])

    def test_saved_output_uses_wrapper_created_safe_temp_directory(self):
        def fake_fetch(_method, _url, output):
            output.write(b"saved")
            return "https://example.com/data", 200, []

        stdout = io.StringIO()
        with mock.patch.object(target, "fetch", side_effect=fake_fetch):
            with mock.patch.object(target.sys, "stdout", stdout):
                result = target.main(["https://example.com/data", "data.csv"])
        path = stdout.getvalue().strip()
        try:
            self.assertEqual(0, result)
            self.assertRegex(path, r"^/tmp/pipeline-codex\.fetch\.[^/]+/data\.csv$")
            with open(path, "rb") as handle:
                self.assertEqual(b"saved", handle.read())
            self.assertEqual(0o600, os.stat(path).st_mode & 0o777)
        finally:
            if path and os.path.exists(path):
                os.unlink(path)
                os.rmdir(os.path.dirname(path))

    def test_only_get_and_head_are_callable(self):
        with self.assertRaises(target.FetchError):
            target.fetch(
                "POST",
                "https://example.com/",
                io.BytesIO(),
                resolver=lambda _host, _port: [PUBLIC_V4],
                connection_factory=ConnectionQueue([]),
            )


if __name__ == "__main__":
    unittest.main()
