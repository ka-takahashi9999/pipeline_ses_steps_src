#!/usr/bin/env python3
"""Send ordinary HTTP API requests to public Internet endpoints only."""

import http.client
import ipaddress
import os
import re
import socket
import ssl
import sys
import tempfile
from dataclasses import dataclass
from typing import BinaryIO, Callable, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urljoin, urlsplit, urlunsplit


ALLOWED_METHODS = {"GET", "HEAD", "POST", "PUT", "PATCH"}
REDIRECT_STATUSES = {301, 302, 303, 307, 308}
MAX_REDIRECTS = 5
MAX_REQUEST_BYTES = 20 * 1024 * 1024
MAX_RESPONSE_BYTES = 100 * 1024 * 1024
TIMEOUT_SECONDS = 30
USER_AGENT = "pipeline-http-request/1.0"
SAFE_OUTPUT_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
HEADER_NAME = re.compile(r"[!#$%&'*+\-.^_`|~0-9A-Za-z]+\Z")
ENV_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
BLOCKED_HOSTS = {
    "localhost",
    "instance-data.ec2.internal",
    "metadata.aws.internal",
    "metadata.google.internal",
}
BLOCKED_HOST_SUFFIXES = (
    ".localhost",
    ".local",
    ".internal",
    ".localdomain",
    ".home.arpa",
)
PROTECTED_REQUEST_HEADERS = {
    "connection",
    "content-length",
    "host",
    "proxy-authorization",
    "transfer-encoding",
}
SENSITIVE_REDIRECT_HEADERS = {"authorization", "cookie", "proxy-authorization"}
SENSITIVE_RESPONSE_HEADERS = {
    "authorization",
    "proxy-authenticate",
    "set-cookie",
    "www-authenticate",
}


class RequestError(Exception):
    """A request rejected by policy or a failed public HTTP request."""


@dataclass(frozen=True)
class RequestSpec:
    method: str
    url: str
    headers: Tuple[Tuple[str, str], ...]
    body_file: Optional[str]
    body_stdin: bool
    output_name: Optional[str]


def _parse_header(value: str) -> Tuple[str, str]:
    if ":" not in value:
        raise RequestError("header must use NAME:VALUE")
    name, header_value = value.split(":", 1)
    name = name.strip()
    header_value = header_value.lstrip()
    if not HEADER_NAME.fullmatch(name):
        raise RequestError("invalid header name")
    if name.lower() in PROTECTED_REQUEST_HEADERS:
        raise RequestError("routing and hop-by-hop headers are managed by the wrapper")
    if "\r" in header_value or "\n" in header_value:
        raise RequestError("header values must not contain newlines")
    return name, header_value


def _parse_header_env(value: str) -> Tuple[str, str]:
    if "=" not in value:
        raise RequestError("header-env must use NAME=ENV_VAR")
    name, env_name = value.split("=", 1)
    name = name.strip()
    env_name = env_name.strip()
    if not ENV_NAME.fullmatch(env_name):
        raise RequestError("invalid header environment variable name")
    if env_name not in os.environ:
        raise RequestError("header environment variable is not set")
    return _parse_header("{}:{}".format(name, os.environ[env_name]))


def parse_cli(argv: Sequence[str]) -> RequestSpec:
    """Parse the intentionally small wrapper interface without echoing secrets."""
    if len(argv) < 2:
        raise RequestError(
            "usage: pipeline-http-request METHOD URL "
            "[--header NAME:VALUE] [--header-env NAME=ENV_VAR] "
            "[--body-file PATH|--body-stdin] [--output NAME]"
        )

    method = argv[0].upper()
    if method not in ALLOWED_METHODS:
        raise RequestError("method must be GET, HEAD, POST, PUT, or PATCH")
    url = argv[1]
    headers: List[Tuple[str, str]] = []
    body_file = None
    body_stdin = False
    output_name = None
    index = 2

    while index < len(argv):
        option = argv[index]
        if option in {"--header", "--header-env", "--body-file", "--output"}:
            if index + 1 >= len(argv):
                raise RequestError("option requires a value")
            value = argv[index + 1]
            index += 2
            if option == "--header":
                headers.append(_parse_header(value))
            elif option == "--header-env":
                headers.append(_parse_header_env(value))
            elif option == "--body-file":
                if body_file is not None or body_stdin:
                    raise RequestError("request body source may be specified only once")
                body_file = value
            else:
                if output_name is not None:
                    raise RequestError("output may be specified only once")
                if not SAFE_OUTPUT_NAME.fullmatch(value) or value in {".", ".."}:
                    raise RequestError("output name must be a safe basename")
                output_name = value
        elif option == "--body-stdin":
            if body_file is not None or body_stdin:
                raise RequestError("request body source may be specified only once")
            body_stdin = True
            index += 1
        else:
            raise RequestError("unsupported option")

    if method in {"GET", "HEAD"} and (body_file is not None or body_stdin):
        raise RequestError("GET and HEAD do not accept a request body")

    return RequestSpec(
        method=method,
        url=url,
        headers=tuple(headers),
        body_file=body_file,
        body_stdin=body_stdin,
        output_name=output_name,
    )


def _normalize_hostname(hostname: str) -> str:
    host = hostname.rstrip(".").lower()
    if not host:
        raise RequestError("URL hostname is required")
    try:
        host = host.encode("idna").decode("ascii")
    except UnicodeError as exc:
        raise RequestError("invalid URL hostname") from exc
    if host in BLOCKED_HOSTS or host.endswith(BLOCKED_HOST_SUFFIXES):
        raise RequestError("local or internal hostnames are not allowed")
    return host


def validate_url(raw_url: str):
    if not raw_url or len(raw_url) > 8192:
        raise RequestError("URL is empty or too long")
    if any(ord(char) < 0x20 for char in raw_url) or "\\" in raw_url:
        raise RequestError("URL contains unsafe characters")
    try:
        parsed = urlsplit(raw_url)
    except ValueError as exc:
        raise RequestError("invalid URL") from exc
    scheme = parsed.scheme.lower()
    if scheme not in {"http", "https"}:
        raise RequestError("only HTTP and HTTPS URLs are allowed")
    if parsed.username is not None or parsed.password is not None:
        raise RequestError("credentials in URLs are not allowed; use a header")
    if parsed.fragment:
        raise RequestError("URL fragments are not allowed")
    host = _normalize_hostname(parsed.hostname or "")
    try:
        port = parsed.port or (443 if scheme == "https" else 80)
    except ValueError as exc:
        raise RequestError("invalid URL port") from exc
    if not 1 <= port <= 65535:
        raise RequestError("invalid URL port")
    return parsed, scheme, host, port


def _require_public_ip(value: str) -> str:
    try:
        address = ipaddress.ip_address(value)
    except ValueError as exc:
        raise RequestError("DNS returned an invalid IP address") from exc
    if not address.is_global:
        raise RequestError("DNS resolved to a non-public IP address")
    return address.compressed


def resolve_public_addresses(host: str, port: int) -> List[str]:
    try:
        infos = socket.getaddrinfo(
            host,
            port,
            family=socket.AF_UNSPEC,
            type=socket.SOCK_STREAM,
            proto=socket.IPPROTO_TCP,
        )
    except socket.gaierror as exc:
        raise RequestError("hostname resolution failed") from exc
    if not infos:
        raise RequestError("hostname did not resolve")

    addresses = []
    for info in infos:
        address = _require_public_ip(info[4][0])
        if address not in addresses:
            addresses.append(address)
    return addresses


class PinnedHTTPConnection(http.client.HTTPConnection):
    def __init__(self, host: str, port: int, pinned_ip: str, timeout: int) -> None:
        super().__init__(host=host, port=port, timeout=timeout)
        self._pinned_ip = pinned_ip

    def connect(self) -> None:
        self.sock = socket.create_connection(
            (self._pinned_ip, self.port),
            self.timeout,
            self.source_address,
        )


class PinnedHTTPSConnection(http.client.HTTPSConnection):
    def __init__(
        self,
        host: str,
        port: int,
        pinned_ip: str,
        context: ssl.SSLContext,
        timeout: int,
    ) -> None:
        super().__init__(host=host, port=port, context=context, timeout=timeout)
        self._pinned_ip = pinned_ip

    def connect(self) -> None:
        sock = socket.create_connection(
            (self._pinned_ip, self.port),
            self.timeout,
            self.source_address,
        )
        self.sock = self._context.wrap_socket(sock, server_hostname=self.host)


def _connection_factory(
    scheme: str,
    host: str,
    port: int,
    address: str,
    context: ssl.SSLContext,
    timeout: int,
):
    if scheme == "https":
        return PinnedHTTPSConnection(host, port, address, context, timeout)
    return PinnedHTTPConnection(host, port, address, timeout)


def _request_once(
    method: str,
    parsed,
    scheme: str,
    host: str,
    port: int,
    addresses: Iterable[str],
    headers: Sequence[Tuple[str, str]],
    body: Optional[bytes],
    context: ssl.SSLContext,
    connection_factory: Callable,
):
    path = urlunsplit(("", "", parsed.path or "/", parsed.query, ""))
    request_headers = {name: value for name, value in headers}
    request_headers.setdefault("Accept", "*/*")
    request_headers.setdefault("User-Agent", USER_AGENT)
    request_headers["Connection"] = "close"
    last_error = None

    for address in addresses:
        connection = connection_factory(
            scheme,
            host,
            port,
            address,
            context,
            TIMEOUT_SECONDS,
        )
        try:
            connection.request(method, path, body=body, headers=request_headers)
            return connection, connection.getresponse()
        except (OSError, ssl.SSLError, http.client.HTTPException) as exc:
            last_error = exc
            connection.close()
    raise RequestError("connection failed for all public DNS results") from last_error


def _origin(parsed) -> Tuple[str, str, int]:
    scheme = parsed.scheme.lower()
    return (
        scheme,
        _normalize_hostname(parsed.hostname or ""),
        parsed.port or (443 if scheme == "https" else 80),
    )


def _read_body(spec: RequestSpec, stdin: BinaryIO) -> Optional[bytes]:
    if spec.body_file is None and not spec.body_stdin:
        return None
    if spec.body_stdin:
        data = stdin.read(MAX_REQUEST_BYTES + 1)
    else:
        try:
            with open(spec.body_file or "", "rb") as handle:
                data = handle.read(MAX_REQUEST_BYTES + 1)
        except OSError as exc:
            raise RequestError("request body file could not be read") from exc
    if len(data) > MAX_REQUEST_BYTES:
        raise RequestError("request body exceeds the size limit")
    return data


def request(
    method: str,
    initial_url: str,
    headers: Sequence[Tuple[str, str]],
    body: Optional[bytes],
    output: BinaryIO,
    resolver: Optional[Callable[[str, int], List[str]]] = None,
    connection_factory: Optional[Callable] = None,
) -> Tuple[str, int, List[Tuple[str, str]]]:
    if method not in ALLOWED_METHODS:
        raise RequestError("unsupported HTTP method")
    resolver = resolver or resolve_public_addresses
    connection_factory = connection_factory or _connection_factory
    context = ssl.create_default_context()
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    current_url = initial_url
    current_method = method
    current_headers = list(headers)
    current_body = body

    for redirect_count in range(MAX_REDIRECTS + 1):
        parsed, scheme, host, port = validate_url(current_url)
        addresses = resolver(host, port)
        if not addresses:
            raise RequestError("hostname did not resolve to a public address")
        connection, response = _request_once(
            current_method,
            parsed,
            scheme,
            host,
            port,
            addresses,
            current_headers,
            current_body,
            context,
            connection_factory,
        )
        try:
            if response.status in REDIRECT_STATUSES:
                location = response.getheader("Location")
                if not location:
                    raise RequestError("redirect response omitted Location")
                if redirect_count >= MAX_REDIRECTS:
                    raise RequestError("too many redirects")
                next_url = urljoin(current_url, location)
                next_parsed, _next_scheme, _next_host, _next_port = validate_url(next_url)
                if _origin(next_parsed) != _origin(parsed):
                    current_headers = [
                        (name, value)
                        for name, value in current_headers
                        if name.lower() not in SENSITIVE_REDIRECT_HEADERS
                    ]
                if response.status == 303 or (
                    response.status in {301, 302}
                    and current_method not in {"GET", "HEAD"}
                ):
                    current_method = "GET"
                    current_body = None
                current_url = next_url
                continue

            response_headers = list(response.getheaders())
            if current_method == "HEAD":
                return current_url, response.status, response_headers

            length = response.getheader("Content-Length")
            if length is not None:
                try:
                    declared_length = int(length)
                except ValueError as exc:
                    raise RequestError("invalid Content-Length") from exc
                if declared_length < 0 or declared_length > MAX_RESPONSE_BYTES:
                    raise RequestError("response exceeds the size limit")

            total = 0
            while True:
                chunk = response.read(64 * 1024)
                if not chunk:
                    break
                total += len(chunk)
                if total > MAX_RESPONSE_BYTES:
                    raise RequestError("response exceeds the size limit")
                output.write(chunk)
            return current_url, response.status, response_headers
        finally:
            connection.close()

    raise RequestError("too many redirects")


def _write_head(status: int, headers: Iterable[Tuple[str, str]]) -> None:
    sys.stdout.write("HTTP {}\n".format(status))
    for key, value in headers:
        safe_key = key.replace("\r", " ").replace("\n", " ")
        if key.lower() in SENSITIVE_RESPONSE_HEADERS:
            safe_value = "<redacted>"
        else:
            safe_value = value.replace("\r", " ").replace("\n", " ")
        sys.stdout.write("{}: {}\n".format(safe_key, safe_value))


def main(argv: Sequence[str]) -> int:
    output_path = None
    output_dir = None
    output_file = None
    try:
        spec = parse_cli(argv)
        body = _read_body(spec, sys.stdin.buffer)
        if spec.output_name is not None:
            output_dir = tempfile.mkdtemp(prefix="pipeline-codex.http.", dir="/tmp")
            output_path = os.path.join(output_dir, spec.output_name)
            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
            if hasattr(os, "O_NOFOLLOW"):
                flags |= os.O_NOFOLLOW
            fd = os.open(output_path, flags, 0o600)
            output_file = os.fdopen(fd, "wb")
            sink = output_file
        else:
            sink = sys.stdout.buffer

        _final_url, status, response_headers = request(
            spec.method,
            spec.url,
            spec.headers,
            body,
            sink,
        )
        if output_file is not None:
            output_file.close()
            output_file = None
            sys.stdout.write(output_path + "\n")
        elif spec.method == "HEAD":
            _write_head(status, response_headers)

        if not 200 <= status < 300:
            sys.stderr.write("pipeline-http-request: HTTP status {}\n".format(status))
            return 22
        return 0
    except RequestError as exc:
        if output_file is not None:
            output_file.close()
        if output_path is not None:
            try:
                os.unlink(output_path)
            except FileNotFoundError:
                pass
        if output_dir is not None:
            try:
                os.rmdir(output_dir)
            except OSError:
                pass
        sys.stderr.write("pipeline-http-request: {}\n".format(exc))
        return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
