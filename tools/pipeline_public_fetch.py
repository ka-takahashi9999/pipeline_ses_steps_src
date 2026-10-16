#!/usr/bin/env python3
"""Fetch public HTTPS resources through a deliberately narrow interface."""

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
from urllib.parse import parse_qsl, urljoin, urlsplit, urlunsplit


MAX_REDIRECTS = 5
MAX_RESPONSE_BYTES = 100 * 1024 * 1024
TIMEOUT_SECONDS = 30
USER_AGENT = "pipeline-public-fetch/1.0"
SAFE_OUTPUT_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
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
BLOCKED_QUERY_KEYS = {
    "access_token",
    "api_key",
    "apikey",
    "auth",
    "authorization",
    "key",
    "password",
    "passwd",
    "sig",
    "signature",
    "token",
    "x-amz-algorithm",
    "x-amz-credential",
    "x-amz-security-token",
    "x-amz-signature",
}
REDIRECT_STATUSES = {301, 302, 303, 307, 308}


class FetchError(Exception):
    """A request rejected by policy or a failed safe fetch."""


@dataclass(frozen=True)
class RequestSpec:
    method: str
    url: str
    output_name: Optional[str]


def parse_cli(argv: Sequence[str]) -> RequestSpec:
    """Accept only GET-to-stdout, GET-to-safe-temp-file, or HEAD."""
    if len(argv) == 1 and not argv[0].startswith("-"):
        return RequestSpec("GET", argv[0], None)
    if len(argv) == 2 and argv[0] == "--head":
        return RequestSpec("HEAD", argv[1], None)
    if len(argv) == 2 and not argv[0].startswith("-"):
        if not SAFE_OUTPUT_NAME.fullmatch(argv[1]) or argv[1] in {".", ".."}:
            raise FetchError("output name must be a safe basename")
        return RequestSpec("GET", argv[0], argv[1])
    raise FetchError(
        "usage: pipeline-public-fetch URL [OUTPUT_NAME] | "
        "pipeline-public-fetch --head URL"
    )


def _normalize_hostname(hostname: str) -> str:
    host = hostname.rstrip(".").lower()
    if not host:
        raise FetchError("URL hostname is required")
    try:
        host = host.encode("idna").decode("ascii")
    except UnicodeError as exc:
        raise FetchError("invalid URL hostname") from exc
    if host in BLOCKED_HOSTS or host.endswith(BLOCKED_HOST_SUFFIXES):
        raise FetchError("local or internal hostnames are not allowed")
    if host.startswith("vpce-") or ".vpce." in host:
        raise FetchError("private AWS endpoint hostnames are not allowed")
    return host


def validate_url(raw_url: str):
    if not raw_url or len(raw_url) > 8192:
        raise FetchError("URL is empty or too long")
    if any(ord(char) < 0x20 for char in raw_url) or "\\" in raw_url:
        raise FetchError("URL contains unsafe characters")
    try:
        parsed = urlsplit(raw_url)
    except ValueError as exc:
        raise FetchError("invalid URL") from exc
    if parsed.scheme.lower() != "https":
        raise FetchError("only HTTPS URLs are allowed")
    if parsed.username is not None or parsed.password is not None:
        raise FetchError("credentials in URLs are not allowed")
    if parsed.fragment:
        raise FetchError("URL fragments are not allowed")
    host = _normalize_hostname(parsed.hostname or "")
    try:
        port = parsed.port or 443
    except ValueError as exc:
        raise FetchError("invalid URL port") from exc
    if port != 443:
        raise FetchError("only HTTPS port 443 is allowed")
    for key, _value in parse_qsl(parsed.query, keep_blank_values=True):
        if key.lower() in BLOCKED_QUERY_KEYS:
            raise FetchError("credential-like query parameters are not allowed")
    return parsed, host, port


def _require_global_ip(value: str) -> str:
    try:
        address = ipaddress.ip_address(value)
    except ValueError as exc:
        raise FetchError("DNS returned an invalid IP address") from exc
    if not address.is_global:
        raise FetchError("DNS resolved to a non-public IP address")
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
        raise FetchError("hostname resolution failed") from exc
    if not infos:
        raise FetchError("hostname did not resolve")

    addresses = []
    for info in infos:
        address = _require_global_ip(info[4][0])
        if address not in addresses:
            addresses.append(address)
    return addresses


class PinnedHTTPSConnection(http.client.HTTPSConnection):
    """TLS connection pinned to an already-vetted DNS result."""

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


def _tls_context() -> ssl.SSLContext:
    context = ssl.create_default_context()
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    return context


def _request_once(
    method: str,
    parsed,
    host: str,
    port: int,
    addresses: Iterable[str],
    context: ssl.SSLContext,
    connection_factory: Callable[..., http.client.HTTPSConnection],
):
    path = urlunsplit(("", "", parsed.path or "/", parsed.query, ""))
    last_error = None
    for address in addresses:
        connection = connection_factory(
            host,
            port,
            address,
            context,
            TIMEOUT_SECONDS,
        )
        try:
            connection.request(
                method,
                path,
                headers={
                    "Accept": "*/*",
                    "Connection": "close",
                    "User-Agent": USER_AGENT,
                },
            )
            return connection, connection.getresponse()
        except (OSError, ssl.SSLError, http.client.HTTPException) as exc:
            last_error = exc
            connection.close()
    raise FetchError("HTTPS connection failed for all public DNS results") from last_error


def fetch(
    method: str,
    initial_url: str,
    output: BinaryIO,
    resolver: Optional[Callable[[str, int], List[str]]] = None,
    connection_factory: Optional[Callable[..., http.client.HTTPSConnection]] = None,
) -> Tuple[str, int, List[Tuple[str, str]]]:
    if method not in {"GET", "HEAD"}:
        raise FetchError("only GET and HEAD are supported")
    resolver = resolver or resolve_public_addresses
    connection_factory = connection_factory or PinnedHTTPSConnection
    context = _tls_context()
    current_url = initial_url

    for redirect_count in range(MAX_REDIRECTS + 1):
        parsed, host, port = validate_url(current_url)
        addresses = resolver(host, port)
        if not addresses:
            raise FetchError("hostname did not resolve to a public address")
        connection, response = _request_once(
            method,
            parsed,
            host,
            port,
            addresses,
            context,
            connection_factory,
        )
        try:
            if response.status in REDIRECT_STATUSES:
                location = response.getheader("Location")
                if not location:
                    raise FetchError("redirect response omitted Location")
                if redirect_count >= MAX_REDIRECTS:
                    raise FetchError("too many redirects")
                current_url = urljoin(current_url, location)
                continue
            if response.status < 200 or response.status >= 300:
                raise FetchError("HTTPS server returned status {}".format(response.status))

            headers = list(response.getheaders())
            if method == "HEAD":
                return current_url, response.status, headers

            length = response.getheader("Content-Length")
            if length is not None:
                try:
                    declared_length = int(length)
                except ValueError as exc:
                    raise FetchError("invalid Content-Length") from exc
                if declared_length < 0 or declared_length > MAX_RESPONSE_BYTES:
                    raise FetchError("response exceeds the size limit")

            total = 0
            while True:
                chunk = response.read(64 * 1024)
                if not chunk:
                    break
                total += len(chunk)
                if total > MAX_RESPONSE_BYTES:
                    raise FetchError("response exceeds the size limit")
                output.write(chunk)
            return current_url, response.status, headers
        finally:
            connection.close()

    raise FetchError("too many redirects")


def _write_head(status: int, headers: Iterable[Tuple[str, str]]) -> None:
    sys.stdout.write("HTTP {}\n".format(status))
    for key, value in headers:
        safe_key = key.replace("\r", " ").replace("\n", " ")
        safe_value = value.replace("\r", " ").replace("\n", " ")
        sys.stdout.write("{}: {}\n".format(safe_key, safe_value))


def main(argv: Sequence[str]) -> int:
    output_path = None
    output_dir = None
    output_file = None
    try:
        spec = parse_cli(argv)
        if spec.output_name is not None:
            output_dir = tempfile.mkdtemp(prefix="pipeline-codex.fetch.", dir="/tmp")
            output_path = os.path.join(output_dir, spec.output_name)
            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
            if hasattr(os, "O_NOFOLLOW"):
                flags |= os.O_NOFOLLOW
            fd = os.open(output_path, flags, 0o600)
            output_file = os.fdopen(fd, "wb")
            sink = output_file
        else:
            sink = sys.stdout.buffer

        _final_url, status, headers = fetch(spec.method, spec.url, sink)
        if output_file is not None:
            output_file.close()
            output_file = None
            sys.stdout.write(output_path + "\n")
        elif spec.method == "HEAD":
            _write_head(status, headers)
        return 0
    except FetchError as exc:
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
        sys.stderr.write("pipeline-public-fetch: {}\n".format(exc))
        return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
