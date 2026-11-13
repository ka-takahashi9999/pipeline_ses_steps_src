#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
04-1_fetch_skillsheets_text: 要員メールからスキルシートテキストを取得

入力:
  - 01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl  (添付ファイル参照)
  - 01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl  (本文URL抽出)
  - 02-2_classify_output_file_project_resource/01_result/resources.jsonl  (処理対象)

出力:
  - 04-1_fetch_skillsheets_text/01_result/fetch_skillsheets_text.jsonl
  - 04-1_fetch_skillsheets_text/01_result/99_no_fetch_skillsheets_text.jsonl  (success=falseのみ)

取得優先順位:
  1. 添付ファイル (mail_master.jsonlのattachmentsを参照)
  2. URLから取得 (Google Drive > OneDrive > その他HTTP)
  3. すべて失敗時は success=false

LLM使用禁止
"""

import os
import sys
import re
import json
import base64
import signal
import shutil
import subprocess
import tempfile
import time
import html
import unicodedata
from dataclasses import dataclass
import requests
import warnings
import logging
import zipfile
import stat
import xml.etree.ElementTree as ET
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import parse_qs, parse_qsl, urlencode, urljoin, urlparse, urlunparse

# common モジュールのパスを追加
STEP_DIR = Path(__file__).resolve().parent.parent
PROJECT_ROOT = STEP_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

from common.logger import get_logger
from common.json_utils import read_jsonl_as_list, append_jsonl
from common.file_utils import write_error_log, write_execution_time

logger = get_logger("04-1_fetch_skillsheets_text")

# ライブラリ警告を抑制
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")
logging.getLogger("pdfminer").setLevel(logging.ERROR)
logging.getLogger("pdfplumber").setLevel(logging.ERROR)

# ---------------------------------------------------------------------------
# 定数・パス
# ---------------------------------------------------------------------------
RESULT_DIR = STEP_DIR / "01_result"
EXEC_TIME_DIR = STEP_DIR / "99_execution_time"
NO_FETCH_JSONL = RESULT_DIR / "99_no_fetch_skillsheets_text.jsonl"
OUTPUT_JSONL = RESULT_DIR / "fetch_skillsheets_text.jsonl"

INPUT_MAIL_MASTER = PROJECT_ROOT / "01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl"
INPUT_CLEANUP_EMAILS = (
    PROJECT_ROOT
    / "01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl"
)
INPUT_RESOURCES = (
    PROJECT_ROOT
    / "02-2_classify_output_file_project_resource/01_result/resources.jsonl"
)

# OneDrive ドメイン判定
ONEDRIVE_DOMAINS = {"onedrive.live.com", "1drv.ms", "sharepoint.com"}
CTSU_PUBLIC_DOMAINS = {"ctsu.jp", "cho-tatsu.com"}

# スキルシートらしいファイル拡張子
SKILLSHEET_EXTENSIONS = {"pdf", "xlsx", "xls", "docx", "doc"}
SAFE_SKILLSHEET_ARCHIVE_EXTENSION = "zip"
UNKNOWN_ATTACHMENT_MIME_TYPES = {
    "",
    "application/octet-stream",
    "binary/octet-stream",
}
MAX_OOXML_SNIFF_BASE64_CHARS = 40 * 1024 * 1024
MAX_OOXML_ZIP_ENTRIES = 4096
MAX_SAFE_SKILLSHEET_ZIP_BYTES = 40 * 1024 * 1024
MAX_SAFE_SKILLSHEET_ZIP_ENTRIES = 32
MAX_SAFE_SKILLSHEET_MEMBER_BYTES = 40 * 1024 * 1024
MAX_SAFE_SKILLSHEET_COMPRESSION_RATIO = 200

SHORTENER_DOMAINS = {
    "bit.ly",
    "tinyurl.com",
    "goo.gl",
    "ow.ly",
    "x.gd",
    "t.co",
    "buff.ly",
    "is.gd",
    "cutt.ly",
    "lnkd.in",
}

CONDITIONAL_HOST_DOMAINS = {
    "dropbox.com",
    "box.com",
    "d.bmb.jp",
    "a.bme.jp",
}

DIRECT_ADOPT_NG_RULES = (
    ("hm-f.jp", "/cc.php"),
)

TITLE_ONLY_TEXTS = {
    "スキルシート",
    "技術者経歴書",
    "経歴書",
    "職務経歴書",
}

LIST_SHEET_MARKERS = (
    "=== シート: 案件一覧リスト ===",
    "=== シート: 注力人材 ===",
    "=== シート: サマリ ===",
    "=== シート: 開発希望若手 ===",
)

URL_CONTEXT_POSITIVE_KEYWORDS = (
    "スキルシート",
    "職務経歴書",
    "経歴書",
    "Skill Sheet",
    "skillsheet",
)

URL_CONTEXT_NEGATIVE_KEYWORDS = (
    "要員リスト",
    "人材リスト",
    "一覧",
    "案件一覧",
    "配信停止",
    "会社HP",
    "URL :",
)

HTML_LINK_NEGATIVE_KEYWORDS = (
    "注力人材",
    "注力要員",
    "注力案件",
    "人材一覧",
    "要員一覧",
    "営業中人材",
    "営業中要員",
    "営業中の要員一覧",
    "案件一覧",
    "その他の人材",
    "その他の要員",
    "その他案件",
    "共有人材一覧",
    "共有人材",
)

ATTACHMENT_EXPLICIT_NEGATIVE_KEYWORDS = (
    "人材一覧",
    "要員一覧",
    "案件一覧",
    "会社案内",
    "会社概要",
    "資格証",
)

ATTACHMENT_STRONG_POSITIVE_KEYWORDS = (
    "スキルシート",
    "職務経歴書",
    "技術経歴書",
    "経歴書",
    "resume",
    "curriculum vitae",
)

ATTACHMENT_GENERIC_NEGATIVE_KEYWORDS = (
    "参考資料",
    "補足資料",
    "その他資料",
)

# HTTP ダウンロード共通ヘッダー
HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
}

# タイムアウト設定
TIMEOUT_GDRIVE = 30   # Google Drive はやや大きめ
TIMEOUT_OTHER = 10    # その他 URL は短め
EXTRACT_TIMEOUT_SECONDS = 30
MIN_TEXT_LENGTH = 30

EXCEL_NOISE_CELL_TEXTS = {
    "FALSE",
    "TRUE",
    "#REF!",
    "#VALUE!",
    "#DIV/0!",
    "#N/A",
    "#NAME?",
    "#NULL!",
    "#NUM!",
}

XLSX_CUSTOM_PROPERTIES = "docProps/custom.xml"
XLSX_BASE_REQUIRED_MEMBERS = {
    "[Content_Types].xml",
    "_rels/.rels",
    "xl/workbook.xml",
    "xl/_rels/workbook.xml.rels",
}
XLSX_REQUIRED_MEMBERS = {
    *XLSX_BASE_REQUIRED_MEMBERS,
    XLSX_CUSTOM_PROPERTIES,
}
CUSTOM_PROPERTIES_NS = (
    "http://schemas.openxmlformats.org/officeDocument/2006/custom-properties"
)
CUSTOM_PROPERTIES_VALUE_NS = (
    "http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes"
)
EXCEL_MAX_ROW = 1_048_576
EXCEL_MAX_COLUMN = 16_384
PATHOLOGICAL_DIMENSION_MIN_RATIO = 1_000
STRICT_SPREADSHEET_NS = "http://purl.oclc.org/ooxml/spreadsheetml/main"
STRICT_OFFICE_REL_NS = "http://purl.oclc.org/ooxml/officeDocument/relationships"
PACKAGE_REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"
STRICT_NAMESPACE_REPLACEMENTS = (
    (
        b"http://purl.oclc.org/ooxml/spreadsheetml/main",
        b"http://schemas.openxmlformats.org/spreadsheetml/2006/main",
    ),
    (
        b"http://purl.oclc.org/ooxml/officeDocument/relationships",
        b"http://schemas.openxmlformats.org/officeDocument/2006/relationships",
    ),
    (
        b"http://purl.oclc.org/ooxml/drawingml/main",
        b"http://schemas.openxmlformats.org/drawingml/2006/main",
    ),
    (
        b"http://purl.oclc.org/ooxml/officeDocument/extendedProperties",
        b"http://schemas.openxmlformats.org/officeDocument/2006/extended-properties",
    ),
    (
        b"http://purl.oclc.org/ooxml/officeDocument/docPropsVTypes",
        b"http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes",
    ),
)
_CELL_REFERENCE_RE = re.compile(r"^\$?([A-Z]{1,3})\$?([1-9]\d*)$")
_DIMENSION_REFERENCE_RE = re.compile(
    r"^\$?([A-Z]{1,3})\$?([1-9]\d*)(?::\$?([A-Z]{1,3})\$?([1-9]\d*))?$"
)


@dataclass
class DownloadResult:
    content: bytes
    final_url: str
    content_type: str
    filename: str = ""


def _base64url_encode(value: str) -> str:
    """URL-safe base64 をパディングなしで返す。"""
    return base64.urlsafe_b64encode(value.encode("utf-8")).decode("ascii").rstrip("=")

# ---------------------------------------------------------------------------
# テキスト抽出
# ---------------------------------------------------------------------------


class ExtractTimeoutError(TimeoutError):
    """ファイル抽出処理が規定秒数を超過した場合の例外。"""


def _extract_timeout_handler(signum: int, frame: Any) -> None:
    raise ExtractTimeoutError(f"抽出タイムアウト({EXTRACT_TIMEOUT_SECONDS}秒)")


def extract_text_from_bytes_with_timeout(data: bytes, filename: str = "") -> str:
    """抽出処理全体を SIGALRM でタイムアウト制御する。"""
    old_handler = signal.getsignal(signal.SIGALRM)
    signal.signal(signal.SIGALRM, _extract_timeout_handler)
    signal.alarm(EXTRACT_TIMEOUT_SECONDS)
    try:
        return extract_text_from_bytes(data, filename)
    except ExtractTimeoutError as e:
        raise ExtractTimeoutError(f"{e}: filename={filename}") from e
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old_handler)


def extract_text_from_pdf(data: bytes) -> str:
    """PDFバイナリからテキストを抽出する。"""
    import pdfplumber

    texts: List[str] = []
    with pdfplumber.open(BytesIO(data)) as pdf:
        for page in pdf.pages:
            t = page.extract_text()
            if t:
                texts.append(t)
    return "\n".join(texts)


def _normalize_excel_cell_text(value: Any) -> Optional[str]:
    """Excelセル値を抽出対象テキストへ正規化する。"""
    if value is None:
        return None
    if isinstance(value, bool):
        return None

    text = str(value).strip()
    if not text:
        return None
    if text.upper() in EXCEL_NOISE_CELL_TEXTS:
        return None
    return text


def _is_missing_custom_property_name_error(error: Exception) -> bool:
    """openpyxlのcustom property name欠損エラーだけを識別する。"""
    message = str(error)
    return (
        isinstance(error, TypeError)
        and "openpyxl.packaging.custom." in message
        and ".name should be <class 'str'>" in message
        and "value is <class 'NoneType'>" in message
    )


def _xml_local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _sanitize_missing_custom_property_names(data: bytes) -> Optional[bytes]:
    """
    必須OOXML構造が正常なXLSXからname属性欠損custom propertyだけを除外する。

    条件を満たさない場合はNoneを返す。原本bytesは変更しない。
    """
    try:
        with zipfile.ZipFile(BytesIO(data), "r") as source:
            entries = source.infolist()
            if len(entries) > MAX_OOXML_ZIP_ENTRIES or source.testzip() is not None:
                return None

            names = [entry.filename for entry in entries if not entry.is_dir()]
            if any(names.count(required) != 1 for required in XLSX_REQUIRED_MEMBERS):
                return None

            worksheet_names = [
                name
                for name in names
                if name.startswith("xl/worksheets/") and name.endswith(".xml")
            ]
            if not worksheet_names:
                return None

            content_types_root = ET.fromstring(source.read("[Content_Types].xml"))
            package_relationships_root = ET.fromstring(source.read("_rels/.rels"))
            workbook_root = ET.fromstring(source.read("xl/workbook.xml"))
            workbook_relationships_root = ET.fromstring(
                source.read("xl/_rels/workbook.xml.rels")
            )
            if (
                _xml_local_name(content_types_root.tag) != "Types"
                or _xml_local_name(package_relationships_root.tag) != "Relationships"
                or _xml_local_name(workbook_root.tag) != "workbook"
                or _xml_local_name(workbook_relationships_root.tag) != "Relationships"
                or not any(
                    _xml_local_name(element.tag) == "sheet"
                    for element in workbook_root.iter()
                )
            ):
                return None

            worksheet_roots = [
                ET.fromstring(source.read(name)) for name in worksheet_names
            ]
            if any(
                _xml_local_name(root.tag) != "worksheet"
                for root in worksheet_roots
            ):
                return None

            # その他のXML/relationshipも構文確認し、別破損を救済しない。
            already_parsed = XLSX_REQUIRED_MEMBERS | set(worksheet_names)
            for name in names:
                if name in already_parsed:
                    continue
                if name == "[Content_Types].xml" or name.endswith((".xml", ".rels")):
                    ET.fromstring(source.read(name))

            custom_root = ET.fromstring(source.read(XLSX_CUSTOM_PROPERTIES))
            if custom_root.tag != f"{{{CUSTOM_PROPERTIES_NS}}}Properties":
                return None

            properties = [
                child
                for child in list(custom_root)
                if child.tag == f"{{{CUSTOM_PROPERTIES_NS}}}property"
            ]
            missing_name = [
                prop for prop in properties if "name" not in prop.attrib
            ]
            if not missing_name:
                return None

            for prop in missing_name:
                custom_root.remove(prop)

            ET.register_namespace("", CUSTOM_PROPERTIES_NS)
            ET.register_namespace("vt", CUSTOM_PROPERTIES_VALUE_NS)
            sanitized_custom_xml = ET.tostring(
                custom_root,
                encoding="utf-8",
                xml_declaration=True,
            )

            sanitized_buffer = BytesIO()
            with zipfile.ZipFile(sanitized_buffer, "w") as destination:
                destination.comment = source.comment
                for entry in entries:
                    member_data = source.read(entry)
                    if entry.filename == XLSX_CUSTOM_PROPERTIES:
                        member_data = sanitized_custom_xml
                    destination.writestr(entry, member_data)
            return sanitized_buffer.getvalue()
    except (
        KeyError,
        OSError,
        RuntimeError,
        ValueError,
        ET.ParseError,
        zipfile.BadZipFile,
        zipfile.LargeZipFile,
    ):
        return None


def _excel_column_number(column_name: str) -> int:
    """Excel列名を1始まりの列番号へ変換する。"""
    number = 0
    for char in column_name:
        number = number * 26 + (ord(char) - ord("A") + 1)
    return number


def _excel_column_name(column_number: int) -> str:
    """1始まりの列番号をExcel列名へ変換する。"""
    chars: List[str] = []
    while column_number > 0:
        column_number, remainder = divmod(column_number - 1, 26)
        chars.append(chr(ord("A") + remainder))
    return "".join(reversed(chars))


def _parse_dimension_reference(reference: str) -> Optional[Tuple[int, int, int, int]]:
    """dimension refを(min_col, min_row, max_col, max_row)へ変換する。"""
    match = _DIMENSION_REFERENCE_RE.fullmatch(reference or "")
    if not match:
        return None
    start_col, start_row, end_col, end_row = match.groups()
    end_col = end_col or start_col
    end_row = end_row or start_row
    return (
        _excel_column_number(start_col),
        int(start_row),
        _excel_column_number(end_col),
        int(end_row),
    )


def _worksheet_has_pathological_dimension(worksheet: Any) -> bool:
    """openpyxlがExcel上限級の宣言dimensionを保持しているか確認する。"""
    return (
        (worksheet.max_row or 0) >= EXCEL_MAX_ROW
        or (worksheet.max_column or 0) >= EXCEL_MAX_COLUMN
    )


def _data_cell_bounds(worksheet_root: ET.Element) -> Optional[Tuple[int, int, int, int, int]]:
    """値・inline文字列・数式を持つcellだけから実データ範囲を返す。"""
    min_col: Optional[int] = None
    min_row: Optional[int] = None
    max_col = 0
    max_row = 0
    data_cell_count = 0

    for cell in worksheet_root.iter():
        if _xml_local_name(cell.tag) != "c":
            continue
        reference = cell.get("r") or ""
        match = _CELL_REFERENCE_RE.fullmatch(reference)
        if not match:
            continue
        if not any(
            _xml_local_name(child.tag) in {"v", "is", "f"}
            for child in list(cell)
        ):
            continue

        column = _excel_column_number(match.group(1))
        row = int(match.group(2))
        min_col = column if min_col is None else min(min_col, column)
        min_row = row if min_row is None else min(min_row, row)
        max_col = max(max_col, column)
        max_row = max(max_row, row)
        data_cell_count += 1

    if min_col is None or min_row is None:
        return None
    return min_col, min_row, max_col, max_row, data_cell_count


def _replace_worksheet_dimension(
    worksheet_xml: bytes, old_reference: str, new_reference: str
) -> Optional[bytes]:
    """dimension ref属性だけを置換し、その他のworksheet XMLを保持する。"""
    pattern = re.compile(
        rb'(<(?:[A-Za-z_][\w.-]*:)?dimension\b[^>]*\bref=["\'])'
        + re.escape(old_reference.encode("ascii"))
        + rb'(["\'])'
    )
    replaced, count = pattern.subn(
        lambda match: match.group(1) + new_reference.encode("ascii") + match.group(2),
        worksheet_xml,
        count=1,
    )
    return replaced if count == 1 else None


def _sanitize_pathological_worksheet_dimensions(data: bytes) -> Optional[bytes]:
    """
    Excel上限級かつ実データ範囲との乖離が極端なdimensionだけを補正する。

    元bytesは変更せず、条件成立時だけメモリ上のOOXMLコピーを返す。
    """
    try:
        with zipfile.ZipFile(BytesIO(data), "r") as source:
            entries = source.infolist()
            if len(entries) > MAX_OOXML_ZIP_ENTRIES or source.testzip() is not None:
                return None

            names = [entry.filename for entry in entries if not entry.is_dir()]
            if any(names.count(required) != 1 for required in XLSX_BASE_REQUIRED_MEMBERS):
                return None
            worksheet_names = [
                name
                for name in names
                if name.startswith("xl/worksheets/") and name.endswith(".xml")
            ]
            if not worksheet_names:
                return None

            workbook_root = ET.fromstring(source.read("xl/workbook.xml"))
            relationships_root = ET.fromstring(
                source.read("xl/_rels/workbook.xml.rels")
            )
            if (
                _xml_local_name(workbook_root.tag) != "workbook"
                or _xml_local_name(relationships_root.tag) != "Relationships"
            ):
                return None

            replacements: Dict[str, bytes] = {}
            for name in worksheet_names:
                worksheet_xml = source.read(name)
                worksheet_root = ET.fromstring(worksheet_xml)
                if _xml_local_name(worksheet_root.tag) != "worksheet":
                    return None

                dimension = next(
                    (
                        element
                        for element in worksheet_root.iter()
                        if _xml_local_name(element.tag) == "dimension"
                    ),
                    None,
                )
                if dimension is None:
                    continue
                old_reference = dimension.get("ref") or ""
                declared = _parse_dimension_reference(old_reference)
                if declared is None:
                    continue
                declared_min_col, declared_min_row, declared_max_col, declared_max_row = declared
                if (
                    declared_max_row < EXCEL_MAX_ROW
                    and declared_max_col < EXCEL_MAX_COLUMN
                ):
                    continue

                actual = _data_cell_bounds(worksheet_root)
                if actual is None:
                    continue
                actual_min_col, actual_min_row, actual_max_col, actual_max_row, _ = actual
                declared_area = (
                    (declared_max_col - declared_min_col + 1)
                    * (declared_max_row - declared_min_row + 1)
                )
                actual_area = (
                    (actual_max_col - actual_min_col + 1)
                    * (actual_max_row - actual_min_row + 1)
                )
                if actual_area <= 0 or declared_area < (
                    actual_area * PATHOLOGICAL_DIMENSION_MIN_RATIO
                ):
                    continue

                new_reference = "{}{}:{}{}".format(
                    _excel_column_name(actual_min_col),
                    actual_min_row,
                    _excel_column_name(actual_max_col),
                    actual_max_row,
                )
                if new_reference == old_reference:
                    continue
                replaced = _replace_worksheet_dimension(
                    worksheet_xml, old_reference, new_reference
                )
                if replaced is None:
                    return None
                replacements[name] = replaced

            if not replacements:
                return None

            sanitized_buffer = BytesIO()
            with zipfile.ZipFile(sanitized_buffer, "w") as destination:
                destination.comment = source.comment
                for entry in entries:
                    member_data = replacements.get(entry.filename, source.read(entry))
                    destination.writestr(entry, member_data)
            return sanitized_buffer.getvalue()
    except (
        KeyError,
        OSError,
        RuntimeError,
        ValueError,
        ET.ParseError,
        zipfile.BadZipFile,
        zipfile.LargeZipFile,
    ):
        return None


def _normalize_strict_ooxml_namespaces(data: bytes) -> Optional[bytes]:
    """構造が正常なStrict XLSXだけをTransitional namespaceのコピーへ変換する。"""
    try:
        with zipfile.ZipFile(BytesIO(data), "r") as source:
            entries = source.infolist()
            if len(entries) > MAX_OOXML_ZIP_ENTRIES or source.testzip() is not None:
                return None

            names = [entry.filename for entry in entries if not entry.is_dir()]
            if any(names.count(required) != 1 for required in XLSX_BASE_REQUIRED_MEMBERS):
                return None
            worksheet_names = [
                name
                for name in names
                if name.startswith("xl/worksheets/") and name.endswith(".xml")
            ]
            if not worksheet_names:
                return None

            content_types_root = ET.fromstring(source.read("[Content_Types].xml"))
            package_relationships_root = ET.fromstring(source.read("_rels/.rels"))
            workbook_root = ET.fromstring(source.read("xl/workbook.xml"))
            workbook_relationships_root = ET.fromstring(
                source.read("xl/_rels/workbook.xml.rels")
            )
            if (
                _xml_local_name(content_types_root.tag) != "Types"
                or _xml_local_name(package_relationships_root.tag) != "Relationships"
                or workbook_root.tag != f"{{{STRICT_SPREADSHEET_NS}}}workbook"
                or workbook_relationships_root.tag
                != f"{{{PACKAGE_REL_NS}}}Relationships"
            ):
                return None

            sheets = [
                element
                for element in workbook_root.iter()
                if element.tag == f"{{{STRICT_SPREADSHEET_NS}}}sheet"
            ]
            relationship_by_id = {
                element.get("Id"): element
                for element in workbook_relationships_root
                if _xml_local_name(element.tag) == "Relationship"
            }
            if not sheets:
                return None
            for sheet in sheets:
                relationship_id = sheet.get(f"{{{STRICT_OFFICE_REL_NS}}}id")
                relationship = relationship_by_id.get(relationship_id)
                if relationship is None:
                    return None
                if relationship.get("Type") != (
                    f"{STRICT_OFFICE_REL_NS}/worksheet"
                ):
                    return None
                target = (relationship.get("Target") or "").lstrip("/")
                if not target or ".." in target.split("/"):
                    return None
                member_name = target if target.startswith("xl/") else f"xl/{target}"
                if member_name not in worksheet_names:
                    return None

            for name in names:
                if name == "[Content_Types].xml" or name.endswith((".xml", ".rels")):
                    root = ET.fromstring(source.read(name))
                    if name in worksheet_names and root.tag != (
                        f"{{{STRICT_SPREADSHEET_NS}}}worksheet"
                    ):
                        return None

            sanitized_buffer = BytesIO()
            with zipfile.ZipFile(sanitized_buffer, "w") as destination:
                destination.comment = source.comment
                for entry in entries:
                    member_data = source.read(entry)
                    if entry.filename.endswith((".xml", ".rels")):
                        for strict_namespace, transitional_namespace in (
                            STRICT_NAMESPACE_REPLACEMENTS
                        ):
                            member_data = member_data.replace(
                                strict_namespace, transitional_namespace
                            )
                    destination.writestr(entry, member_data)
            return sanitized_buffer.getvalue()
    except (
        KeyError,
        OSError,
        RuntimeError,
        ValueError,
        ET.ParseError,
        zipfile.BadZipFile,
        zipfile.LargeZipFile,
    ):
        return None


def _load_xlsx_workbook(data: bytes) -> Any:
    """通常loadを優先し、限定条件成立時だけsanitized copyを再読込する。"""
    import openpyxl

    try:
        workbook = openpyxl.load_workbook(
            BytesIO(data), data_only=True, read_only=True
        )
    except Exception as error:
        if _is_missing_custom_property_name_error(error):
            sanitized = _sanitize_missing_custom_property_names(data)
            if sanitized is None:
                raise
            return openpyxl.load_workbook(
                BytesIO(sanitized), data_only=True, read_only=True
            )
        strict_copy = _normalize_strict_ooxml_namespaces(data)
        if strict_copy is None:
            raise
        workbook = openpyxl.load_workbook(
            BytesIO(strict_copy), data_only=True, read_only=True
        )
        if not workbook.sheetnames:
            raise ValueError("Strict OOXML変換後もworksheetを取得できません")
        logger.info("Strict OOXML namespace fallbackを適用")
        return workbook

    if workbook.sheetnames:
        return workbook

    strict_copy = _normalize_strict_ooxml_namespaces(data)
    if strict_copy is None:
        return workbook
    strict_workbook = openpyxl.load_workbook(
        BytesIO(strict_copy), data_only=True, read_only=True
    )
    if not strict_workbook.sheetnames:
        raise ValueError("Strict OOXML変換後もworksheetを取得できません")
    logger.info("Strict OOXML namespace fallbackを適用")
    return strict_workbook


def extract_text_from_excel(data: bytes, filename: str = "") -> str:
    """Excel (.xlsx / .xls) バイナリからテキストを抽出する。"""
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""

    # .xls (古い形式)
    if ext == "xls" and data[:4] != b"PK\x03\x04":
        import xlrd

        wb = xlrd.open_workbook(file_contents=data)
        rows: List[str] = []
        for sname in wb.sheet_names():
            sheet = wb.sheet_by_name(sname)
            rows.append(f"=== シート: {sname} ===")
            for ri in range(sheet.nrows):
                cells = []
                for ci in range(sheet.ncols):
                    cell_text = _normalize_excel_cell_text(sheet.cell(ri, ci).value)
                    if cell_text is not None:
                        cells.append(cell_text)
                if cells:
                    rows.append(" | ".join(cells))
        return "\n".join(rows)

    # .xlsx (ZIP形式)
    wb = _load_xlsx_workbook(data)
    if any(_worksheet_has_pathological_dimension(sheet) for sheet in wb.worksheets):
        sanitized = _sanitize_pathological_worksheet_dimensions(data)
        if sanitized is not None:
            logger.info(f"異常worksheet dimension fallbackを適用: {filename}")
            wb = _load_xlsx_workbook(sanitized)
    rows = []
    for sname in wb.sheetnames:
        sheet = wb[sname]
        rows.append(f"=== シート: {sname} ===")
        for row in sheet.iter_rows(values_only=True):
            cells = []
            for cell_value in row:
                cell_text = _normalize_excel_cell_text(cell_value)
                if cell_text is not None:
                    cells.append(cell_text)
            if cells:
                rows.append(" | ".join(cells))
    return "\n".join(rows)


def extract_text_from_word(data: bytes) -> str:
    """Word (.docx) バイナリからテキストを抽出する。"""
    import docx

    doc = docx.Document(BytesIO(data))
    parts: List[str] = []
    for para in doc.paragraphs:
        if para.text.strip():
            parts.append(para.text)
    for table in doc.tables:
        for row in table.rows:
            cells = [c.text.strip() for c in row.cells if c.text.strip()]
            if cells:
                parts.append(" | ".join(cells))
    return "\n".join(parts)


def _list_ole_streams(data: bytes) -> List[str]:
    """OLE2 コンテナのストリーム名一覧を返す。"""
    import olefile

    with olefile.OleFileIO(BytesIO(data)) as ole:
        return [entry[-1] for entry in ole.listdir()]


def detect_ole_office_type(data: bytes, filename: str = "") -> Tuple[Optional[str], str]:
    """
    OLE2 Office バイナリを Word / Excel に判定する。
    戻り値: ("word" | "excel" | None, detail)
    """
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    if ext == "doc":
        return "word", "拡張子 .doc"
    if ext == "xls":
        return "excel", "拡張子 .xls"

    try:
        streams = _list_ole_streams(data)
    except Exception as e:
        return None, f"OLEストリーム確認失敗: {e}"

    lowered = {name.lower() for name in streams}
    if "worddocument" in lowered:
        return "word", "OLEストリーム WordDocument"
    if "workbook" in lowered or "book" in lowered:
        return "excel", "OLEストリーム Workbook/Book"
    return None, f"OLEストリーム種別不明: {streams}"


def _run_legacy_word_command(args: List[str], data: bytes, suffix: str = ".doc") -> str:
    """旧Word抽出コマンドを一時ファイル経由で実行する。"""
    fd, path = tempfile.mkstemp(suffix=suffix)
    os.close(fd)
    try:
        with open(path, "wb") as wf:
            wf.write(data)
        completed = subprocess.run(
            args + [path],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="ignore",
            check=False,
        )
        if completed.returncode != 0:
            stderr = (completed.stderr or "").strip()
            raise ValueError(stderr or f"returncode={completed.returncode}")
        return completed.stdout
    finally:
        try:
            os.remove(path)
        except OSError:
            pass


def extract_text_from_legacy_word(data: bytes, filename: str = "") -> str:
    """旧Word (.doc) バイナリからテキストを抽出する。"""
    command_candidates = [
        ("antiword", ["antiword"]),
        ("catdoc", ["catdoc"]),
        ("wvText", ["wvText"]),
    ]
    for label, args in command_candidates:
        if shutil.which(args[0]):
            try:
                text = _run_legacy_word_command(args, data, suffix=".doc")
                if text.strip():
                    return text
                raise ValueError("抽出テキストが空")
            except Exception as e:
                raise ValueError(f"旧Word(.doc)抽出失敗[{label}]: {e}") from e

    if shutil.which("strings"):
        try:
            text = _run_legacy_word_command(["strings", "-e", "l", "-n", "4"], data, suffix=".doc")
            cleaned_lines = []
            skip_terms = {
                "Root Entry",
                "WordDocument",
                "1Table",
                "SummaryInformation",
                "DocumentSummaryInformation",
                "CompObj",
            }
            for line in text.splitlines():
                stripped = line.strip()
                if not stripped:
                    continue
                if stripped in skip_terms:
                    continue
                if re.fullmatch(r"0{4,}", stripped):
                    continue
                cleaned_lines.append(stripped)
            cleaned = "\n".join(cleaned_lines)
            if cleaned.strip():
                return cleaned
            raise ValueError("抽出テキストが空")
        except Exception as e:
            raise ValueError(f"旧Word(.doc)抽出失敗[strings]: {e}") from e

    raise ValueError("旧Word(.doc)抽出未対応: 利用可能コマンドなし")


def extract_text_from_bytes(data: bytes, filename: str = "") -> str:
    """ファイル形式を自動判定してテキストを抽出する。"""
    header = data[:4]
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""

    if header == b"%PDF":
        return extract_text_from_pdf(data)

    if header == b"PK\x03\x04":
        # ZIP: Excel か Word か拡張子で判定
        if ext in ("xlsx", "xls"):
            return extract_text_from_excel(data, filename)
        if ext == "docx":
            return extract_text_from_word(data)
        # 拡張子不明 → Excel を先に試す
        try:
            return extract_text_from_excel(data, filename)
        except ExtractTimeoutError:
            raise
        except Exception:
            return extract_text_from_word(data)

    # OLE2 Office (旧Word / 旧Excel)
    if header == b"\xd0\xcf\x11\xe0":
        ole_type, detail = detect_ole_office_type(data, filename)
        if ole_type == "word":
            return extract_text_from_legacy_word(data, filename)
        if ole_type == "excel":
            return extract_text_from_excel(data, filename)
        raise ValueError(f"OLE2形式判定失敗: {detail}")

    if ext == "doc":
        return extract_text_from_legacy_word(data, filename)

    if ext == "xls":
        return extract_text_from_excel(data, filename)

    # テキストとして読む
    try:
        decoded = data.decode("utf-8")
        if decoded.strip():
            return decoded
    except UnicodeDecodeError:
        pass
    try:
        decoded = data.decode("cp932")
        if decoded.strip():
            return decoded
    except UnicodeDecodeError:
        pass

    raise ValueError(f"サポートされていないファイル形式: filename={filename!r}, header={header!r}")


def extract_safe_single_file_zip(data: bytes) -> Tuple[bytes, str]:
    """安全な単一ファイルZIPから既存対応形式の1ファイルだけを返す。"""
    if len(data) > MAX_SAFE_SKILLSHEET_ZIP_BYTES:
        raise ValueError("ZIPアーカイブがサイズ上限を超えています")

    try:
        with zipfile.ZipFile(BytesIO(data)) as archive:
            entries = archive.infolist()
            if len(entries) > MAX_SAFE_SKILLSHEET_ZIP_ENTRIES:
                raise ValueError("ZIP entry数が上限を超えています")

            file_entries = []
            for entry in entries:
                normalized_name = (entry.filename or "").replace("\\", "/")
                path_name = normalized_name.rstrip("/") if entry.is_dir() else normalized_name
                parts = path_name.split("/")
                if (
                    not path_name
                    or normalized_name.startswith("/")
                    or re.match(r"^[A-Za-z]:", normalized_name)
                    or any(part in ("", ".", "..") for part in parts)
                ):
                    raise ValueError(f"ZIP内pathが安全ではありません: {entry.filename!r}")

                mode = (entry.external_attr >> 16) & 0xFFFF
                if stat.S_ISLNK(mode):
                    raise ValueError(f"ZIP内symlinkは対象外です: {entry.filename!r}")
                if entry.flag_bits & 0x1:
                    raise ValueError("暗号化ZIPまたはpassword付きZIPは対象外です")
                if not entry.is_dir():
                    file_entries.append(entry)

            if len(file_entries) != 1:
                raise ValueError(
                    f"ZIP内の非directory file数が1件ではありません: {len(file_entries)}"
                )

            member = file_entries[0]
            member_name = member.filename.replace("\\", "/")
            member_ext = (
                member_name.rsplit(".", 1)[-1].lower()
                if "." in member_name
                else ""
            )
            if member_ext not in SKILLSHEET_EXTENSIONS:
                raise ValueError(
                    f"ZIP内ファイル形式が既存対応外です: {member_name!r}"
                )
            if member.file_size > MAX_SAFE_SKILLSHEET_MEMBER_BYTES:
                raise ValueError("ZIP内ファイルがサイズ上限を超えています")
            compression_ratio = member.file_size / max(member.compress_size, 1)
            if compression_ratio > MAX_SAFE_SKILLSHEET_COMPRESSION_RATIO:
                raise ValueError("ZIP圧縮率が安全上限を超えています")

            extracted = archive.read(member)
            if len(extracted) != member.file_size:
                raise ValueError("ZIP内ファイルの展開サイズがmetadataと一致しません")
            return extracted, member_name
    except (zipfile.BadZipFile, zipfile.LargeZipFile) as error:
        raise ValueError(f"ZIPファイルを安全に展開できません: {error}") from error


# ---------------------------------------------------------------------------
# 添付ファイル処理
# ---------------------------------------------------------------------------


def extract_from_attachment(attachment: Dict[str, Any]) -> str:
    """
    Gmail添付ファイル辞書からテキストを抽出する。
    data フィールドは base64url エンコード済み。
    """
    data_b64 = attachment.get("data", "")
    if not data_b64:
        raise ValueError("attachment.data が空です")

    # base64url デコード（パディング調整）
    padded = data_b64 + "=" * (-len(data_b64) % 4)
    raw_bytes = base64.urlsafe_b64decode(padded)

    filename = attachment.get("filename", "")
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    if ext == SAFE_SKILLSHEET_ARCHIVE_EXTENSION:
        member_bytes, member_name = extract_safe_single_file_zip(raw_bytes)
        return extract_text_from_bytes_with_timeout(member_bytes, member_name)
    return extract_text_from_bytes_with_timeout(raw_bytes, filename)


# ---------------------------------------------------------------------------
# URL 抽出・分類
# ---------------------------------------------------------------------------


def extract_urls_from_text(text: str) -> List[str]:
    """テキスト中の https:// を含む URL をすべて抽出する。"""
    pattern = r'https?://[^\s\)\]\}\"\'<>]+'
    raw_urls = re.findall(pattern, text)
    # 末尾の不要な記号をトリム
    trailing = set(")]}>\"'.,;:!?")
    cleaned = []
    for u in raw_urls:
        while u and u[-1] in trailing:
            u = u[:-1]
        if u:
            cleaned.append(u)
    return cleaned


def classify_url(url: str) -> str:
    """
    URL を source 種別に分類する。
    戻り値: "google_drive" | "one_drive" | "ctsu_public_talent" | "other"
    """
    lower = url.lower()
    if "drive.google.com" in lower or "docs.google.com" in lower:
        return "google_drive"
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    if is_ctsu_public_talent_url(url):
        return "ctsu_public_talent"
    for d in ONEDRIVE_DOMAINS:
        if host_matches(host, d):
            return "one_drive"
    return "other"


def host_matches(host: str, domain: str) -> bool:
    """ホストがドメインと一致またはサブドメインかを判定する。"""
    return host == domain or host.endswith(f".{domain}")


def get_url_extension(url: str) -> str:
    """URLパスからファイル拡張子を抽出する。"""
    path = urlparse(url).path.lower()
    if "." not in path.rsplit("/", 1)[-1]:
        return ""
    return path.rsplit(".", 1)[-1]


def is_google_docs_url(url: str) -> bool:
    """Google Drive / Docs のURLかを判定する。"""
    lower = url.lower()
    return "drive.google.com/file/d/" in lower or "docs.google.com/" in lower


def is_cloudinary_raw_url(url: str) -> bool:
    """Cloudinary raw/upload URL かを判定する。"""
    lower = url.lower()
    parsed = urlparse(url)
    return host_matches(parsed.netloc.lower(), "res.cloudinary.com") and "/raw/upload/" in lower


def is_direct_adopt_ng_url(url: str) -> bool:
    """直接採用してはいけないURLかを判定する。"""
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    path = parsed.path.lower()
    for domain, path_prefix in DIRECT_ADOPT_NG_RULES:
        if host_matches(host, domain) and path.startswith(path_prefix):
            return True
    return False


def is_ctsu_public_talent_url(url: str) -> bool:
    """CTSU公開人材ページURLかを判定する。"""
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    path = parsed.path.lower()
    return any(host_matches(host, domain) for domain in CTSU_PUBLIC_DOMAINS) and path.startswith("/public-talents/")


def classify_url_candidate(url: str) -> str:
    """
    URL候補を分類する。
    戻り値:
      - google_drive
      - one_drive
      - ctsu_public_talent
      - cloudinary_raw
      - extension_file
      - conditional
      - direct_adopt_ng
    """
    parsed = urlparse(url)
    host = parsed.netloc.lower()

    if is_google_docs_url(url):
        return "google_drive"

    for d in ONEDRIVE_DOMAINS:
        if host_matches(host, d):
            return "one_drive"

    if is_ctsu_public_talent_url(url):
        return "ctsu_public_talent"

    if is_cloudinary_raw_url(url):
        return "cloudinary_raw"

    if get_url_extension(url) in SKILLSHEET_EXTENSIONS:
        return "extension_file"

    if is_direct_adopt_ng_url(url):
        return "direct_adopt_ng"

    for d in SHORTENER_DOMAINS | CONDITIONAL_HOST_DOMAINS:
        if host_matches(host, d):
            return "conditional"

    if host:
        return "conditional"

    return "direct_adopt_ng"


def _url_context_score(body_text: str, url: str, window_size: int = 100) -> int:
    """本文上で URL の近くにある語から優先度スコアを返す。"""
    if not body_text or not url:
        return 0

    scores: List[int] = []
    start = 0
    while True:
        index = body_text.find(url, start)
        if index < 0:
            break
        window_start = max(0, index - window_size)
        window_end = min(len(body_text), index + len(url) + window_size)
        context = body_text[window_start:window_end]
        context_lower = context.lower()

        score = 0
        for keyword in URL_CONTEXT_POSITIVE_KEYWORDS:
            if keyword.lower() in context_lower:
                score += 10
        for keyword in URL_CONTEXT_NEGATIVE_KEYWORDS:
            if keyword.lower() in context_lower:
                score -= 10
        scores.append(score)
        start = index + len(url)

    return max(scores) if scores else 0


def sort_urls_by_priority(urls: List[str], body_text: str = "") -> List[Tuple[str, str]]:
    """
    URL リストを (url, category) のタプルリストにして優先順位順に並べ替える。
    優先:
      google_drive > one_drive > ctsu_public_talent > cloudinary_raw > extension_file > conditional > direct_adopt_ng
    同一種別内では本文上の URL 近傍文脈を優先する。
    """
    priority = {
        "google_drive": 0,
        "one_drive": 1,
        "ctsu_public_talent": 2,
        "cloudinary_raw": 3,
        "extension_file": 4,
        "conditional": 5,
        "direct_adopt_ng": 6,
    }
    tagged = [
        (u, classify_url_candidate(u), _url_context_score(body_text, u), i)
        for i, u in enumerate(urls)
    ]
    tagged.sort(key=lambda x: (priority[x[1]], -x[2], x[3]))
    # 重複 URL を除去（同じ URL が複数あっても1回だけ試す）
    seen: Set[str] = set()
    deduped = []
    for u, s, _score, _index in tagged:
        if u not in seen:
            seen.add(u)
            deduped.append((u, s))
    return deduped


def is_skillsheet_link_text(text: str) -> bool:
    """HTML anchorがpositiveに一致し、negativeには一致しないか判定する。"""
    normalized = unicodedata.normalize("NFKC", text or "").casefold()
    if is_auxiliary_link_text(normalized):
        return False
    return any(keyword.casefold() in normalized for keyword in URL_CONTEXT_POSITIVE_KEYWORDS)


def is_auxiliary_link_text(text: str) -> bool:
    """HTML anchor textがHIGH_CONFIDENCE auxiliary / negativeか判定する。"""
    normalized = unicodedata.normalize("NFKC", text or "").casefold()
    return any(keyword.casefold() in normalized for keyword in HTML_LINK_NEGATIVE_KEYWORDS)


ADJACENT_SKILLSHEET_CONTEXT_KEYWORDS = (
    "スキルシート",
    "経歴書",
    "職務経歴書",
    "技術経歴書",
)
ADJACENT_SKILLSHEET_CONTEXT_WINDOW = 50
RAW_ONLY_SKILLSHEET_CONTEXT_KEYWORDS = tuple(dict.fromkeys(
    URL_CONTEXT_POSITIVE_KEYWORDS
    + ADJACENT_SKILLSHEET_CONTEXT_KEYWORDS
    + ("Resume",)
))
RAW_ONLY_NEGATIVE_CONTEXT_KEYWORDS = tuple(dict.fromkeys(
    URL_CONTEXT_NEGATIVE_KEYWORDS
    + HTML_LINK_NEGATIVE_KEYWORDS
    + (
        "会社案内",
        "ホームページ",
        "会社サイト",
        "広告",
        "portal",
        "一覧表",
        "営業リスト",
        "unsubscribe",
    )
))
GENERIC_HTML_ANCHOR_TEXTS = {
    "こちら",
    "詳細",
    "url",
    "link",
    "ホームページ",
    "会社サイト",
    "line",
    "お問い合わせ",
}
CONTEXTUAL_ANCHOR_NEGATIVE_DOMAINS = {
    "line.me",
    "facebook.com",
    "instagram.com",
    "twitter.com",
    "x.com",
}
CONTEXTUAL_ANCHOR_NEGATIVE_URL_MARKERS = (
    "unsubscribe",
    "optout",
    "opt-out",
)
HIGH_CONFIDENCE_URL_CATEGORIES = {
    "google_drive",
    "one_drive",
    "ctsu_public_talent",
    "cloudinary_raw",
    "extension_file",
}


def is_generic_html_anchor_text(text: str) -> bool:
    """周辺文脈だけで用途を特定できないgeneric anchorか判定する。"""
    normalized = unicodedata.normalize("NFKC", text or "").strip().casefold()
    normalized = re.sub(r"[\s　:：・\-_/]+", "", normalized)
    normalized = re.sub(r"(?:はこちら|へ)$", "", normalized)
    return normalized in GENERIC_HTML_ANCHOR_TEXTS


def is_contextual_anchor_url_excluded(url: str) -> bool:
    """隣接文脈fallbackで新規routingしない明示negative URLを判定する。"""
    parsed = urlparse((url or "").strip())
    if parsed.scheme.lower() not in ("http", "https"):
        return True
    host = parsed.netloc.lower()
    if any(host_matches(host, domain) for domain in CONTEXTUAL_ANCHOR_NEGATIVE_DOMAINS):
        return True
    normalized_url = unicodedata.normalize("NFKC", url or "").casefold()
    return any(marker in normalized_url for marker in CONTEXTUAL_ANCHOR_NEGATIVE_URL_MARKERS)


def has_adjacent_skillsheet_context(body_text: str, anchor_text: str) -> bool:
    """同一行のanchor直前50文字以内にskillsheet明示語があるか確認する。"""
    body = unicodedata.normalize("NFKC", body_text or "")
    anchor = unicodedata.normalize("NFKC", anchor_text or "").strip()
    if not body or not anchor:
        return False

    start = 0
    while True:
        index = body.find(anchor, start)
        if index < 0:
            return False
        line_start = body.rfind("\n", 0, index) + 1
        window_start = max(line_start, index - ADJACENT_SKILLSHEET_CONTEXT_WINDOW)
        context = body[window_start:index].casefold()
        if any(keyword.casefold() in context for keyword in ADJACENT_SKILLSHEET_CONTEXT_KEYWORDS):
            return True
        start = index + len(anchor)


def extract_contextual_skillsheet_urls_from_html_links(
    mail_record: Optional[Dict[str, Any]],
    cleanup_record: Optional[Dict[str, Any]],
    excluded_urls: Optional[Set[str]] = None,
) -> List[str]:
    """明示文脈に隣接する非generic anchorだけを限定fallback候補として返す。"""
    body_text = (cleanup_record or {}).get("body_text", "") or ""
    excluded = excluded_urls or set()
    urls: List[str] = []
    for link in (mail_record or {}).get("html_links") or []:
        if not isinstance(link, dict):
            continue
        href = (link.get("href") or "").strip()
        anchor_text = (link.get("text") or "").strip()
        if not href or href in excluded:
            continue
        if is_auxiliary_link_text(anchor_text) or is_generic_html_anchor_text(anchor_text):
            continue
        if is_contextual_anchor_url_excluded(href):
            continue
        if has_adjacent_skillsheet_context(body_text, anchor_text):
            urls.append(href)
    return urls


def extract_auxiliary_hrefs_from_html_links(
    mail_record: Optional[Dict[str, Any]],
) -> Set[str]:
    """negative HTML anchorからsource横断deny対象hrefを抽出する。"""
    auxiliary_hrefs: Set[str] = set()
    for link in (mail_record or {}).get("html_links") or []:
        if not isinstance(link, dict):
            continue
        href = (link.get("href") or "").strip()
        if href and is_auxiliary_link_text(link.get("text", "") or ""):
            auxiliary_hrefs.add(href)
    return auxiliary_hrefs


def is_safe_raw_only_skillsheet_url(body_text: str, url: str) -> bool:
    """cleanupで落ちたURLのうち、明示的な個人資料候補だけを救済する。"""
    if classify_url_candidate(url) not in HIGH_CONFIDENCE_URL_CATEGORIES:
        return False
    if is_contextual_anchor_url_excluded(url):
        return False

    normalized_body = unicodedata.normalize("NFKC", body_text or "")
    normalized_url = unicodedata.normalize("NFKC", url or "")
    start = 0
    while normalized_url:
        index = normalized_body.find(normalized_url, start)
        if index < 0:
            break
        window_start = max(0, index - 100)
        line_end = normalized_body.find("\n", index + len(normalized_url))
        window_end = len(normalized_body) if line_end < 0 else line_end
        context = (
            normalized_body[window_start:index]
            + normalized_body[index + len(normalized_url):window_end]
        ).casefold()
        has_positive = any(
            keyword.casefold() in context
            for keyword in RAW_ONLY_SKILLSHEET_CONTEXT_KEYWORDS
        )
        has_negative = any(
            keyword.casefold() in context
            for keyword in RAW_ONLY_NEGATIVE_CONTEXT_KEYWORDS
        )
        if has_positive and not has_negative:
            return True
        start = index + len(normalized_url)
    return False


def extract_skillsheet_urls_from_html_links(
    mail_record: Optional[Dict[str, Any]],
    excluded_urls: Optional[Set[str]] = None,
) -> List[str]:
    """mail_master.html_links からスキルシート系リンクの href を出現順に抽出する。"""
    html_links = (mail_record or {}).get("html_links") or []
    urls: List[str] = []
    excluded = excluded_urls or set()
    for link in html_links:
        if not isinstance(link, dict):
            continue
        href = (link.get("href") or "").strip()
        if not href:
            continue
        if href in excluded:
            continue
        if is_skillsheet_link_text(link.get("text", "") or ""):
            urls.append(href)
    return urls


def build_url_candidates(
    mail_record: Optional[Dict[str, Any]],
    cleanup_record: Optional[Dict[str, Any]],
) -> Tuple[List[Tuple[str, str]], List[str], List[str]]:
    """
    URL候補を構築する。
    HTMLリンク由来のスキルシート系 href を本文URLより優先し、同じURLは1回だけ試す。
    """
    body_text = (cleanup_record or {}).get("body_text", "") or ""
    body_urls = extract_urls_from_text(body_text)
    raw_body_text = (mail_record or {}).get("body_text", "") or ""
    raw_body_urls = extract_urls_from_text(raw_body_text)
    removed_body_urls = set(raw_body_urls) - set(body_urls)
    explicit_auxiliary_hrefs = extract_auxiliary_hrefs_from_html_links(mail_record)
    rescued_raw_urls = [
        url
        for url in raw_body_urls
        if url in removed_body_urls
        and url not in explicit_auxiliary_hrefs
        and is_safe_raw_only_skillsheet_url(raw_body_text, url)
    ]
    rescued_raw_url_set = set(rescued_raw_urls)
    auxiliary_hrefs = (
        (removed_body_urls - rescued_raw_url_set) | explicit_auxiliary_hrefs
    )
    html_urls = extract_skillsheet_urls_from_html_links(mail_record, auxiliary_hrefs)
    eligible_body_urls = [url for url in body_urls if url not in auxiliary_hrefs]

    # CHECK-05限定: 既存の高信頼候補がない場合だけ、本文上のskillsheet明示語に
    # 隣接する非generic HTML anchorをfallback候補へ追加する。
    has_high_confidence_candidate = (
        any(is_eligible_attachment(att) for att in (mail_record or {}).get("attachments") or [])
        or bool(html_urls)
        or bool(rescued_raw_urls)
        or any(
            classify_url_candidate(url) in HIGH_CONFIDENCE_URL_CATEGORIES
            for url in eligible_body_urls
        )
    )
    if not has_high_confidence_candidate:
        for url in extract_contextual_skillsheet_urls_from_html_links(
            mail_record, cleanup_record, auxiliary_hrefs
        ):
            if url not in html_urls:
                html_urls.append(url)

    ordered: List[Tuple[str, str]] = []
    seen: Set[str] = set()
    for url in html_urls:
        if url in seen:
            continue
        seen.add(url)
        ordered.append((url, classify_url_candidate(url)))

    for url in rescued_raw_urls:
        if url in seen:
            continue
        seen.add(url)
        ordered.append((url, classify_url_candidate(url)))

    for url, category in sort_urls_by_priority(eligible_body_urls, body_text):
        if url in seen:
            continue
        seen.add(url)
        ordered.append((url, category))

    return ordered, html_urls, rescued_raw_urls + eligible_body_urls


def detect_ooxml_attachment_type(attachment: Dict[str, Any]) -> Optional[str]:
    """形式不明の取得済み添付をOOXML内部構造だけでxlsx/docx相当へ限定判定する。"""
    filename = unicodedata.normalize(
        "NFKC", attachment.get("filename", "") or ""
    ).casefold()
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    mime = (attachment.get("mime_type", "") or "").lower().strip()
    data_b64 = attachment.get("data", "")

    if ext in SKILLSHEET_EXTENSIONS:
        return None
    if mime not in UNKNOWN_ATTACHMENT_MIME_TYPES:
        return None
    if not isinstance(data_b64, str) or not data_b64:
        return None
    if len(data_b64) > MAX_OOXML_SNIFF_BASE64_CHARS:
        return None

    try:
        padded = data_b64 + "=" * (-len(data_b64) % 4)
        raw_bytes = base64.b64decode(padded, altchars=b"-_", validate=True)
        with zipfile.ZipFile(BytesIO(raw_bytes)) as archive:
            entries = archive.infolist()
            if len(entries) > MAX_OOXML_ZIP_ENTRIES:
                return None
            names = {entry.filename for entry in entries if not entry.is_dir()}
    except (ValueError, OSError, zipfile.BadZipFile, zipfile.LargeZipFile):
        return None

    if "[Content_Types].xml" not in names:
        return None
    has_excel = "xl/workbook.xml" in names
    has_word = "word/document.xml" in names
    if has_excel == has_word:
        return None
    return "xlsx" if has_excel else "docx"


def is_eligible_attachment(attachment: Dict[str, Any]) -> bool:
    """現行対応形式のうち、明示的に無関係な添付をskillsheet候補外にする。"""
    filename = unicodedata.normalize(
        "NFKC", attachment.get("filename", "") or ""
    ).casefold()
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    mime = (attachment.get("mime_type", "") or "").lower()
    is_supported = ext in SKILLSHEET_EXTENSIONS or ext == SAFE_SKILLSHEET_ARCHIVE_EXTENSION or any(
        keyword in mime
        for keyword in ("pdf", "excel", "word", "spreadsheet", "document", "officedocument")
    )
    if not is_supported:
        is_supported = detect_ooxml_attachment_type(attachment) is not None
    if not is_supported:
        return False

    if any(keyword.casefold() in filename for keyword in ATTACHMENT_EXPLICIT_NEGATIVE_KEYWORDS):
        return False

    has_strong_positive = any(
        keyword.casefold() in filename
        for keyword in ATTACHMENT_STRONG_POSITIVE_KEYWORDS
    ) or re.search(r"(?<![a-z0-9])cv(?![a-z0-9])", filename) is not None
    if has_strong_positive:
        return True

    if any(keyword.casefold() in filename for keyword in ATTACHMENT_GENERIC_NEGATIVE_KEYWORDS):
        return False

    return True


def is_probable_file_url(url: str) -> bool:
    """URL文字列から実ファイルURLらしさを判定する。"""
    return (
        is_google_docs_url(url)
        or is_cloudinary_raw_url(url)
        or get_url_extension(url) in SKILLSHEET_EXTENSIONS
    )


def is_probable_file_response(final_url: str, content_type: str) -> bool:
    """最終URLまたはContent-Typeから実ファイル応答らしさを判定する。"""
    normalized_type = (content_type or "").lower()
    if is_probable_file_url(final_url):
        return True
    file_markers = (
        "application/pdf",
        "application/msword",
        "application/vnd.ms-excel",
        "application/vnd.openxmlformats-officedocument",
        "application/octet-stream",
        "binary/octet-stream",
    )
    return any(marker in normalized_type for marker in file_markers)


def is_html_response(content: bytes, content_type: str) -> bool:
    """HTMLページ応答かを緩めに判定する。"""
    normalized_type = (content_type or "").lower()
    if "text/html" in normalized_type or "application/xhtml+xml" in normalized_type:
        return True
    head = content[:1000].lower()
    html_markers = (b"<!doctype html", b"<html", b"<head", b"<body")
    return any(marker in head for marker in html_markers)


def extract_html_redirect_url(content: bytes, base_url: str) -> Optional[str]:
    """HTML本文中の JS / meta refresh リダイレクト URL を抽出する。"""
    try:
        html = content[:20000].decode("utf-8", errors="ignore")
    except Exception:
        return None

    patterns = (
        r'http-equiv=["\']refresh["\'][^>]*content=["\'][^"\']*url=([^"\'>]+)',
        r'location(?:\.href|\.replace)?\s*=\s*["\']([^"\']+)["\']',
        r'window\.open\(\s*["\']([^"\']+)["\']',
    )
    for pattern in patterns:
        match = re.search(pattern, html, flags=re.IGNORECASE)
        if match:
            return urljoin(base_url, match.group(1).strip())
    return None


def validate_skillsheet_text(text: str) -> Optional[str]:
    """success=true にする前の最低限の品質チェック。問題がなければ None を返す。"""
    stripped = text.strip()
    if not stripped:
        return "抽出テキストが空"

    lowered = stripped.lower()
    if any(marker in lowered for marker in ("<!doctype html", "<html", "<head", "<body")):
        return "HTML本文のため不採用"

    if any(marker in stripped for marker in LIST_SHEET_MARKERS):
        return "一覧シート本文のため不採用"

    if text.count("(cid:") >= 20:
        return "cid文字化け混入のため不採用"

    compact = re.sub(r"\s+", "", stripped)
    if len(compact) < MIN_TEXT_LENGTH:
        return f"テキストが短すぎるため不採用({len(compact)}文字)"

    lines = [line.strip() for line in stripped.splitlines() if line.strip()]
    if lines:
        page_number_lines = [
            line for line in lines
            if re.fullmatch(r"\d+\s*/\s*\d*", line)
        ]
        page_number_ratio = len(page_number_lines) / len(lines)
        if page_number_lines and (
            len(page_number_lines) == len(lines)
            or (len(lines) >= 10 and page_number_ratio >= 0.9)
        ):
            return "ページ番号だけの本文のため不採用"

    if lines and len(lines) <= 3:
        normalized = "".join(lines)
        normalized = re.sub(r"[ 　\t\r\n:：\-_/()（）【】\[\]]+", "", normalized)
        if normalized in TITLE_ONLY_TEXTS:
            return "表題だけの本文のため不採用"

    return None


IDENTITY_INITIAL_TOKEN_RE = re.compile(
    r"(?<![A-Za-z])([A-Za-z](?:[._・_]?\s*[A-Za-z]){1,2})(?![A-Za-z])"
)
IDENTITY_NAME_LABEL_RE = re.compile(
    r"(?:【\s*|■\s*)?(?:氏\s*名|名\s*前|技術者名)(?:\s*】|\s*■)?",
    re.IGNORECASE,
)
IDENTITY_AGE_LABEL_RE = re.compile(
    r"(?:【\s*|■\s*)?年\s*齢(?:\s*】|\s*■)?",
    re.IGNORECASE,
)
IDENTITY_STATION_LABEL_RE = re.compile(
    r"(?:【\s*|■\s*)?最\s*寄(?:り?\s*駅)?(?:\s*】|\s*■)?",
    re.IGNORECASE,
)
MAX_UNAMBIGUOUS_IDENTITY_VALUES = 5
MIN_CLEAR_AGE_DIFFERENCE = 5


def _normalize_identity_text(text: str) -> str:
    return unicodedata.normalize(
        "NFKC", (text or "").replace("\r\n", "\n").replace("\r", "\n")
    )


def _identity_label_windows(text: str, label_re: re.Pattern) -> List[str]:
    """label直後の同一行、または次の非空行だけをidentity値候補として返す。"""
    normalized = _normalize_identity_text(text)
    windows: List[str] = []
    for match in label_re.finditer(normalized):
        for raw_line in normalized[match.end():].splitlines():
            line = raw_line.strip(" \t:：|】■")
            if line:
                windows.append(line[:160])
                break
    return windows


def _normalize_initial(value: str) -> str:
    return re.sub(r"[^A-Z]", "", (value or "").upper())


def _extract_identity_initials(text: str, skillsheet: bool = False) -> Set[str]:
    values: Set[str] = set()
    for window in _identity_label_windows(text, IDENTITY_NAME_LABEL_RE):
        split_initial = re.match(
            r"^([A-Za-z])\s*\|\s*([A-Za-z])(?:\s*\||$)", window
        )
        if split_initial:
            values.add(
                _normalize_initial(split_initial.group(1) + split_initial.group(2))
            )
            continue
        token = IDENTITY_INITIAL_TOKEN_RE.search(window)
        if token:
            normalized = _normalize_initial(token.group(1))
            if 2 <= len(normalized) <= 3 and (not skillsheet or len(normalized) == 2):
                values.add(normalized)

    if skillsheet:
        normalized_text = _normalize_identity_text(text)
        sheet_title_re = re.compile(r"===\s*シート:\s*([^=\n]+)", re.IGNORECASE)
        summary_re = re.compile(
            r"(?m)^\s*([A-Za-z](?:[._・_]?\s*[A-Za-z]){1,2})"
            r"\s*[|｜]\s*(?:男性|女性|男|女)"
        )
        for raw_value in sheet_title_re.findall(normalized_text) + summary_re.findall(normalized_text):
            token = IDENTITY_INITIAL_TOKEN_RE.search(raw_value)
            if token:
                normalized = _normalize_initial(token.group(1))
                if len(normalized) == 2:
                    values.add(normalized)
    return values


def _extract_identity_ages(text: str, skillsheet: bool = False) -> Set[int]:
    values: Set[int] = set()
    for window in _identity_label_windows(text, IDENTITY_AGE_LABEL_RE):
        match = re.search(r"(?<!\d)([2-7]\d)\s*歳?", window)
        if match:
            values.add(int(match.group(1)))

    # 「氏名: AB(35歳)」形式を扱う。
    for window in _identity_label_windows(text, IDENTITY_NAME_LABEL_RE):
        values.update(
            int(value)
            for value in re.findall(r"(?<!\d)([2-7]\d)\s*歳", window)
        )

    if skillsheet:
        # labelの無い職務サマリーは先頭部だけを補助的に見る。
        normalized_text = _normalize_identity_text(text)
        values.update(
            int(value)
            for value in re.findall(
                r"(?<!\d)([2-7]\d)\s*歳", normalized_text[:500]
            )
        )
    return values


def _extract_identity_stations(text: str) -> Set[str]:
    values: Set[str] = set()
    for window in _identity_label_windows(text, IDENTITY_STATION_LABEL_RE):
        matches = re.findall(
            r"([一-龯ぁ-んァ-ヶA-Za-z0-9・ー]+)\s*駅", window
        )
        if matches:
            values.add(matches[-1])
    return values


def _initials_match(left: str, right: str) -> bool:
    if left == right:
        return True
    return len(left) == 2 and left == right[::-1]


def _stations_match(left: str, right: str) -> bool:
    return left == right or left in right or right in left


def _initial_present_in_text(initial: str, text: str) -> bool:
    chars = list(initial)
    pattern = r"(?<![A-Za-z])" + r"[._・_]?\s*".join(
        re.escape(char) for char in chars
    ) + r"(?![A-Za-z])"
    if re.search(pattern, _normalize_identity_text(text), re.IGNORECASE):
        return True
    if len(initial) == 2:
        reverse_pattern = r"(?<![A-Za-z])" + r"[._・_]?\s*".join(
            re.escape(char) for char in reversed(chars)
        ) + r"(?![A-Za-z])"
        return bool(
            re.search(
                reverse_pattern,
                _normalize_identity_text(text),
                re.IGNORECASE,
            )
        )
    return False


def detect_clear_identity_mismatch(
    email_body_text: str, skillsheet_text: str
) -> Dict[str, Any]:
    """年齢差と別属性の不一致が揃う明確な別人物だけを返す。"""
    body_initials = _extract_identity_initials(email_body_text)
    sheet_initials = _extract_identity_initials(skillsheet_text, skillsheet=True)
    body_ages = _extract_identity_ages(email_body_text)
    sheet_ages = _extract_identity_ages(skillsheet_text, skillsheet=True)
    body_stations = _extract_identity_stations(email_body_text)
    sheet_stations = _extract_identity_stations(skillsheet_text)

    sheet_value_sets = (sheet_initials, sheet_ages, sheet_stations)
    if any(len(values) > MAX_UNAMBIGUOUS_IDENTITY_VALUES for values in sheet_value_sets):
        return {}
    if len(body_ages) != 1 or not sheet_ages:
        return {}

    body_age = next(iter(body_ages))
    age_mismatch = all(
        abs(body_age - sheet_age) >= MIN_CLEAR_AGE_DIFFERENCE
        for sheet_age in sheet_ages
    )
    if not age_mismatch:
        return {}

    body_initial = next(iter(body_initials)) if len(body_initials) == 1 else ""
    initial_mismatch = bool(
        body_initial
        and sheet_initials
        and not _initial_present_in_text(body_initial, skillsheet_text)
        and not any(
            _initials_match(body_initial, sheet_initial)
            for sheet_initial in sheet_initials
        )
    )
    normalized_skillsheet = _normalize_identity_text(skillsheet_text)
    body_station = next(iter(body_stations)) if len(body_stations) == 1 else ""
    station_mismatch = (
        bool(body_station)
        and bool(sheet_stations)
        and body_station not in normalized_skillsheet
        and not any(
            _stations_match(body_station, sheet_station)
            for sheet_station in sheet_stations
        )
    )
    if not (initial_mismatch or station_mismatch):
        return {}

    mismatched_attributes = ["age"]
    if initial_mismatch:
        mismatched_attributes.append("initial")
    if station_mismatch:
        mismatched_attributes.append("station")
    return {
        "mismatched_attributes": mismatched_attributes,
        "body_initials": sorted(body_initials),
        "skillsheet_initials": sorted(sheet_initials),
        "body_ages": sorted(body_ages),
        "skillsheet_ages": sorted(sheet_ages),
        "body_stations": sorted(body_stations),
        "skillsheet_stations": sorted(sheet_stations),
    }


def classify_attachment_failure(filename: str, error: Exception) -> str:
    """添付ファイル失敗理由を対象区分つきで整形する。"""
    message = str(error).strip() or "unknown attachment error"
    if isinstance(error, ExtractTimeoutError) or "抽出タイムアウト" in message:
        scope = "要確認"
        kind = "extract_timeout"
    elif (
        message.startswith("ZIP")
        or "暗号化ZIP" in message
        or "could not read strings" in message.lower()
        or "サポートされていないファイル形式" in message
        or "旧Word(.doc)抽出未対応" in message
        or "抽出テキストが空" in message
    ):
        scope = "対象外"
        kind = "extract_unsupported_file"
    elif (
        "一覧シート本文のため不採用" in message
        or "cid文字化け混入のため不採用" in message
    ):
        scope = "対象外"
        kind = "extract_quality_ng"
    else:
        scope = "要確認"
        kind = "attachment_fetch_failed"
    return f"{scope}[{kind}] attachment:{filename}: {message}"


def classify_url_failure(category: str, url: str, error: Exception) -> str:
    """URL失敗理由を対象区分つきで整形する。"""
    message = str(error).strip() or "unknown url error"
    host = urlparse(url).netloc.lower()
    lower = message.lower()

    if isinstance(error, ExtractTimeoutError) or "抽出タイムアウト" in message:
        scope = "要確認"
        kind = "extract_timeout"
    elif host_matches(host, "d.bmb.jp") or host_matches(host, "a.bme.jp"):
        scope = "改善対象"
        kind = "redirect_url_resolution"
    elif category == "one_drive" or host_matches(host, "1drv.ms"):
        scope = "改善対象"
        kind = "onedrive_short_url_resolution"
    elif category == "ctsu_public_talent":
        scope = "改善対象"
        kind = "ctsu_fileid_resolution"
    elif any(code in lower for code in ("httpエラー 401", "httpエラー 403", "httpエラー 404", "httpエラー 410")):
        scope = "対象外"
        kind = "expired_or_permission_denied"
    elif "htmlページ応答のため不採用" in lower:
        scope = "対象外"
        kind = "no_actual_file_url"
    elif (
        "抽出テキストが空" in message
        or "サポートされていないファイル形式" in message
        or "テキストが短すぎるため不採用" in message
        or "表題だけの本文のため不採用" in message
    ):
        scope = "対象外"
        kind = "extract_unsupported_file"
    elif (
        "一覧シート本文のため不採用" in message
        or "cid文字化け混入のため不採用" in message
    ):
        scope = "対象外"
        kind = "extract_quality_ng"
    else:
        scope = "要確認"
        kind = "url_fetch_failed"
    return f"{scope}[{kind}] url[{category}] {url}: {message}"


def resolve_redirect_url(
    session: requests.Session,
    url: str,
    timeout: int = TIMEOUT_OTHER,
    max_hops: int = 8,
) -> str:
    """Location / meta refresh / JS redirect を手動追跡して最終URL候補を返す。"""
    current_url = url
    for _ in range(max_hops):
        response = session.get(
            current_url,
            headers=HTTP_HEADERS,
            timeout=timeout,
            allow_redirects=False,
            stream=True,
        )

        if 300 <= response.status_code < 400:
            location = (response.headers.get("Location") or "").strip()
            if not location:
                raise ValueError(f"HTTPリダイレクト先が空です: {current_url}")
            current_url = urljoin(current_url, location)
            continue

        if response.status_code == 200:
            if is_html_response(response.content, response.headers.get("Content-Type", "")):
                redirect_url = extract_html_redirect_url(
                    response.content,
                    response.url or current_url,
                )
                if redirect_url and redirect_url != current_url:
                    current_url = redirect_url
                    continue
            return response.url or current_url

        raise ValueError(f"HTTPエラー {response.status_code}: {current_url}")

    raise ValueError(f"リダイレクト追跡上限超過: {url}")


# ---------------------------------------------------------------------------
# Google Drive ダウンロード
# ---------------------------------------------------------------------------


def extract_gdrive_file_id(url: str) -> Optional[str]:
    """Google Drive URL からファイル ID を抽出する。"""
    patterns = [
        r"open\?id=([^&\s]+)",
        r"uc\?.*id=([^&\s]+)",
        r"drive\.google\.com/file/d/([^/\?\s]+)",
        r"docs\.google\.com/spreadsheets/d/([^/\?\s]+)",
        r"docs\.google\.com/document/d/([^/\?\s]+)",
        r"/d/([a-zA-Z0-9_-]{20,})",
    ]
    for p in patterns:
        m = re.search(p, url)
        if m:
            return m.group(1)
    return None


def _detect_google_doc_type(url: str) -> str:
    """
    Google Docs URL の種別を判定する。
    戻り値: "spreadsheet" | "document" | "file"
    """
    lower = url.lower()
    if "docs.google.com/spreadsheets/" in lower:
        return "spreadsheet"
    if "docs.google.com/document/" in lower:
        return "document"
    return "file"


def download_google_drive(url: str, timeout: int = TIMEOUT_GDRIVE) -> DownloadResult:
    """
    Google Drive URL からファイルをダウンロードして bytes を返す。
    Spreadsheet は xlsx、Document は docx で export する。
    失敗時は例外を送出する。
    """
    # open?id= 形式を正規化
    parsed = urlparse(url)
    if parsed.path in ("/open", "/uc"):
        params = parse_qs(parsed.query)
        file_id = params.get("id", [None])[0]
    else:
        file_id = extract_gdrive_file_id(url)

    if not file_id:
        raise ValueError(f"Google Drive ファイルIDを取得できません: {url}")

    doc_type = _detect_google_doc_type(url)
    session = requests.Session()

    # Spreadsheet / Document は export API を使う
    if doc_type == "spreadsheet":
        download_url = f"https://docs.google.com/spreadsheets/d/{file_id}/export?format=xlsx"
    elif doc_type == "document":
        download_url = f"https://docs.google.com/document/d/{file_id}/export?format=docx"
    else:
        download_url = f"https://drive.google.com/uc?export=download&id={file_id}"

    resp = session.get(download_url, stream=True, timeout=timeout, headers=HTTP_HEADERS, allow_redirects=True)

    # uc?export=download の場合のみウイルス警告クッキー処理
    if doc_type == "file":
        for k, v in resp.cookies.items():
            if k.startswith("download_warning"):
                resp = session.get(
                    download_url,
                    params={"id": file_id, "confirm": v},
                    stream=True,
                    timeout=timeout,
                    headers=HTTP_HEADERS,
                )
                break

    if resp.status_code != 200:
        raise ValueError(f"HTTPエラー {resp.status_code}: {download_url}")

    content = resp.content
    if not content:
        raise ValueError("ダウンロードされたコンテンツが空です")

    # HTML エラーページ判定
    if len(content) < 10000 and b"<html" in content[:200]:
        raise ValueError("Google Drive がエラーHTMLを返しました (認証が必要か公開設定されていない可能性)")

    return DownloadResult(
        content=content,
        final_url=resp.url or download_url,
        content_type=resp.headers.get("Content-Type", ""),
        filename="",
    )


# ---------------------------------------------------------------------------
# OneDrive ダウンロード
# ---------------------------------------------------------------------------


def build_sharepoint_excel_download_url(url: str) -> str:
    """SharePointのExcel共有リンクだけに download=1 を設定する。"""
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if (
        parsed.scheme.lower() != "https"
        or not host_matches(host, "sharepoint.com")
        or "/:x:/" not in parsed.path.lower()
    ):
        return url

    query_items = [
        (key, value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
        if key.lower() != "download"
    ]
    query_items.append(("download", "1"))
    return urlunparse(parsed._replace(query=urlencode(query_items, doseq=True)))


def download_onedrive(url: str, timeout: int = TIMEOUT_OTHER) -> DownloadResult:
    """
    OneDrive URL からファイルをダウンロードして bytes を返す。
    1drv.ms 短縮 URL のリダイレクトも追跡する。
    """
    session = requests.Session()

    actual_url = url
    if "1drv.ms" in url:
        try:
            actual_url = resolve_redirect_url(session, url, timeout=timeout)
        except Exception:
            actual_url = url

    share_candidates: List[str] = []
    for candidate in (url, actual_url):
        if not candidate:
            continue
        token = _base64url_encode(candidate)
        share_candidates.append(
            f"https://api.onedrive.com/v1.0/shares/u!{token}/root/content"
        )

    for candidate_url in share_candidates:
        try:
            resp = session.get(
                candidate_url,
                stream=True,
                timeout=timeout,
                headers=HTTP_HEADERS,
                allow_redirects=True,
            )
        except Exception:
            continue
        if resp.status_code != 200 or not resp.content:
            continue
        if is_html_response(resp.content, resp.headers.get("Content-Type", "")):
            continue
        return DownloadResult(
            content=resp.content,
            final_url=resp.url or candidate_url,
            content_type=resp.headers.get("Content-Type", ""),
            filename="",
        )

    # SharePoint Excel共有リンクは既存queryを保ってdownload指定を追加する。
    actual_url = build_sharepoint_excel_download_url(actual_url)

    # OneDrive の共有リンクはダウンロード URL に変換
    # https://onedrive.live.com/... → download パラメータ追加
    if "onedrive.live.com" in actual_url and "download" not in actual_url.lower():
        if "?" in actual_url:
            actual_url += "&download=1"
        else:
            actual_url += "?download=1"

    resp = session.get(actual_url, stream=True, timeout=timeout, headers=HTTP_HEADERS, allow_redirects=True)
    if resp.status_code != 200:
        raise ValueError(f"HTTPエラー {resp.status_code}: {actual_url}")

    content = resp.content
    if not content:
        raise ValueError("ダウンロードされたコンテンツが空です")

    return DownloadResult(
        content=content,
        final_url=resp.url or actual_url,
        content_type=resp.headers.get("Content-Type", ""),
        filename="",
    )


# ---------------------------------------------------------------------------
# その他 URL ダウンロード
# ---------------------------------------------------------------------------


def download_other_url(url: str, timeout: int = TIMEOUT_OTHER) -> DownloadResult:
    """任意の URL からファイルをダウンロードして bytes を返す。
    リダイレクト先が Google Docs/Drive の場合は専用処理に委譲する。
    """
    session = requests.Session()

    try:
        final_url = resolve_redirect_url(session, url, timeout=timeout)
    except Exception:
        final_url = url

    # リダイレクト先が Google Docs/Drive なら専用処理に委譲
    final_lower = final_url.lower()
    if "drive.google.com" in final_lower or "docs.google.com" in final_lower:
        return download_google_drive(final_url, timeout=TIMEOUT_GDRIVE)
    final_host = urlparse(final_lower).netloc.lower()
    if any(host_matches(final_host, d) for d in ONEDRIVE_DOMAINS):
        return download_onedrive(final_url, timeout=TIMEOUT_OTHER)

    # 通常のダウンロード
    resp = session.get(
        final_url,
        stream=True,
        timeout=timeout,
        headers=HTTP_HEADERS,
        allow_redirects=True,
    )
    if resp.status_code != 200:
        raise ValueError(f"HTTPエラー {resp.status_code}: {final_url}")

    content = resp.content
    if not content:
        raise ValueError("ダウンロードされたコンテンツが空です")

    return DownloadResult(
        content=content,
        final_url=resp.url or final_url,
        content_type=resp.headers.get("Content-Type", ""),
        filename="",
    )


def _extract_ctsu_file_ids(html: str) -> List[str]:
    """CTSU公開ページHTMLから fileId 候補を抽出する。"""
    patterns = (
        r'fileId\s*[:=]\s*["\']?([A-Za-z0-9_-]{8,})',
        r'"fileId"\s*:\s*"([A-Za-z0-9_-]{8,})"',
        r'"fileId"\s*,\s*"([A-Za-z0-9_-]{8,})"',
        r'fileId["\']?\s*,\s*["\']([A-Za-z0-9_-]{8,})["\']',
        r'\\"fileId\\"\s*,\s*\\"([A-Za-z0-9_-]{8,})\\"',
        r'/api/files/([A-Za-z0-9_-]{8,})',
    )
    found: List[str] = []
    seen: Set[str] = set()
    for pattern in patterns:
        for file_id in re.findall(pattern, html, flags=re.IGNORECASE):
            if file_id not in seen:
                seen.add(file_id)
                found.append(file_id)
    return found


def _extract_text_from_html_document(raw_html: str) -> str:
    """HTML文書から可読テキストだけを抜き出す。"""
    cleaned = re.sub(r"(?is)<script[^>]*>.*?</script>", " ", raw_html)
    cleaned = re.sub(r"(?is)<style[^>]*>.*?</style>", " ", cleaned)
    cleaned = re.sub(r"(?is)<noscript[^>]*>.*?</noscript>", " ", cleaned)
    cleaned = re.sub(r"(?i)<br\\s*/?>", "\n", cleaned)
    cleaned = re.sub(r"(?i)</(?:p|div|section|article|li|tr|h[1-6])>", "\n", cleaned)
    cleaned = re.sub(r"(?s)<[^>]+>", " ", cleaned)
    cleaned = html.unescape(cleaned)
    cleaned = cleaned.replace("\r", "\n")
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    cleaned = re.sub(r"[ \t\f\v]+", " ", cleaned)
    return "\n".join(line.strip() for line in cleaned.splitlines() if line.strip())


def _normalize_ctsu_file_payload(payload: Any) -> Dict[str, Any]:
    """CTSU /api/files 応答から signedUrl/fileName を持つ辞書を取り出す。"""
    if isinstance(payload, dict):
        if payload.get("signedUrl"):
            return payload
        for key in ("data", "file", "result"):
            child = payload.get(key)
            normalized = _normalize_ctsu_file_payload(child)
            if normalized:
                return normalized
    elif isinstance(payload, list):
        for item in payload:
            normalized = _normalize_ctsu_file_payload(item)
            if normalized:
                return normalized
    return {}


def _score_ctsu_candidate(file_name: str, signed_url: str) -> Tuple[int, int]:
    """CTSU候補の優先度スコアを返す。大きいほど優先。"""
    base_name = (file_name or urlparse(signed_url).path.rsplit("/", 1)[-1]).lower()
    ext = base_name.rsplit(".", 1)[-1] if "." in base_name else ""
    ext_priority = {
        "pdf": 5,
        "docx": 4,
        "doc": 3,
        "xlsx": 2,
        "xls": 1,
    }.get(ext, 0)
    keyword_score = 0
    for kw in ("スキルシート", "経歴書", "職務経歴書"):
        if kw in (file_name or ""):
            keyword_score += 1
    return keyword_score, ext_priority


def _select_ctsu_candidate(candidates: List[Dict[str, str]]) -> Dict[str, str]:
    """CTSU候補一覧から優先順位に従って1件選ぶ。"""
    if not candidates:
        return {}
    ranked = sorted(
        enumerate(candidates),
        key=lambda item: (_score_ctsu_candidate(item[1].get("file_name", ""), item[1].get("signed_url", "")), -item[0]),
        reverse=True,
    )
    best_score = _score_ctsu_candidate(ranked[0][1].get("file_name", ""), ranked[0][1].get("signed_url", ""))
    if best_score == (0, 0):
        return candidates[0]
    return ranked[0][1]


def download_ctsu_public_talent(url: str, timeout: int = TIMEOUT_OTHER) -> DownloadResult:
    """
    CTSU公開人材ページから fileId -> /api/files/{fileId} -> signedUrl の順で実ファイルを取得する。
    """
    session = requests.Session()

    try:
        page_resp = session.get(
            url,
            timeout=timeout,
            headers=HTTP_HEADERS,
            allow_redirects=True,
        )
    except Exception as e:
        raise ValueError(f"CTSU公開ページ取得失敗: {e}") from e

    if page_resp.status_code != 200:
        raise ValueError(f"CTSU公開ページ取得失敗: HTTP {page_resp.status_code}")

    html = page_resp.text or ""
    file_ids = _extract_ctsu_file_ids(html)
    if not file_ids:
        page_text = _extract_text_from_html_document(html)
        if validate_skillsheet_text(page_text) is None:
            return DownloadResult(
                content=page_text.encode("utf-8"),
                final_url=page_resp.url or url,
                content_type="text/plain; charset=utf-8",
                filename="ctsu_public_talent.txt",
            )
        raise ValueError("CTSU fileId 抽出失敗")

    candidates: List[Dict[str, str]] = []
    api_errors: List[str] = []
    for file_id in file_ids:
        api_url = f"https://ctsu.jp/api/files/{file_id}"
        try:
            api_resp = session.get(
                api_url,
                timeout=timeout,
                headers=HTTP_HEADERS,
                allow_redirects=True,
            )
        except Exception as e:
            api_errors.append(f"{file_id}: {e}")
            continue

        if api_resp.status_code != 200:
            api_errors.append(f"{file_id}: HTTP {api_resp.status_code}")
            continue

        try:
            payload = api_resp.json()
        except json.JSONDecodeError as e:
            api_errors.append(f"{file_id}: JSON decode error {e}")
            continue

        file_info = _normalize_ctsu_file_payload(payload)
        signed_url = file_info.get("signedUrl")
        file_name = file_info.get("fileName", "")
        if not signed_url:
            api_errors.append(f"{file_id}: signedUrl missing")
            continue

        candidates.append(
            {
                "file_id": file_id,
                "signed_url": signed_url,
                "file_name": file_name,
            }
        )

    if not candidates:
        detail = ", ".join(api_errors[:3]) if api_errors else "candidate missing"
        raise ValueError(f"CTSU api/files 応答不正: {detail}")

    selected = _select_ctsu_candidate(candidates)
    signed_url = selected["signed_url"]

    try:
        file_resp = session.get(
            signed_url,
            timeout=timeout,
            headers=HTTP_HEADERS,
            allow_redirects=True,
        )
    except Exception as e:
        raise ValueError(f"CTSU signedUrl ダウンロード失敗: {e}") from e

    if file_resp.status_code != 200:
        raise ValueError(f"CTSU signedUrl ダウンロード失敗: HTTP {file_resp.status_code}")

    content = file_resp.content
    if not content:
        raise ValueError("CTSU signedUrl ダウンロード失敗: empty content")

    return DownloadResult(
        content=content,
        final_url=file_resp.url or signed_url,
        content_type=file_resp.headers.get("Content-Type", ""),
        filename=selected.get("file_name", ""),
    )


# ---------------------------------------------------------------------------
# メイン取得処理
# ---------------------------------------------------------------------------


def fetch_skillsheet(
    message_id: str,
    mail_record: Optional[Dict[str, Any]],
    cleanup_record: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    スキルシートを取得してレコードを返す。

    Returns:
        {message_id, skillsheet, source, success, urls}
    """
    identity_body_text = (
        (mail_record or {}).get("body_text")
        or (cleanup_record or {}).get("body_text")
        or ""
    )

    # --- ① 添付ファイル優先 ---
    attachments = (mail_record or {}).get("attachments") or []
    failure_reasons: List[str] = []
    if attachments:
        errors: List[str] = []
        classified_errors: List[str] = []
        for att in attachments:
            filename = att.get("filename", "")
            if not is_eligible_attachment(att):
                # 画像や会社案内等、skillsheet候補外の添付はURL利用を妨げない。
                continue
            try:
                logger.info(f"添付ファイル抽出開始: {filename}", message_id)
                text = extract_from_attachment(att)
                quality_error = validate_skillsheet_text(text)
                if quality_error is None:
                    identity_mismatch = detect_clear_identity_mismatch(
                        identity_body_text, text
                    )
                    if identity_mismatch:
                        attributes = ",".join(
                            identity_mismatch["mismatched_attributes"]
                        )
                        errors.append(
                            f"{filename}: 本文人物とskillsheet人物が不一致({attributes})"
                        )
                        classified_errors.append(
                            "要確認[identity_mismatch] "
                            f"attachment:{filename}: mismatched_attributes={attributes}"
                        )
                        logger.warn(
                            "添付skillsheet人物不一致のため次候補へ進む: "
                            f"{filename} ({attributes})",
                            message_id,
                        )
                        continue
                    logger.ok(f"添付ファイルからテキスト取得: {filename}", message_id)
                    return {
                        "message_id": message_id,
                        "skillsheet": text,
                        "source": "attachment",
                        "success": True,
                        "urls": False,
                    }
                errors.append(f"{filename}: {quality_error}")
                classified_errors.append(
                    classify_attachment_failure(filename, ValueError(quality_error))
                )
            except Exception as e:
                errors.append(f"{filename}: {e}")
                classified_errors.append(classify_attachment_failure(filename, e))
                logger.warn(f"添付ファイル抽出失敗 {filename}: {e}", message_id)

        # 添付はあったが全部失敗 or 対象外だった場合は URL に進む
        if errors:
            failure_reasons.extend(classified_errors)
            logger.warn(f"添付ファイル取得失敗（URL抽出に進む）: {errors}", message_id)

    # --- ② URL からの取得 ---
    sorted_urls, html_urls, body_urls = build_url_candidates(mail_record, cleanup_record)

    tried_urls: List[str] = []
    for url, category in sorted_urls:
        tried_urls.append(url)
        source = classify_url(url)
        try:
            logger.info(f"URL取得開始 ({source}/{category}): {url}", message_id)
            if source == "google_drive":
                download_result = download_google_drive(url)
            elif source == "one_drive":
                download_result = download_onedrive(url)
            elif source == "ctsu_public_talent":
                download_result = download_ctsu_public_talent(url)
            else:
                download_result = download_other_url(url)

            if category in ("conditional", "direct_adopt_ng"):
                if is_html_response(download_result.content, download_result.content_type):
                    raise ValueError("HTMLページ応答のため不採用")
                if not is_probable_file_response(download_result.final_url, download_result.content_type):
                    raise ValueError(
                        f"最終URLが実ファイルと判定できません: {download_result.final_url}"
                    )

            if is_html_response(download_result.content, download_result.content_type):
                raise ValueError("HTMLページ応答のため不採用")

            # URL のファイル名から拡張子推定
            if download_result.filename:
                url_filename = download_result.filename
            else:
                filename_url = download_result.final_url if is_probable_file_url(download_result.final_url) else url
                url_filename = urlparse(filename_url).path.rsplit("/", 1)[-1]
            logger.info(f"URLファイル抽出開始 ({source}/{category}): {url_filename}", message_id)
            text = extract_text_from_bytes_with_timeout(download_result.content, url_filename)

            quality_error = validate_skillsheet_text(text)
            if quality_error is None:
                identity_mismatch = detect_clear_identity_mismatch(
                    identity_body_text, text
                )
                if identity_mismatch:
                    attributes = ",".join(
                        identity_mismatch["mismatched_attributes"]
                    )
                    failure_reasons.append(
                        "要確認[identity_mismatch] "
                        f"url[{category}] {url}: mismatched_attributes={attributes}"
                    )
                    logger.warn(
                        "URL skillsheet人物不一致のため次候補へ進む "
                        f"({source}/{category}): {url} ({attributes})",
                        message_id,
                    )
                    continue
                logger.ok(
                    f"URLからテキスト取得 ({source}/{category}): {url} -> {download_result.final_url}",
                    message_id,
                )
                return {
                    "message_id": message_id,
                    "skillsheet": text,
                    "source": source,
                    "success": True,
                    "urls": url,
                }
            if source == "ctsu_public_talent" and quality_error == "抽出テキストが空":
                raise ValueError("CTSU 抽出テキストが空")
            raise ValueError(quality_error)
        except Exception as e:
            reason = classify_url_failure(category, url, e)
            failure_reasons.append(reason)
            logger.warn(f"URL取得失敗 ({source}/{category}) {url}: {e}", message_id)
            continue

    # --- ③ 取得失敗 ---
    urls_value = tried_urls[0] if tried_urls else False
    logger.warn(f"スキルシート取得失敗 tried_urls={tried_urls}", message_id)
    result = {
        "message_id": message_id,
        "skillsheet": None,
        "source": None,
        "success": False,
        "urls": urls_value,
    }
    if tried_urls:
        result["tried_urls"] = tried_urls
    if failure_reasons:
        result["failure_reason"] = " | ".join(failure_reasons[:10])
    else:
        if html_urls or body_urls:
            result["failure_reason"] = "要確認[unknown_failure] URL取得失敗理由が記録されていません"
        else:
            result["failure_reason"] = "対象外[no_actual_file_url] 本文に実URLなし"
    return result


# ---------------------------------------------------------------------------
# エントリポイント
# ---------------------------------------------------------------------------


def load_mail_index(target_ids: Set[str]) -> Dict[str, Dict[str, Any]]:
    """
    mail_master.jsonl から対象 message_id のレコードのみ読み込む。
    添付データ(base64)を含むため全件読むとメモリが肥大する。
    """
    index: Dict[str, Dict[str, Any]] = {}
    logger.info(f"mail_master.jsonl 読み込み (対象IDのみ): {INPUT_MAIL_MASTER}")
    for rec in read_jsonl_as_list(str(INPUT_MAIL_MASTER)):
        mid = rec.get("message_id")
        if mid and mid in target_ids:
            index[mid] = rec
    logger.info(f"mail_master 読み込み完了: {len(index)} 件 (対象 {len(target_ids)} 件中)")
    return index


def main() -> None:
    start_time = time.time()

    # 出力ディレクトリ作成
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    EXEC_TIME_DIR.mkdir(parents=True, exist_ok=True)

    logger.info("=== 04-1_fetch_skillsheets_text 開始 ===")

    # 入力ファイル存在チェック
    for p in (INPUT_RESOURCES, INPUT_MAIL_MASTER, INPUT_CLEANUP_EMAILS):
        if not p.exists():
            err = FileNotFoundError(f"入力ファイルが見つかりません: {p}")
            logger.error(str(err))
            write_error_log(str(RESULT_DIR), err, context="入力ファイル確認")
            sys.exit(1)

    # --- 処理対象 ID 読み込み ---
    logger.info(f"resources.jsonl 読み込み: {INPUT_RESOURCES}")
    resources = read_jsonl_as_list(str(INPUT_RESOURCES))
    target_ids_list = [r["message_id"] for r in resources]
    target_ids_set = set(target_ids_list)
    total = len(target_ids_list)
    logger.info(f"処理対象: {total} 件")

    # --- mail_master: 対象IDのみロード（メモリ節約）---
    mail_index = load_mail_index(target_ids_set)

    # --- cleanup_emails: 対象IDのみロード ---
    logger.info(f"cleanup_emails.jsonl 読み込み: {INPUT_CLEANUP_EMAILS}")
    cleanup_index: Dict[str, Dict[str, Any]] = {}
    for rec in read_jsonl_as_list(str(INPUT_CLEANUP_EMAILS)):
        mid = rec.get("message_id")
        if mid and mid in target_ids_set:
            cleanup_index[mid] = rec
    logger.info(f"cleanup_emails 読み込み完了: {len(cleanup_index)} 件")

    # --- 出力ファイル初期化（都度 append するため事前クリア）---
    OUTPUT_JSONL.write_text("", encoding="utf-8")
    NO_FETCH_JSONL.write_text("", encoding="utf-8")

    # --- 処理（1件ずつ書き込み）---
    success_count = 0
    fail_count = 0

    for i, message_id in enumerate(target_ids_list, 1):
        mail_rec = mail_index.get(message_id)
        cleanup_rec = cleanup_index.get(message_id)

        record = fetch_skillsheet(message_id, mail_rec, cleanup_rec)

        # 即時書き込み（途中終了しても結果が残る）
        append_jsonl(str(OUTPUT_JSONL), record)
        if record["success"]:
            success_count += 1
        else:
            fail_count += 1
            append_jsonl(str(NO_FETCH_JSONL), record)

        # 10件ごとに進捗ログ
        if i % 10 == 0 or i == total:
            elapsed = time.time() - start_time
            rate = success_count / i * 100
            avg = elapsed / i
            logger.info(
                f"進捗: {i}/{total} | 成功:{success_count} 失敗:{fail_count} "
                f"取得率:{rate:.1f}% | 経過:{elapsed:.0f}秒 平均:{avg:.1f}秒/件"
            )

    # --- 最終サマリ ---
    elapsed = time.time() - start_time
    rate = success_count / total * 100 if total else 0.0

    logger.info("=== 完了 ===")
    logger.info(f"処理件数: {total} 件")
    logger.info(f"成功: {success_count} 件 / 失敗: {fail_count} 件 (取得率 {rate:.1f}%)")
    logger.info(f"出力: {OUTPUT_JSONL}")
    logger.info(f"未取得: {NO_FETCH_JSONL}")

    write_execution_time(
        str(EXEC_TIME_DIR),
        "fetch_skillsheets_text",
        elapsed,
        record_count=total,
    )

    if rate < 90.0:
        logger.warn(f"取得率が90%未満です: {rate:.1f}% (成功 {success_count}/{total})")

    sys.exit(0)


if __name__ == "__main__":
    main()
