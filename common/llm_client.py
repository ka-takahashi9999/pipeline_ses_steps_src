"""
LLM呼び出し専用モジュール（OpenAI API使用）
・JSONスキーマを必ず適用すること
・LLMはキー変更禁止（値のみ更新）
・LLM使用は02-1補助・03-50・07-1・08-5・10_assistance_toolのみ許可

APIキーはAWS SSM Parameter Store (/openai/api_key) から取得する。
直接コードにAPIキーをハードコード禁止。
"""

import json
import time
import threading
from typing import Any, Dict, List, Optional

import boto3
import requests

from common.logger import get_logger

_logger = get_logger("llm_client")

# SSM からAPIキーをキャッシュ（プロセス内1回のみ取得）
_api_key_cache: Optional[str] = None
_api_key_lock = threading.Lock()

# レート制限用ロック
_rate_limit_lock = threading.Lock()
_last_call_time: float = 0.0
_MIN_INTERVAL_SECONDS: float = 0.5  # 最小呼び出し間隔
_HTTP_ERROR_BODY_MAX_CHARS: int = 1000


def _get_api_key() -> str:
    """AWS SSM Parameter StoreからOpenAI APIキーを取得する（プロセス内キャッシュあり）。"""
    global _api_key_cache
    with _api_key_lock:
        if _api_key_cache is not None:
            return _api_key_cache
        try:
            ssm = boto3.client("ssm", region_name="ap-northeast-1")
            response = ssm.get_parameter(Name="/openai/api_key", WithDecryption=True)
            _api_key_cache = response["Parameter"]["Value"]
            _logger.info("OpenAI APIキーをSSMから取得しました")
            return _api_key_cache
        except Exception as e:
            raise RuntimeError(f"OpenAI APIキーのSSM取得に失敗しました: {e}") from e


def _enforce_rate_limit() -> None:
    """最小呼び出し間隔を強制する。"""
    global _last_call_time
    with _rate_limit_lock:
        now = time.monotonic()
        elapsed = now - _last_call_time
        if elapsed < _MIN_INTERVAL_SECONDS:
            time.sleep(_MIN_INTERVAL_SECONDS - elapsed)
        _last_call_time = time.monotonic()


def _truncate_http_error_text(
    value: Any,
    max_chars: int = _HTTP_ERROR_BODY_MAX_CHARS,
) -> str:
    text = "" if value is None else str(value)
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + "...(truncated)"


def _format_http_error_detail(response: Optional[requests.Response]) -> str:
    """HTTPエラー応答の本文から、ログ出力してよい範囲の詳細だけを整形する。"""
    if response is None:
        return "status_code=None response_body=<no response>"

    status_code = response.status_code
    response_text = _truncate_http_error_text(response.text)
    try:
        body = response.json()
    except ValueError:
        return f"status_code={status_code} response_body={response_text}"

    error = body.get("error") if isinstance(body, dict) else None
    if not isinstance(error, dict):
        return f"status_code={status_code} response_body={response_text}"

    parts = [f"status_code={status_code}"]
    for key in ("message", "type", "code", "param"):
        value = error.get(key)
        if value is not None:
            parts.append(f"{key}={_truncate_http_error_text(value)}")
    parts.append(f"response_body={response_text}")
    return " ".join(parts)


def call_llm(
    system_prompt: str,
    user_prompt: str,
    response_schema: Dict[str, Any],
    model: str = "gpt-4o-mini",
    temperature: float = 0.0,
    max_tokens: int = 1024,
    max_retries: int = 3,
    retry_wait_seconds: float = 5.0,
) -> Dict[str, Any]:
    """
    OpenAI APIを呼び出してJSONレスポンスを取得する。

    Args:
        system_prompt: システムプロンプト
        user_prompt: ユーザープロンプト
        response_schema: 期待するJSONスキーマ（キー名と型を定義）
                         LLMはこのスキーマのキーを変更してはならない（値のみ更新）
        model: 使用するOpenAIモデル
        temperature: 温度パラメータ（0.0=決定的）
        max_tokens: 最大トークン数
        max_retries: リトライ回数
        retry_wait_seconds: リトライ間隔（秒）

    Returns:
        パースされたJSONレスポンス（response_schemaのキー構造を保持）

    Raises:
        RuntimeError: APIキー取得失敗・全リトライ失敗時
        ValueError: レスポンスJSONのパース失敗時
    """
    api_key = _get_api_key()

    # スキーマ情報をプロンプトに組み込む
    schema_str = json.dumps(response_schema, ensure_ascii=False, indent=2)
    full_system_prompt = (
        f"{system_prompt}\n\n"
        f"必ず以下のJSONスキーマに従ってJSONのみを返すこと。キー名は変更禁止。値のみ更新可。\n"
        f"```json\n{schema_str}\n```"
    )

    messages = [
        {"role": "system", "content": full_system_prompt},
        {"role": "user", "content": user_prompt},
    ]

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
        "response_format": {"type": "json_object"},
    }

    last_error: Optional[Exception] = None
    last_error_detail: Optional[str] = None
    for attempt in range(1, max_retries + 1):
        _enforce_rate_limit()
        try:
            _logger.llm(f"API呼び出し試行 {attempt}/{max_retries} (model={model})")
            resp = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers=headers,
                json=payload,
                timeout=60,
            )
            resp.raise_for_status()
            content = resp.json()["choices"][0]["message"]["content"]
            parsed = json.loads(content)
            _validate_schema_keys(parsed, response_schema)
            _logger.llm(f"API呼び出し成功 (attempt={attempt})")
            return parsed
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else None
            http_error_detail = _format_http_error_detail(e.response)
            _logger.warn(
                f"HTTPエラー: {status} (attempt={attempt}): {http_error_detail}"
            )
            last_error = e
            last_error_detail = f"{e}; {http_error_detail}"
            if status in (400, 401, 403):
                # リトライしても意味がないエラー
                break
        except (json.JSONDecodeError, ValueError) as e:
            _logger.warn(f"JSONパースエラー (attempt={attempt}): {e}")
            last_error = e
            last_error_detail = str(e)
        except requests.exceptions.RequestException as e:
            _logger.warn(f"リクエストエラー (attempt={attempt}): {e}")
            last_error = e
            last_error_detail = str(e)

        if attempt < max_retries:
            _logger.info(f"{retry_wait_seconds}秒後にリトライします...")
            time.sleep(retry_wait_seconds)

    if isinstance(last_error, (json.JSONDecodeError, ValueError)):
        raise ValueError(
            f"OpenAI APIレスポンスJSON不正: {last_error_detail}"
        ) from last_error

    raise RuntimeError(
        f"OpenAI API呼び出しが{max_retries}回失敗しました: {last_error_detail}"
    ) from last_error


def _validate_schema_keys(response: Dict[str, Any], schema: Dict[str, Any]) -> None:
    """
    レスポンスのキーがスキーマのキーと一致することを確認する。
    LLMはキー変更禁止のため、スキーマにないキーや欠損キーを検出する。
    警告ログのみ出力し、例外は発生させない（値は信頼する）。
    """
    schema_keys = set(schema.keys())
    response_keys = set(response.keys())

    missing = schema_keys - response_keys
    extra = response_keys - schema_keys

    if missing:
        _logger.warn(f"LLMレスポンスにスキーマキーが不足しています: {missing}")
    if extra:
        _logger.warn(f"LLMレスポンスにスキーマ外のキーがあります: {extra}")


def build_schema_with_defaults(template: Dict[str, Any]) -> Dict[str, Any]:
    """
    JSONテンプレートからLLM呼び出し用スキーマを構築する。
    nullは禁止のため、デフォルト値が設定されたテンプレートを渡すこと。
    """
    return dict(template)


def call_llm_with_fallback(
    system_prompt: str,
    user_prompt: str,
    response_schema: Dict[str, Any],
    fallback_value: Dict[str, Any],
    step_name: str = "",
    message_id: str = "",
    **kwargs: Any,
) -> Dict[str, Any]:
    """
    LLM呼び出しを試み、失敗時はfallback_valueを返す。
    fallback_valueはresponse_schemaと同じキー構造を持つこと。
    """
    try:
        return call_llm(system_prompt, user_prompt, response_schema, **kwargs)
    except Exception as e:
        _logger.error(
            f"LLM呼び出し失敗、フォールバック値を使用します: {e}",
            message_id=message_id or None,
        )
        return dict(fallback_value)


def get_available_models() -> List[str]:
    """利用可能なモデル一覧を返す（設定参考用）。"""
    return [
        "gpt-4o",
        "gpt-4o-mini",
        "gpt-4-turbo",
        "gpt-3.5-turbo",
    ]
