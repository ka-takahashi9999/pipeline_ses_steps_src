"""
LLM呼び出し専用モジュール（OpenAI API使用）
・JSONスキーマを必ず適用すること
・LLMはキー変更禁止（値のみ更新）
・LLM使用は02-1補助・03-50・07-1・08-5・10_assistance_toolのみ許可

APIキーはAWS SSM Parameter Store (/openai/api_key) から取得する。
直接コードにAPIキーをハードコード禁止。
"""

import json
import os
import time
import threading
import fcntl
from pathlib import Path
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

# counter fileを利用できない場合だけ使うプロセス内fallback。
# 通常経路はfile lock付きcounterにより、同一run/step内のcall_numberを採番する。
_telemetry_counter_lock = threading.Lock()
_telemetry_fallback_counters: Dict[str, int] = {}


class LLMOutputTruncatedError(ValueError):
    """LLM出力がトークン上限で途中終了したことを表す。"""


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


def _metadata_text(value: Any) -> str:
    """telemetry metadataを本文を含まない安全な文字列へ正規化する。"""
    if value is None:
        return ""
    if isinstance(value, (str, int, float)) and not isinstance(value, bool):
        return str(value)
    return ""


def _safe_filename_part(value: str) -> str:
    normalized = "".join(
        char if char.isascii() and (char.isalnum() or char in ("-", "_")) else "_"
        for char in value
    ).strip("_")
    return normalized or "manual"


def _build_telemetry_state(
    telemetry_context: Optional[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """caller指定のstep/output先とrun環境変数だけから保存状態を作る。"""
    if not isinstance(telemetry_context, dict):
        return None

    step = _metadata_text(telemetry_context.get("step")).strip()
    output_dir = telemetry_context.get("output_dir")
    if not step or not isinstance(output_dir, (str, os.PathLike)):
        return None

    run_id = _metadata_text(
        telemetry_context.get("run_id", os.environ.get("RUN_ID", ""))
    ).strip()
    run_date = _metadata_text(
        telemetry_context.get("run_date", os.environ.get("RUN_DATE", ""))
    ).strip()
    filename_run_id = _safe_filename_part(run_id)
    usage_path = Path(output_dir) / f"llm_usage_{filename_run_id}.jsonl"
    return {
        "run_id": run_id,
        "run_date": run_date,
        "step": step,
        "usage_path": usage_path,
    }


def _next_call_number(telemetry_state: Dict[str, Any]) -> int:
    """同一usage fileのlogical call番号をfile lock付きで採番する。"""
    usage_path = Path(telemetry_state["usage_path"])
    counter_path = usage_path.with_suffix(".counter")
    try:
        counter_path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(counter_path, os.O_RDWR | os.O_CREAT, 0o600)
        with os.fdopen(fd, "r+", encoding="utf-8") as counter_file:
            fcntl.flock(counter_file.fileno(), fcntl.LOCK_EX)
            try:
                current_text = counter_file.read().strip()
                current = int(current_text) if current_text else 0
                call_number = current + 1
                counter_file.seek(0)
                counter_file.truncate()
                counter_file.write(str(call_number))
                counter_file.flush()
            finally:
                fcntl.flock(counter_file.fileno(), fcntl.LOCK_UN)
        return call_number
    except Exception as error:
        _logger.warn(
            "LLM usage telemetry call_number採番失敗; "
            f"プロセス内採番へfallbackします: {type(error).__name__}: {error}"
        )

    fallback_key = str(usage_path)
    with _telemetry_counter_lock:
        call_number = _telemetry_fallback_counters.get(fallback_key, 0) + 1
        _telemetry_fallback_counters[fallback_key] = call_number
        return call_number


def _nonnegative_token(container: Dict[str, Any], key: str) -> tuple:
    if key not in container:
        return 0, False
    value = container.get(key)
    if isinstance(value, bool):
        return 0, False
    try:
        number = int(value)
    except (TypeError, ValueError, OverflowError):
        return 0, False
    if number < 0:
        return 0, False
    return number, True


def _first_token_value(container: Dict[str, Any], keys: tuple) -> tuple:
    for key in keys:
        value, available = _nonnegative_token(container, key)
        if available:
            return value, True
    return 0, False


def _extract_usage(response_json: Any) -> Dict[str, Any]:
    """Chat Completions top-level usageを既知shapeから防御的に抽出する。"""
    empty = {
        "input_tokens": 0,
        "cached_input_tokens": 0,
        "output_tokens": 0,
        "total_tokens": 0,
        "usage_available": False,
    }
    if not isinstance(response_json, dict):
        return empty
    usage = response_json.get("usage")
    if not isinstance(usage, dict):
        return empty

    input_tokens, input_available = _first_token_value(
        usage, ("prompt_tokens", "input_tokens")
    )
    output_tokens, output_available = _first_token_value(
        usage, ("completion_tokens", "output_tokens")
    )
    total_tokens, total_available = _first_token_value(usage, ("total_tokens",))

    cached_input_tokens, _ = _first_token_value(
        usage, ("cached_input_tokens", "cached_prompt_tokens")
    )
    if cached_input_tokens == 0:
        for details_key in ("prompt_tokens_details", "input_tokens_details"):
            details = usage.get(details_key)
            if not isinstance(details, dict):
                continue
            cached_input_tokens, cached_available = _first_token_value(
                details, ("cached_tokens", "cached_input_tokens")
            )
            if cached_available:
                break

    return {
        "input_tokens": input_tokens,
        "cached_input_tokens": cached_input_tokens,
        "output_tokens": output_tokens,
        "total_tokens": total_tokens,
        "usage_available": (
            input_available or output_available or total_available
        ),
    }


def _append_telemetry_record(path: Path, record: Dict[str, Any]) -> None:
    """1 recordをprocess間file lock付きでJSONL appendする。"""
    line = json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    with os.fdopen(fd, "a", encoding="utf-8") as usage_file:
        fcntl.flock(usage_file.fileno(), fcntl.LOCK_EX)
        try:
            usage_file.write(line)
            usage_file.flush()
        finally:
            fcntl.flock(usage_file.fileno(), fcntl.LOCK_UN)


def _record_telemetry_attempt(
    telemetry_state: Optional[Dict[str, Any]],
    model: str,
    call_number: int,
    attempt_number: int,
    response_json: Any,
    success: bool,
    error_type: str,
) -> None:
    """usage保存をbest-effortで行い、例外をLLM処理へ伝播させない。"""
    if telemetry_state is None:
        return
    try:
        usage = _extract_usage(response_json)
        record = {
            "run_id": telemetry_state["run_id"],
            "run_date": telemetry_state["run_date"],
            "step": telemetry_state["step"],
            "model": str(model),
            "call_number": call_number,
            "attempt_number": attempt_number,
            "input_tokens": usage["input_tokens"],
            "cached_input_tokens": usage["cached_input_tokens"],
            "output_tokens": usage["output_tokens"],
            "total_tokens": usage["total_tokens"],
            "usage_available": usage["usage_available"],
            "success": bool(success),
            "error_type": "" if success else str(error_type or "unexpected_error"),
        }
        _append_telemetry_record(Path(telemetry_state["usage_path"]), record)
    except Exception as error:
        _logger.warn(
            "LLM usage telemetry書き込み失敗; LLM処理は継続します: "
            f"{type(error).__name__}: {error}"
        )


def call_llm(
    system_prompt: str,
    user_prompt: str,
    response_schema: Dict[str, Any],
    model: str = "gpt-4o-mini",
    temperature: float = 0.0,
    max_tokens: int = 1024,
    max_retries: int = 3,
    retry_wait_seconds: float = 5.0,
    telemetry_context: Optional[Dict[str, Any]] = None,
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
        telemetry_context: usage保存用のstep/output_dir/run metadata（省略可）

    Returns:
        パースされたJSONレスポンス（response_schemaのキー構造を保持）

    Raises:
        RuntimeError: APIキー取得失敗・全リトライ失敗時
        LLMOutputTruncatedError: finish_reason=length で出力が途中終了した時
        ValueError: レスポンスJSONのパース失敗時
    """
    api_key = _get_api_key()
    telemetry_state = _build_telemetry_state(telemetry_context)
    call_number = _next_call_number(telemetry_state) if telemetry_state else 0

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
        response_json: Any = None
        response: Optional[requests.Response] = None
        attempt_success = False
        attempt_error_type = "unexpected_error"
        try:
            _logger.llm(f"API呼び出し試行 {attempt}/{max_retries} (model={model})")
            response = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers=headers,
                json=payload,
                timeout=60,
            )
            response.raise_for_status()
            response_json = response.json()
            choice = response_json["choices"][0]
            content = choice["message"]["content"]
            finish_reason = choice.get("finish_reason")
            if finish_reason == "length":
                content_chars = len(content or "")
                error_message = (
                    "LLM output truncated: "
                    f"finish_reason=length model={model} "
                    f"max_tokens={max_tokens} content_chars={content_chars}"
                )
                _logger.warn(error_message)
                raise LLMOutputTruncatedError(error_message)
            parsed = json.loads(content)
            _validate_schema_keys(parsed, response_schema)
            attempt_success = True
            attempt_error_type = ""
            _logger.llm(f"API呼び出し成功 (attempt={attempt})")
            return parsed
        except LLMOutputTruncatedError as e:
            # 同じmax_tokensで再試行しても解消しないため即時に呼び出し元へ返す。
            attempt_error_type = type(e).__name__
            raise
        except requests.exceptions.HTTPError as e:
            attempt_error_type = type(e).__name__
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
            attempt_error_type = type(e).__name__
            _logger.warn(f"JSONパースエラー (attempt={attempt}): {e}")
            last_error = e
            last_error_detail = str(e)
        except requests.exceptions.RequestException as e:
            attempt_error_type = type(e).__name__
            _logger.warn(f"リクエストエラー (attempt={attempt}): {e}")
            last_error = e
            last_error_detail = str(e)
        finally:
            if response_json is None and response is not None:
                try:
                    response_json = response.json()
                except Exception:
                    response_json = None
            _record_telemetry_attempt(
                telemetry_state=telemetry_state,
                model=model,
                call_number=call_number,
                attempt_number=attempt,
                response_json=response_json,
                success=attempt_success,
                error_type=attempt_error_type,
            )

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
