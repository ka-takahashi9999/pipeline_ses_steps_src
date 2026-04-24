#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
要員ロケーション抽出パーサーモジュール

抽出フロー:
  1. 最寄り/最寄駅/居住地/住所ラベル行 + 次行 からキャンディデート取得（最優先）
  2. キャンディデートを正規化（路線名・駅suffix・括弧・※注釈除去）
  3. 辞書マッチ（CJK前置チェック付き：複合駅名の部分一致を防ぐ）
  4. ラベルで取れない場合、在住/居住/最寄 等の近傍行のみで辞書マッチ（body fallback）
  ※ リモートフォールバックなし（要員は居住地を対象とする）
  ※ 会社署名や案件勤務地ラベルはbody fallbackで除外する

出力:
  (location, location_raw, location_source)
  location_source: "label" | "body" | ""
"""

import re
import unicodedata
from typing import List, Optional, Set, Tuple

# ── ラベル検出 ────────────────────────────────────────────
# 03-10の「場所」「勤務地」「作業場所」は含めない（案件勤務地を誤認防止）
# \s* で「最　寄」「最 寄 駅」等の全角スペース・半角スペース入りラベルに対応
# 「住まい」「お住まい」: 居住都道府県を書く要員メールに対応
_LABEL_RE = re.compile(r"最\s*寄\s*り?\s*駅?|居住地?|住\s*所|住まい|お住まい")

# ── 路線名除去 ─────────────────────────────────────────────
_RAILWAY_RE = re.compile(
    r"(?:JR|メトロ|東京メトロ|都営|東急|西武|東武|小田急|京急|京王|相鉄|"
    r"りんかい|ゆりかもめ|つくばエクスプレス|横浜市営|神戸市営|"
    r"阪急|阪神|近鉄|南海|京阪|市営地下鉄|地下鉄|名鉄|"
    r"JR[^\s　、,，/（(【]+線?|"
    r"[^\s　、,，/（(【]+(?:線|鉄道))"
    r"(?=\S)",
    re.IGNORECASE,
)

# ── 駅 suffix 除去 ────────────────────────────────────────
_EKI_RE = re.compile(r"駅$")

# ── 括弧内コンテンツ（末尾）除去 ─────────────────────────
_TRAILING_PAREN_RE = re.compile(r"\s*[（(][^）)]{0,30}[）)]\s*$")

# ── ※注釈行の先頭除去 ─────────────────────────────────────
_NOTE_PREFIX_RE = re.compile(r"^[※＊\*]\s*\d*\s*最寄り?駅?は\s*")

# ── 区切り文字 ─────────────────────────────────────────────
_SPLIT_RE = re.compile(r"[、,，/・]|\bor\b|または")

# ── セクションヘッダー検出 ───────────────────────────────
_HEADER_RE = re.compile(r"^[【◆■□※▼▶★]|^\[")

# ── 〒 を含む行（署名住所） ──────────────────────────────
_POSTAL_RE = re.compile(r"〒")

# ── CJK文字パターン（複合駅名部分一致チェック用） ───────────
# 「新福島」の「福島」、「東長崎」の「長崎」などを防ぐ
_CJK_RE = re.compile(r"[\u4e00-\u9fff\u3400-\u4dbf\u30a0-\u30ff\u3040-\u309f]")

# ── 海外在住判定（地方抽出に失敗した場合のみ使用） ─────────
# 「海外在住」「イギリス在住」「中国在住」等の明示的な海外居住表現のみマッチ
# 「海外案件」「海外勤務」等の案件勤務地表現はマッチしない
_OVERSEAS_RE = re.compile(
    r"(?:"
    r"海外[に]?\s*在住|"
    r"(?:アメリカ|米国|カナダ|イギリス|英国|フランス|ドイツ|イタリア|スペイン|"
    r"オーストラリア|ニュージーランド|中国|韓国|台湾|香港|マカオ|"
    r"シンガポール|タイ|インド|ベトナム|フィリピン|マレーシア|"
    r"インドネシア|ミャンマー|バングラデシュ|スリランカ|"
    r"ブラジル|メキシコ|アルゼンチン|"
    r"ヨーロッパ|欧州)[に]?\s*(?:在住|居住中?)"
    r")"
)

# ── 本文辞書検索でスキップする行パターン（強化版） ─────────
# ・会社フッター由来（本社:、オフィス:、MAIL:、本社営業部 等）
# ・案件勤務地ラベル（場所:、勤務地: 等）を追加（03-10と違い要員parserでは不要）
_BODY_SIG_LINE_RE = re.compile(
    r"住所\s*[:：]|所在地\s*[:：]|"
    r"本\s*社\s*[:：]|"                                   # 本社：/ 本　　社：(全角スペース含む)
    r"(?:東京|大阪|名古屋|福岡|札幌|仙台|横浜|神戸|京都)\s*本社|"  # 東京本社 etc.
    r"本社営業|"
    r"オフィス\s*[:：]|"                                  # 秋田オフィス：etc.
    r"営業電話|"
    r"本社\s*[/｜|・]|[/｜|・]\s*本社|"
    r"支社\s*[/｜|・]|[/｜|・]\s*支社|"
    r"支社営業部|支店営業部|"
    r"SES\s*事業部|営業部\s*[:：]|"
    r"MAIL\s*[:：]|共通メール|受信用|"
    r"配信解除|プライバシーマーク|"
    r"派遣事業|"
    r"場所\s*[:：]|勤務地\s*[:：]|作業場所\s*[:：]|就業場所\s*[:：]|"  # 案件勤務地ラベル
    r"勤務場所\s*[:：]|就業地\s*[:：]",
    re.IGNORECASE,
)

# ── body fallbackのトリガーキーワード ────────────────────
# これらのキーワードを含む行の周辺のみを検索対象にする
_LOCATION_CONTEXT_RE = re.compile(
    r"在住|居住|都道府県|最寄り?駅?|生活地|活動地|出身地?|在籍地|拠点"
)

# ── 地名でない説明 token の判定 ───────────────────────────
_JUNK_TOKEN_RE = re.compile(
    r"^フェーズ|"
    r"^確認中|"
    r"^週\s*\d|"
    r"^徒歩\s*\d|"
    r"^バス\s*\d|"
    r"^約\s*\d|"
    r"^\d+時間|"           # 1時間以内希望 等
    r"^圏内|"
    r"^フルリモ|"
    r"^リモート|"
    r"^常駐|"
    r"^相談"
)

# ── 氏名/名前 行の括弧内駅名抽出 ────────────────────────
# 「【氏名】NT（上板橋駅）」「氏名：EM（50歳/男性）」等から駅名を取り出す
_NAME_LABEL_RE = re.compile(r"(?:氏\s*名|名\s*前)")

# 年齢・性別のみの括弧内容を判定（これらは地名ではない）
# 例: 「46歳・女性」「男性」「28歳/男性」→ 除外
_AGE_GENDER_ONLY_RE = re.compile(
    r"^(?:\d+[歳才]|[男女]性?|その他)"
    r"(?:[/・,，\s　]*(?:\d+[歳才]|[男女]性?|その他|日本|中国))*$"
)

# ── プロフィール「XX＠地名」形式 ─────────────────────────
# 「MH＠志村三丁目」「TT＠川西能勢口」「YH@筥崎宮前」等
# 条件: 行全体が大文字イニシャル(1-4字) + ＠ + CJK含む地名（ドット・ドメイン構造なし）
_PROFILE_AT_RE = re.compile(
    r"^[A-Z]{1,4}[A-Za-z.]*\s*[＠@]([^\s　.@,，/（(]{2,15})$"
)
# CJK文字(漢字・ひらがな・カタカナ)を含む → メールアドレスでなく地名と判定
_CJK_CONTENT_RE = re.compile(
    r"[\u4e00-\u9fff\u3400-\u4dbf\u3040-\u309f\u30a0-\u30ff]"
)

# ── 【基本情報】スラッシュ区切り行から駅名を検出 ──────────
_BASIC_INFO_RE = re.compile(r"[【\[]?\s*基本情報\s*[】\]]")
_AGE_FIELD_RE  = re.compile(r"^\d+歳$")
_SEX_FIELD_RE  = re.compile(r"^(男性?|女性?|その他)$")
_NATION_FIELD_RE = re.compile(r"[籍]$|国籍$")
_NAME_FIELD_RE = re.compile(
    r"^[A-Za-z]\.|"           # T.B, K.H
    r"^[\u4e00-\u9fff]\.|"    # 張.ZJ
    r"^[A-Z]{1,3}$"           # NT 等の頭文字
)


def _n(s: str) -> str:
    """NFKC正規化"""
    return unicodedata.normalize("NFKC", s or "")


def _safe_kw_match(pattern: "re.Pattern", text: str) -> Optional["re.Match"]:
    """
    辞書キーワードのマッチを返す。
    マッチ位置の直前がCJK文字の場合（複合駅名・複合地名の一部）は除外する。

    例:
      「福島」 in 「新福島」 → 直前が「新」(CJK) → None
      「長崎」 in 「東長崎」 → 直前が「東」(CJK) → None
      「福島」 in 「福島市」 → 直前なし → マッチ返す
      「大阪」 in 「大阪市内」 → 直前なし → マッチ返す
    """
    m = pattern.search(text)
    if m is None:
        return None
    if m.start() > 0 and _CJK_RE.match(text[m.start() - 1]):
        return None
    return m


def _get_label_inline_content(line: str) -> Optional[str]:
    """ラベル行から同行のコンテンツを取得する。"""
    # ※注釈形式: ※２最寄り駅は盛岡駅
    note_m = _NOTE_PREFIX_RE.match(line.strip())
    if note_m:
        content = line.strip()[note_m.end():].strip()
        return content if content else None

    # 「：」または「:」セパレータ
    sep_m = re.search(r"[：:]", line)
    if sep_m:
        content = line[sep_m.end():].strip()
        return content if content else None

    # 【...】の後ろにコンテンツ
    bracket_m = re.search(r"】\s*(.+)", line)
    if bracket_m:
        content = bracket_m.group(1).strip()
        return content if content else None

    return None  # ラベルのみの行 → 次行参照が必要


def _extract_basic_info_station(line: str) -> Optional[str]:
    """
    【基本情報】行のスラッシュ区切りフィールドから駅名フィールドを抽出する。
    例: 「【基本情報】T.B / 男性 / 31歳 / 西国立 / 中国籍」→ 「西国立」
    """
    if not _BASIC_INFO_RE.search(line):
        return None
    # 】以降のコンテンツを取得
    m = re.search(r"[】\]]\s*(.+)", _n(line))
    if not m:
        return None
    content = m.group(1).strip()
    fields = [f.strip() for f in re.split(r"\s*/\s*", content)]

    # 1. 駅 suffix を持つフィールドを優先
    for f in fields:
        if re.search(r"駅[)）]?$", f) and len(f) >= 3:
            return f

    # 2. 名前/性別/年齢/国籍 以外のフィールドを駅名候補とする
    for f in fields:
        if not f or len(f) < 2:
            continue
        if _AGE_FIELD_RE.match(f):
            continue
        if _SEX_FIELD_RE.match(f):
            continue
        if _NATION_FIELD_RE.search(f):
            continue
        if _NAME_FIELD_RE.match(f):
            continue
        return f

    return None


def _extract_name_line_station(line: str) -> List[str]:
    """
    氏名/名前 行の括弧内から駅名候補を抽出する。

    対応パターン:
      「【氏　名】NT（上板橋駅）」       → 上板橋
      「氏名：KH（男性・56歳）」          → スキップ（年齢/性別のみ）
      「■氏名：C.K（女性・53歳＠JR線・中山駅）」 → 中山
      「【名前】HH（31歳/女性＠北小金駅）」 → 北小金
      「【氏名】TK（32歳・女性・下総橘駅）」  → 下総橘
    """
    if not _NAME_LABEL_RE.search(_n(line)):
        return []

    results: List[str] = []
    for m in re.finditer(r"[（(]([^）)]{2,40})[）)]", _n(line)):
        content = m.group(1).strip()

        # ＠/@ 以降に駅名表記があるパターン
        # 「女性・53歳＠JR線・中山駅」→ ＠以降を逆に探して `駅` 付き語を取得
        at_m = re.search(r"[＠@][^＠@]*?([^\s　・,，/]{2,10})駅?$", content)
        if at_m:
            s = at_m.group(1).strip()
            # 路線名（線/鉄道 で終わる）は除外
            if not re.search(r"(?:線|鉄道|モノレール)$", s) and len(s) >= 2:
                results.append(s)
            continue

        # 末尾に 駅 suffix があるパターン（※注釈は無視）
        # 「上板橋駅」「茨木市駅 ※大阪府」「下総橘駅」
        station_m = re.search(r"([^\s　・,，/]{2,10})駅(?:\s*[※＊][^）)]*)?$", content)
        if station_m:
            s = station_m.group(1).strip()
            if len(s) >= 2 and not re.search(r"(?:線|鉄道|モノレール)$", s):
                results.append(s)
            continue

        # 年齢・性別のみは除外（「46歳・女性」「男性」「28歳/男性」）
        if _AGE_GENDER_ONLY_RE.match(content):
            continue

    return results


def _extract_label_candidates(lines: List[str]) -> List[str]:
    """ラベル行と次行、基本情報行、氏名括弧内からロケーション候補文字列リストを返す。"""
    candidates: List[str] = []

    for i, raw_line in enumerate(lines):
        line = raw_line.strip()

        if _POSTAL_RE.search(line):
            continue

        # 【基本情報】スラッシュ区切り形式
        basic = _extract_basic_info_station(line)
        if basic:
            candidates.append(basic)
            continue

        # プロフィール「XX＠地名」形式（「MH＠志村三丁目」「YH@筥崎宮前」等）
        # メールアドレスと区別: ＠以降にCJK文字を含む場合のみ地名として採用
        at_m = _PROFILE_AT_RE.match(_n(line))
        if at_m:
            candidate = at_m.group(1).strip()
            if _CJK_CONTENT_RE.search(candidate):
                candidates.append(candidate)
            continue

        # 氏名/名前 行の括弧内駅名（「【氏名】NT（上板橋駅）」等）
        name_stations = _extract_name_line_station(line)
        if name_stations:
            candidates.extend(name_stations)
            continue

        if not _LABEL_RE.search(_n(line)):  # NFKC正規化後にラベル検出（全角スペース対応）
            continue

        inline = _get_label_inline_content(line)
        if inline:
            candidates.append(inline)
            continue

        # ラベルのみ行 → 次の非空行を取得
        for j in range(i + 1, min(i + 5, len(lines))):
            next_line = lines[j].strip()
            if not next_line:
                continue
            if _HEADER_RE.match(next_line):
                break
            if _POSTAL_RE.search(next_line):
                break
            candidates.append(next_line)
            break

    return candidates


def _normalize_candidate(candidate: str) -> List[str]:
    """
    候補文字列を正規化してトークンリストに分割する。

    処理順:
      NFKC正規化 → HTML残骸除去 → ※注釈先頭除去 → 路線名除去
      → 区切り分割 → 各token:
          ※以降の説明除去・先頭記号除去・括弧内容抽出・末尾括弧除去
          ・都道府県suffix除去・駅suffix除去
    """
    text = _n(candidate)

    # HTML残骸除去（&nbsp; 等）
    text = re.sub(r"&[a-zA-Z]+;", " ", text)

    # ※注釈の先頭除去（行全体が ※最寄り駅は... 形式の場合）
    note_m = _NOTE_PREFIX_RE.match(text)
    if note_m:
        text = text[note_m.end():].strip()

    # 路線名除去
    text = _RAILWAY_RE.sub("", text)
    text = re.sub(r"\s{2,}", " ", text).strip()

    parts = _SPLIT_RE.split(text)
    tokens: List[str] = []
    paren_extras: List[str] = []

    for p in parts:
        t = p.strip()
        # 先頭記号・コロン除去（「： 北国分」→「北国分」）
        t = re.sub(r"^[\s　・★■□▼※\-\.※【】◆☆：:]+", "", t).strip()
        # HTML残骸が残っていればここでも除去
        t = re.sub(r"&[a-zA-Z]+;.*$", "", t).strip()
        # ※以降の説明テキスト除去（「高尾駅※常駐可」→「高尾駅」）
        t = re.sub(r"\s*[※＊]\s*.+$", "", t).strip()
        # token単位の路線名プレフィックス除去（空白あり/なし両対応・各種suffix対象）
        # 例: 「池上線御嶽山」→「御嶽山」、「スカイツリーライン新田」→「新田」、「本線瀬谷」→「瀬谷」
        t = re.sub(r"^[^\s　、,，/（(【]{1,10}(?:線|鉄道|モノレール|ライン|電鉄)\s*", "", t)
        # RAILWAY_RE が京王/東武等の会社名のみ除去した残り「線柴崎」・「線 聖蹟桜ヶ丘」等を処理
        # 空白あり/なし両対応（「線柴崎」→「柴崎」、「線 千歳烏山」→「千歳烏山」）
        t = re.sub(r"^(?:線|鉄道|モノレール)\s*", "", t)
        # 末尾の補足説明除去（「常駐可」「相談可」「通勤X時間以内」）
        t = re.sub(r"\s*(?:常駐可能?|相談可能?|通勤\s*\d+\s*時間(?:以内)?)\s*$", "", t).strip()

        # 末尾括弧内コンテンツを別途抽出
        # 「T.S（片倉町駅）」→ 括弧内「片倉町駅」→「片倉町」を paren_extras へ
        # 「JR宇都宮線」のような路線名括弧は対象外（辞書未ヒットで自然に落ちる）
        paren_m = re.search(r"[（(]([^）)]{2,20})[）)]$", t)
        if paren_m:
            pc = paren_m.group(1).strip()
            pc = re.sub(r"[都道府県]$", "", pc).strip()
            pc = _EKI_RE.sub("", pc).strip()
            if len(pc) >= 2 and not _JUNK_TOKEN_RE.match(pc):
                paren_extras.append(pc)

        # 末尾括弧除去: 「辻堂（神奈川県）」→「辻堂」
        t = _TRAILING_PAREN_RE.sub("", t).strip()
        # 括弧のみトークンはアンラップ
        t = re.sub(r"^[(（]\s*(.*?)\s*[)）]$", r"\1", t)
        t = t.strip()
        # 都道府県suffix除去: 「広島県」→「広島」
        t = re.sub(r"[都道府県]$", "", t).strip()
        # 駅suffix除去
        t = _EKI_RE.sub("", t).strip()
        if not t or len(t) < 2:
            continue
        if _JUNK_TOKEN_RE.match(t):
            continue
        tokens.append(t)

    # 括弧抽出トークンを末尾に追加（重複除外）
    for pe in paren_extras:
        if pe not in tokens:
            tokens.append(pe)

    return tokens


def _match_from_tokens(
    tokens: List[str],
    entries: List[Tuple[str, str, "re.Pattern"]],
) -> Optional[Tuple[str, str]]:
    """
    トークンリストから最初にマッチした地方を返す。
    CJK前置チェックにより複合駅名の部分一致を防ぐ。
    """
    for token in tokens:
        text = _n(token)
        best_pos = None
        best_region = None
        best_kw = None
        for region, keyword, pattern in entries:
            m = _safe_kw_match(pattern, text)
            if m and (best_pos is None or m.start() < best_pos):
                best_pos = m.start()
                best_region = region
                best_kw = keyword
        if best_region:
            return (best_region, best_kw)

    return None


def _get_location_context_idxs(lines: List[str]) -> Set[int]:
    """
    在住/居住/最寄 等のキーワードを含む行の前後を含むインデックスセットを返す。
    署名行・〒行は除外する。
    """
    context_idxs: Set[int] = set()
    for i, line in enumerate(lines):
        if "〒" in line:
            continue
        if _BODY_SIG_LINE_RE.search(line):
            continue
        if _LOCATION_CONTEXT_RE.search(line):
            for j in range(max(0, i - 1), min(len(lines), i + 3)):
                context_idxs.add(j)
    return context_idxs


def _body_dict_search(
    body: str,
    entries: List[Tuple[str, str, "re.Pattern"]],
) -> Optional[Tuple[str, str, str]]:
    """
    本文の「在住/居住/最寄」近傍行のみを対象とした辞書マッチ（フォールバック）。
    ・全文検索ではなく location context 近傍だけを検索
    ・会社署名行・案件勤務地ラベル行は除外
    ・CJK前置チェックで複合駅名の部分一致を防ぐ

    Returns:
        (region, location_raw, "body") or None
    """
    text = _n(body)
    lines = text.splitlines()

    # location context 行のインデックス収集
    context_idxs = _get_location_context_idxs(lines)
    if not context_idxs:
        return None

    # context 行のみ連結（署名行・〒行は除外）
    filtered_lines = [
        lines[i] for i in sorted(context_idxs)
        if "〒" not in lines[i] and not _BODY_SIG_LINE_RE.search(lines[i])
    ]
    if not filtered_lines:
        return None

    text = "\n".join(filtered_lines)

    best_pos = None
    best_region = None
    best_kw = None

    for region, keyword, pattern in entries:
        m = _safe_kw_match(pattern, text)
        if m and (best_pos is None or m.start() < best_pos):
            best_pos = m.start()
            best_region = region
            best_kw = keyword

    if best_region is None:
        return None

    start = max(0, best_pos - 20)
    end = min(len(text), best_pos + len(best_kw) + 30)
    raw = text[start:end].strip()[:100]
    return (best_region, raw, "body")


def parse_location(
    body: str,
    entries: List[Tuple[str, str, "re.Pattern"]],
) -> Tuple[str, Optional[str], str]:
    """
    要員メール本文からロケーション（地方）を抽出する。

    Args:
        body: 署名除去済みのメール本文
        entries: ロケーション辞書エントリー [(region, keyword, pattern), ...]

    Returns:
        (location, location_raw, location_source)
        location は必ず非nullの文字列:
          - 9地方のいずれか / "overseas" / "unknown"
    """
    if not body:
        return "unknown", None, ""

    lines = body.splitlines()

    # ── フェーズ1: ラベル行 + 次行からの抽出（最優先） ──────
    label_candidates = _extract_label_candidates(lines)
    for candidate in label_candidates:
        tokens = _normalize_candidate(candidate)
        if not tokens:
            continue
        result = _match_from_tokens(tokens, entries)
        if result:
            region, kw = result
            raw = _n(candidate)[:100]
            return region, raw, "label"

    # ── フェーズ2: location context 近傍行への辞書マッチ ──
    result2 = _body_dict_search(body, entries)
    if result2:
        return result2

    # ── フェーズ3: 海外在住判定 ─────────────────────────────
    # 地方抽出に失敗した場合のみ、海外在住を明示する表現を探す
    # 署名除去済みボディに対して適用（案件勤務地ラベルの誤判定防止はフェーズ1/2で担保）
    m = _OVERSEAS_RE.search(_n(body))
    if m:
        raw = m.group(0)[:50]
        return "overseas", raw, "body"

    # ── フェーズ4: 地方が確定できない場合 → unknown ──────────
    return "unknown", None, ""
