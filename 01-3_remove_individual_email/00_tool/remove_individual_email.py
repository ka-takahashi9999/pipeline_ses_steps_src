#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 01-3: 個別除外処理スクリプト

複数要員メール・キャンペーンメール等を個別除外する。
手動除外条件は 10_assistance_tool/exclude_list.txt で管理する。

手動除外に該当しないメールだけ、SubjectからHIGH_CONFIDENCEに判定できる
明示的な複数要員・複数案件・一覧そのものをdeterministicに除外する。

除外リスト形式（1行につき）:
  from のみ    → そのアドレスからの全メールを除外
  from,subject → from + subject が一致するメールを除外
  # で始まる行・空行はスキップ

入力①（本文参照用）:
  01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl

入力②（処理対象のmessage_id）:
  01-2_remove_duplicate_emails/01_result/remove_duplicate_emails_raw.jsonl

出力①: 01_result/remove_individual_emails_raw.jsonl  （除外後の message_id）
出力②: 01_result/99_removed_individual_emails_raw.jsonl （除外された message_id）
出力③: 01_result/99_removed_individual_emails_detail.jsonl
       （限定テンプレートで追加除外したメールの元全項目 + reason / rule_id）
"""

import fnmatch
import re
import sys
import time
import unicodedata
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

# common モジュールのパス解決
_STEP_DIR = Path(__file__).resolve().parent.parent
_PROJECT_ROOT = _STEP_DIR.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from common.file_utils import ensure_result_dirs, write_error_log, write_execution_time
from common.json_utils import read_jsonl_as_dict, read_jsonl_as_list, write_jsonl
from common.logger import get_logger

STEP_NAME = "01-3_remove_individual_email"
logger = get_logger(STEP_NAME)

INPUT_MASTER = str(
    _PROJECT_ROOT / "01-1_fetch_gmail" / "01_result" / "fetch_gmail_mail_master.jsonl"
)
INPUT_PREV = str(
    _PROJECT_ROOT / "01-2_remove_duplicate_emails" / "01_result" / "remove_duplicate_emails_raw.jsonl"
)
EXCLUDE_LIST_PATH = str(_STEP_DIR / "10_assistance_tool" / "exclude_list.txt")

OUTPUT_FILTERED = "remove_individual_emails_raw.jsonl"
OUTPUT_REMOVED = "99_removed_individual_emails_raw.jsonl"
OUTPUT_DETAIL = "99_removed_individual_emails_detail.jsonl"

# 2026-09-17レビューの実例に限定。汎用の複数要員・非SES判定には広げない。
TEMPLATE_REASONS = {"multi_item_mail", "list_or_portal_notice", "service_notification"}
_PROFILE_OR_PROJECT_RE = re.compile(
    r"氏\s*名|名\s*前|年\s*齢|最\s*寄|単\s*価|単\s*金|"
    r"案件名|案件概要|業務内容|作業内容|必須スキル|【案件】"
)
_ADDITIONAL_MATCHING_DETAIL_RE = re.compile(
    r"イニシャル|所\s*属|希望\s*単\s*(?:価|金)|"
    r"稼\s*(?:働|動)\s*開始|入場日|参画可能|"
    r"勤務地|作業場所|就業場所|精算|募集人数|要望スキル|契約形態"
)
_INLINE_MATCHING_DETAIL_RES = (
    re.compile(r"(?:^|[\s/|｜])(?:[A-Z][.．]){1,2}[A-Z]?(?:$|[\s/|｜])"),
    re.compile(r"(?<![0-9])[0-9]{2}歳(?!まで|以下|迄)"),
    re.compile(r"[^\s/|｜]{1,20}駅(?:[（(][^)）]+[)）])?"),
    re.compile(r"(?<![0-9])[0-9]{2,3}万(?:円)?(?![0-9])"),
)
_LIST_PORTAL_LABEL_RE = re.compile(
    r"(?:要員|人材|案件)(?:一覧(?:リスト)?|リスト)"
)
_LIST_PORTAL_CONTEXT_RE = re.compile(
    r"(?:要員|人材|案件)(?:一覧(?:リスト)?|リスト)|"
    r"営業中(?:の)?[^\n]{0,40}(?:人材|要員)"
)
_EXTERNAL_URL_RE = re.compile(r"https?://[^\s<>]+", re.IGNORECASE)
_LIST_LINK_SEPARATOR_RE = re.compile(
    r"^[\s【】\[\]■◆◇●・:：_\-ー―‐－━—─]*$"
)
_CHO_LINK_URL = r"https://u29046571\.ct\.sendgrid\.net/ls/click\?\S+"
_CHO_INVITE_BODY_RE = re.compile(
    r"(?P<company>[^\n]+?)の(?P<person>[^\n]+?)様より、チョータツでの連携のご依頼です\n+"
    r"チョータツにご登録いただくと、メールでお送りしている案件に加えて、会員限定で公開している案件・人材もご覧いただけます。\n"
    r"さらに(?P=company)と連携いただくと、案件・人材の一覧がまとまり、お互いに提案できるようになります。\n+"
    r"チョータツは基本無料です。会社のメールアドレスがあれば1分で登録できます。\n+"
    r"▼ (?P=company)と連携する\n" + _CHO_LINK_URL + r"\n+"
    r"- - -\nチョータツでできること\n"
    r"・会員限定の案件・人材を検索・閲覧\n"
    r"・自社の案件・人材を、登録済みの配信先へワンクリックで一斉配信\n"
    r"・3000社以上の登録企業とつながる\n- - -\n+---\n"
    r"このメールアドレスは送信専用のため、ご返信いただけません。\n"
    r"ご不明な点・ご質問はお問い合わせください。\n" + _CHO_LINK_URL
)
_IDENTITY_LIST_MAIN_RE = re.compile(
    r"株式会社テクノヴァース\nご担当者様\n+お世話になっております。\n"
    r"アイデンティティーのビジネスパートナーチームです。\n+"
    r"本日時点で弊社で営業中のPython人材にマッチする案件を探しております。\n+"
    r"見合う案件がございましたら、ぜひご提案いただけますと幸いです。\n+"
    r"※スキルシートは、下記URL内のリンクからご確認くださいませ。\n"
    r"※ご提案いただく際は、商流をご教示いただくようお願い申し上げます。\n+"
    r"※ご提案いただきました案件はすべて確認させていただいておりますが、\n"
    r"弊社から2営業日以内に連絡がない場合は、お見送りとご判断くださいませ。\n+"
    r"■人材一覧リスト\n[—]+\n+https://info\.techcareer\.jp/e/998201/"
    r"MU-edit-gid-969571495-range-A1/\S+\n+[—]+\n+"
    r"以上、ご提案をお待ちしております。"
)
_IDENTITY_WEB_LIST_RULE_IDS = {
    "Webディレクター": "identity_web_director_list_portal_v1",
    "Webデザイナー": "identity_web_designer_list_portal_v1",
}
_IDENTITY_WEB_LIST_ROLE_PATTERN = "(?:{})".format(
    "|".join(re.escape(role) for role in _IDENTITY_WEB_LIST_RULE_IDS)
)
_IDENTITY_WEB_LIST_SUBJECT_RE = re.compile(
    r"ご提案可能な営業中(?P<role>" + _IDENTITY_WEB_LIST_ROLE_PATTERN + r")のご紹介"
)
_IDENTITY_WEB_LIST_RE = re.compile(
    r"株式会社テクノヴァース\nご担当者様\nお世話になっております。\n"
    r"アイデンティティーのビジネスパートナーチームです。\n"
    r"本日時点で弊社で営業中の(?P<role>" + _IDENTITY_WEB_LIST_ROLE_PATTERN + r")"
    r"にマッチする案件を探しております。\n"
    r"見合う案件がございましたら、ぜひご提案いただけますと幸いです。\n"
    r"※スキルシートは、下記URL内のリンクからご確認くださいませ。\n"
    r"※ご提案いただく際は、商流をご教示いただくようお願い申し上げます。\n"
    r"※ご提案いただきました案件はすべて確認させていただいておりますが、\n"
    r"弊社から2営業日以内に連絡がない場合は、お見送りとご判断くださいませ。\n"
    r"■人材一覧リスト\n[—]+\n"
    r"(?P<url>https://info\.techcareer\.jp/e/998201/"
    r"[A-Za-z0-9_-]+-edit-gid-[0-9]+(?:-range-[A-Za-z0-9]+)?/"
    r"[A-Za-z0-9_-]+/[0-9]+/h/[A-Za-z0-9_-]+)\n[—]+\n"
    r"以上、ご提案をお待ちしております。"
)
_RADSTATE_MULTI_RESOURCE_SUBJECT_RE = re.compile(
    r"更新しました!☆彡【直人材】.+\(ラッドステイト株式会社\)"
)
_RADSTATE_PROFILE_NUMBER_RE = re.compile(r"(?m)^[1-9][0-9]*[ \t]*$")
_RADSTATE_NAME_RE = re.compile(r"(?m)^氏名[ \t]*:")
_RADSTATE_STATION_RE = re.compile(r"(?m)^最寄駅[ \t]*:")
_RADSTATE_PRICE_RE = re.compile(r"(?m)^単価[ \t]*:")
_RADSTATE_SKILL_SHEET_RE = re.compile(r"(?m)^●スキルシート[ \t]*:?[ \t]*$")
_RADSTATE_SKILL_SHEET_URL_RE = re.compile(
    r"(?m)^●スキルシート[ \t]*:?[ \t]*\n[ \t]*https?://[^\s]+[ \t]*$"
)
_WEBBOLT_EMPLOYEE_INTRO_SUBJECT_RE = re.compile(
    r"◎◎弊社社員\([^()\n]+\)の紹介です。"
)
_WEBBOLT_EMPLOYEE_INTRO_RE = re.compile(
    r"\A\(BCCでご案内しております。\)\n+"
    r"お世話になります。ウェブボルトの増山です。\n+"
    r"標題の件、弊社要員をご紹介させていただきます。\n+"
    r"見合う案件がございましたら、[^\n]+\n+"
    r"ご紹介くださいますよう、よろしくお願い申しあげます。"
)
_WEBBOLT_NAME_RE = re.compile(r"(?m)^[ \t]*【名[ \t]+前】.+$")
_WEBBOLT_AFFILIATION_RE = re.compile(r"(?m)^[ \t]*【所[ \t]*属】.+$")
_WEBBOLT_STATION_RE = re.compile(r"(?m)^[ \t]*【最寄駅】.+$")
_WEBBOLT_AVAILABLE_RE = re.compile(r"(?m)^[ \t]*【就業時期】.+$")
_WEBBOLT_PRICE_RE = re.compile(r"(?m)^[ \t]*【単[ \t]+金】.+$")
_EXCEL_SKILL_SHEET_SUFFIXES = {".xls", ".xlsx", ".xlsm", ".xlsb"}
_TECHNICATION_INTRO_RE = re.compile(
    r"\A株式会社テクノヴァース\n+"
    r"ご担当者様\n+"
    r"お世話になっております。\n+"
    r"株式会社テクニケーションシードの[^\n]+です。\n+"
    r"[^\n]*をご紹介いたします。"
)
_TECHNICATION_NAME_RE = re.compile(r"(?m)^[ \t]*【氏[ \t]*名】.+$")
_TECHNICATION_AGE_RE = re.compile(r"(?m)^[ \t]*【年[ \t]*齢】.+$")
_TECHNICATION_AFFILIATION_RE = re.compile(r"(?m)^[ \t]*【所[ \t]*属】.+$")
_TECHNICATION_AVAILABLE_RE = re.compile(r"(?m)^[ \t]*【稼[ \t]*働】.+$")
_TECHNICATION_PRICE_RE = re.compile(r"(?m)^[ \t]*【単[ \t]*価】.+$")
_TECHNICATION_PHASE_RE = re.compile(r"(?m)^[ \t]*【工[ \t]*程】.+$")
_TECHNICATION_SKILL_RE = re.compile(r"(?m)^[ \t]*【スキル】.+$")
_TECHNICATION_INTRODUCTION_RE = re.compile(r"(?m)^[ \t]*【紹介文】.+$")
_SIGNPOST_INTRO_RE = re.compile(
    r"\A株式会社テクノヴァース\n+"
    r"[^\n]+様\n+"
    r"お世話になっております。\n+"
    r"株式会社SIGNPOSTでございます。\n+"
    r"表題の件についてご連絡させていただきました。\n+"
    r"(?:新しく営業スタートしている技術者情報をお送りさせていただきます。\n+)?"
    r"見合う案件ございましたらご紹介いただけますと幸いです。"
)
_SIGNPOST_NAME_RE = re.compile(r"(?m)^[ \t]*氏[ \t]*名[ \t]*:.+$")
_SIGNPOST_AFFILIATION_RE = re.compile(r"(?m)^[ \t]*所[ \t]*属[ \t]*:.+$")
_SIGNPOST_AGE_RE = re.compile(r"(?m)^[ \t]*年[ \t]*齢[ \t]*:.+$")
_SIGNPOST_STATION_RE = re.compile(r"(?m)^[ \t]*最[ \t]*寄[ \t]*:.+$")
_SIGNPOST_PRICE_RE = re.compile(r"(?m)^[ \t]*単[ \t]*価[ \t]*:.+$")
_SIGNPOST_PERIOD_RE = re.compile(r"(?m)^[ \t]*期[ \t]*間[ \t]*:.+$")
_SIGNPOST_WORK_STYLE_RE = re.compile(r"(?m)^[ \t]*働き方[ \t]*:.+$")
_SIGNPOST_OVERVIEW_RE = re.compile(
    r"(?m)^[ \t]*(?:概[ \t]*要[ \t]*:|概1\.[ \t]*概要(?:[ \t]*:)?)"
)
_SIGNPOST_SKILL_SHEET_URL_RE = re.compile(
    r"(?m)^[ \t]*スキルシート[ \t]*:[ \t]*https?://[^\s]+[ \t]*$"
)
_ALIPLAZA_MULTI_RESOURCE_SUBJECT_RE = re.compile(
    r"【[0-9]{2}/[0-9]{2}】【JAVA[ \t]+SE[ \t]*(?P<count>[0-9]+)名】"
    r"【要件定義~】.+"
)
_ALIPLAZA_MULTI_RESOURCE_INTRO_RE = re.compile(
    r"\A株式会社テクノヴァース\n+"
    r"営業担当様\n+"
    r"いつもお世話になっております。\n+"
    r"アリプラザのヒョウです。\n+"
    r"早速ですが、[ \t]*弊社現在注力中の要員情報を共有いたします。\n+"
    r"見合う案件がございましたら、\n+"
    r"ぜひご紹介いただけますと幸いです。"
)
_ALIPLAZA_RESOURCE_BLOCK_RE = re.compile(
    r"(?m)^要員情報([1-9][0-9]*)[ \t]*$"
)
_ALIPLAZA_AFFILIATION_RE = re.compile(r"(?m)^■[ \t]*所[ \t]*属[ \t]*:")
_ALIPLAZA_EXPERIENCE_RE = re.compile(r"(?m)^■[ \t]*経[ \t]*験[ \t]*:")
_ALIPLAZA_JAPANESE_RE = re.compile(r"(?m)^■[ \t]*日本語[ \t]*:")
_ALIPLAZA_SKILL_RE = re.compile(r"(?m)^■[ \t]*スキル[ \t]*:")
_ALIPLAZA_PHASE_RE = re.compile(r"(?m)^■[ \t]*工[ \t]*程[ \t]*:")
_ALIPLAZA_AVAILABLE_RE = re.compile(r"(?m)^■[ \t]*稼[ \t]*働[ \t]*:")
_ALIPLAZA_COMMUTE_RE = re.compile(r"(?m)^■[ \t]*通[ \t]*勤[ \t]*:")
_T_E_SYSTEM_RESOURCE_SUBJECT_RE = re.compile(
    r"【要員情報】インフラ要員/AWS、Azure、Docker、Kubernetes、Shell、Python"
)
_T_E_SYSTEM_RESOURCE_INTRO_RE = re.compile(
    r"\A株式会社テクノヴァース\n+"
    r"御担当者[ \t]*様\n+"
    r"いつもお世話になっております。TESの金子です。\n+"
    r"弊社より、(?:Java|インフラ)要員をご提案させていただきます。\n+"
    r"お手数おかけしますが、ご検討のほどよろしくお願いします。"
)
_T_E_SYSTEM_PROFILE_RE = re.compile(
    r"(?m)^【氏[ \t]*名】[ \t]*:?[ \t]*"
)
_T_E_SYSTEM_AGE_RE = re.compile(r"[0-9]{2}歳")
_T_E_SYSTEM_EXPERIENCE_RE = re.compile(
    r"(?m)^【(?:IT)?[ \t]*経[ \t]*験】"
)
_T_E_SYSTEM_JAPANESE_RE = re.compile(r"(?m)^【日本語】")
_T_E_SYSTEM_SKILL_RE = re.compile(r"(?m)^【(?:スキル|開発言語)】")
_T_E_SYSTEM_PHASE_RE = re.compile(r"(?m)^【工[ \t]*程】")
_T_E_SYSTEM_PRICE_RE = re.compile(r"(?m)^【単[ \t]*(?:価|金)】")
_T_E_SYSTEM_AVAILABLE_RE = re.compile(r"(?m)^【稼[ \t]*(?:働|動)(?:日)?】")
_T_E_SYSTEM_PR_RE = re.compile(r"(?m)^【PR】")
_CAIRN_MULTI_RESOURCE_SUBJECT_RE = re.compile(
    r"[0-9]{1,2}月~【弊社社員/サーバエンジニア×(?P<count>[0-9]+)名】.+"
)
_CAIRN_MULTI_RESOURCE_INTRO_RE = re.compile(
    r"\A株式会社テクノヴァース\n+"
    r"ご担当者[ \t]*様\n+"
    r"お世話になっております。\n+"
    r"株式会社CAIRNの営業部です。\n+"
    r"[0-9]{1,2}月より稼働可能な、弊社正社員の注力要員をご紹介させていただきます。\n+"
    r"見合う案件がございましたら、ぜひご紹介いただけますと幸いです。"
)
_CAIRN_PROFILE_RE = re.compile(r"(?m)^【名前】[ \t]*:[ \t]*")
_CAIRN_STATION_RE = re.compile(r"(?m)^【最寄】[ \t]*:")
_CAIRN_AVAILABLE_RE = re.compile(r"(?m)^【稼[働動]】[ \t]*:")
_CAIRN_AFFILIATION_RE = re.compile(r"(?m)^【所属】[ \t]*:")
_CAIRN_PRICE_RE = re.compile(r"(?m)^【単価】[ \t]*:")
_CAIRN_PHASE_RE = re.compile(r"(?m)^【工程】[ \t]*:")
_CAIRN_SKILL_RE = re.compile(r"(?m)^【スキル】[ \t]*:")
_CAIRN_PREFERENCE_RE = re.compile(r"(?m)^【希望】[ \t]*:")
_CAIRN_SKILL_SHEET_URL_RE = re.compile(
    r"(?m)^【スキルシート】[ \t]*:[ \t]*(?:\n[ \t]*)?"
    r"https?://[^\s]+[ \t]*$"
)
_SAKYA_PROCUREMENT_SERVICE_SUBJECT = (
    "受託案件のエンジニア調達、まるごとお引き受けします"
    "【0円・当日回答・1案件から】/ サクヤ"
)
_SAKYA_PROCUREMENT_SERVICE_NAME = (
    "サービス「サクヤ調達部」を行っております。"
)
_SAKYA_PROCUREMENT_SERVICE_SCALE = (
    "・協力会社5,000社 / 常時4,000名規模から、絞り込んだ候補のみご提案"
)
_SAKYA_PROCUREMENT_SERVICE_CTA = (
    "本メールにご返信のうえ、案件概要(スキル・単価帯・時期)をお送りください。"
)
_SAKYA_INDIVIDUAL_STRUCTURE_RE = re.compile(
    r"(?m)^(?:【(?:案件名|氏名|所属|最寄|単価|スキル|必須スキル)】"
    r"|(?:案件名|業務内容|作業内容|必須スキル)[ \t]*:)"
)
_SAKYA_RECIRCULATION_SERVICE_SUBJECT = (
    "当日中にご提案!!案件側失注の要員/他決後の再募集案件を承ります【サクヤ大嶽】"
)
_SAKYA_RECIRCULATION_SERVICE_INTRO = "株式会社サクヤの大嶽でございます。"
_SAKYA_RECIRCULATION_RESOURCE_NOTICE = (
    "【要員】オファー後に案件側が流れ、次を至急お探しの方"
)
_SAKYA_RECIRCULATION_PROJECT_NOTICE = (
    "【案件】オファーを出したが他決となり、再募集せざるを得ない案件"
)
_SAKYA_RECIRCULATION_SERVICE_CTA = (
    "スキルシート、または案件概要をそのまま転送いただくだけで構いません。"
)
_ROUTE_ZERO_INFORMATION_SHARE_SUBJECT = "【情報共有のお願い】"
_ROUTE_ZERO_INFORMATION_SHARE_REQUEST = (
    "この度は注力情報の共有をお願いしたく、ご連絡させていただきました。"
)
_ROUTE_ZERO_INFORMATION_SHARE_CTA = (
    "もしよろしければ本メールに返信で弊社の一社先でも可能な案件や"
    "貴社要員様などご紹介いただけますと幸いです。"
)
_ROUTE_ZERO_INFORMATION_SHARE_OFFER = (
    "また、弊社の注力情報(案件or要員ご指定下さい)もご入用でしたらお送りいたします。"
)
_CLARIFY_DISTRIBUTION_REGISTRATION_SUBJECT = "配信メールご登録のお願い"
_CLARIFY_DISTRIBUTION_REGISTRATION_CONTEXT = (
    "貴社より案件情報を配信されているメールアドレスの登録先に、"
)
_CLARIFY_DISTRIBUTION_REGISTRATION_REQUEST = (
    "弊社の下記アドレスを追加いただくことは可能でしょうか。"
)
_CLARIFY_DISTRIBUTION_REGISTRATION_ADDRESS = (
    "【配信先アドレス】\nsales@clarify.co.jp"
)
_ESMC_NETWORKING_EVENT_SUBJECT_RE = re.compile(
    r"【ご招待】ご担当者\s*様へ\s*まだ間に合う!ESMC交流会!"
    r"ご参加お待ちお待ちしております!"
)
_ESMC_NETWORKING_EVENT_ORGANIZER = (
    "いつも大変お世話になっております。ESMC運営事務局で御座います。"
)
_ESMC_NETWORKING_EVENT_ANNOUNCEMENT = "下記日時で、交流会を開催します。"
_ESMC_NETWORKING_EVENT_APPLICATION = ">>参加申込はコチラから!"
_ESMC_NETWORKING_EVENT_APPLICATION_URL_RE = re.compile(
    r"https://a17\.hm-f\.jp/cc\.php\?t=M[0-9]+&c=5915&d=5990"
)

# CHECK-02 (2026-10-30): 人数だけでは除外せず、同一メール内で人物profileの
# 境界labelと属性labelが人数分反復する場合だけ複数要員とみなす。
_REPEATED_PROFILE_NAME_RE = re.compile(
    r"(?im)^[ \t]*(?:[・■●◆◇][ \t]*)?【?[ \t]*"
    r"(?:氏[ \t]*名|名[ \t]*前)[ \t]*】?[ \t]*[:：]?"
)
_REPEATED_PROFILE_ATTRIBUTE_RES = (
    re.compile(
        r"(?im)^[ \t]*(?:[・■●◆◇][ \t]*)?【?[ \t]*"
        r"所[ \t]*属[ \t]*】?[ \t]*[:：]?"
    ),
    re.compile(
        r"(?im)^[ \t]*(?:[・■●◆◇][ \t]*)?【?[ \t]*"
        r"最[ \t]*寄(?:駅|り駅)?[ \t]*】?[ \t]*[:：]?"
    ),
    re.compile(
        r"(?im)^[ \t]*(?:[・■●◆◇][ \t]*)?【?[ \t]*"
        r"単[ \t]*(?:価|金)[ \t]*】?[ \t]*[:：]?"
    ),
)
_SUMMARY_PERSON_RE = re.compile(
    r"(?m)^[ \t]*・[ \t]*[A-Z]{1,3}[ \t]+[0-9]{2}歳(?:[ \t]|$)"
)

_REPEATED_FULL_PROFILE_HEADER = "【注力人材】のご提案"
_REPEATED_FULL_PROFILE_ID_RE = re.compile(
    r"(?m)^[ \t]*ID[ \t]*\n[ \t]*[:：][ \t]*(?P<id>[0-9]+)[ \t]*$"
)
_REPEATED_FULL_PROFILE_LABEL_RES = (
    re.compile(r"(?m)^[ \t]*イニシャル[ \t]*$"),
    re.compile(r"(?m)^[ \t]*最寄り駅[ \t]*$"),
    re.compile(r"(?m)^[ \t]*単金[ \t]*$"),
)

_MULTI_PROJECT_ENUMERATION_RE = re.compile(
    r"(?m)^[^\n]*①[^\n]*②[^\n]*③[^\n]*④[^\n]*⑤[^\n]*(?:案件|要員)[^\n]*$"
)
_MULTI_PROJECT_SEPARATOR_RE = re.compile(
    r"(?m)^[ \t]*[-ー―‐－]{20,}[ \t]*$"
)
_MULTI_PROJECT_BLOCK_LABEL_RE = re.compile(
    r"(?im)^[ \t]*(?:【|\{)?(?P<label>案件(?:名)?|プロジェクト概要|"
    r"期間|場所|勤務場所|単価|単金|人数|募集(?:人員)?|必要スキル|"
    r"必須スキル|作業内容|業務)(?:】|\})?[ \t]*[:：}]?"
)
_COMPLETE_PROJECT_BLOCK_HEADER_RE = re.compile(r"(?m)^[ \t]*【案件】")
_COMPLETE_PROJECT_BLOCK_REQUIRED_LABEL_RES = (
    re.compile(r"(?m)^[ \t]*【内容】"),
    re.compile(r"(?m)^[ \t]*【要件】"),
    re.compile(r"(?m)^[ \t]*【募集】"),
)

# CHECK-02 (2026-10-30): 明示された案件数と、完全な「■案件名」block数が
# 一致する場合だけを、件数明示形式の限定補完として扱う。
_COUNTED_PROJECT_SUBJECT_COUNT_RE = re.compile(
    r"(?<![0-9])(?P<count>(?:[2-9]|[1-9][0-9]+))[ \t]*件(?![0-9])"
)
_COUNTED_PROJECT_SUBJECT_PROJECT_COUNT_RE = re.compile(
    r"(?<![0-9])(?P<count>(?:[2-9]|[1-9][0-9]+))[ \t]*案件[ \t]*"
    r"(?:(?:が[ \t]*)?(?:動いて(?:います|おります)|あります|ございます)|"
    r"(?:を[ \t]*)?ご紹介(?:いた)?します)"
)
_COUNTED_PROJECT_BLOCK_HEADER_RE = re.compile(
    r"(?m)^[ \t]*■[ \t]*案件名[ \t]*[:：]"
)
_COUNTED_PROJECT_BLOCK_REQUIRED_LABEL_RES = (
    re.compile(r"(?m)^[ \t]*■[ \t]*概要[ \t]*[:：]"),
    re.compile(r"(?m)^[ \t]*■[ \t]*募集人数[ \t]*[:：]"),
    re.compile(r"(?m)^[ \t]*■[ \t]*単価[ \t]*[:：]"),
)

# CHECK-02 (2026-11-03): 既存multi-project root causeの表記差だけを補完する。
# 案件名の反復だけでは除外せず、各blockに内容・単価・他の案件属性が
# 揃い、かつ案件名が異なる場合だけ既存ruleに含める。
_BOXED_PROJECT_NAME_BLOCK_HEADER_RE = re.compile(
    r"(?m)^[ \t]*■[ \t]*案件名[ \t]*(?:■|[:：])?[ \t]*(?P<title>[^\n]*)$"
)
_FLEX_PROJECT_NAME_BLOCK_HEADER_RE = re.compile(
    r"(?m)^[ \t]*(?:"
    r"■[ \t]*案件名[ \t]*(?:■|[:：])?[ \t]*(?P<boxed_title>[^\n]*)|"
    r"案[ \t]*件(?:[ \t]*名)?[ \t]*[:：][ \t]*(?P<plain_title>[^\n]+)"
    r")[ \t]*$"
)
_COMPLETE_NAMED_PROJECT_CONTENT_RE = re.compile(
    r"(?im)^[ \t]*(?:"
    r"■[ \t]*(?:案件[ \t]*)?(?:内容|概要)[ \t]*(?:■|[:：])|"
    r"(?:内[ \t]*容|概[ \t]*要|業[ \t]*務[ \t]*内[ \t]*容|"
    r"作[ \t]*業[ \t]*内[ \t]*容)[ \t]*[:：]"
    r")"
)
_COMPLETE_NAMED_PROJECT_RATE_RE = re.compile(
    r"(?im)^[ \t]*(?:■[ \t]*)?単[ \t]*(?:価|金)[ \t]*(?:■|[:：])"
)
_COMPLETE_NAMED_PROJECT_OTHER_ATTRIBUTE_RES = (
    re.compile(
        r"(?im)^[ \t]*(?:■[ \t]*)?"
        r"(?:場[ \t]*所|勤[ \t]*務[ \t]*地|作[ \t]*業[ \t]*場[ \t]*所)"
        r"[ \t]*(?:■|[:：])"
    ),
    re.compile(
        r"(?im)^[ \t]*(?:■[ \t]*)?"
        r"(?:期[ \t]*間|開[ \t]*始[ \t]*時[ \t]*期)"
        r"[ \t]*(?:■|[:：])"
    ),
    re.compile(
        r"(?im)^[ \t]*(?:■[ \t]*)?"
        r"(?:ス[ \t]*キ[ \t]*ル|必[ \t]*須(?:[ \t]*ス[ \t]*キ[ \t]*ル|[ \t]*要[ \t]*件)?)"
        r"[ \t]*(?:■|[:：])"
    ),
    re.compile(
        r"(?im)^[ \t]*(?:■[ \t]*)?(?:面[ \t]*談|面[ \t]*接)"
        r"[ \t]*(?:■|[:：])"
    ),
    re.compile(
        r"(?im)^[ \t]*(?:■[ \t]*)?"
        r"(?:募[ \t]*集(?:[ \t]*人[ \t]*数|[ \t]*人[ \t]*員)?|人[ \t]*数)"
        r"[ \t]*(?:■|[:：])"
    ),
    re.compile(
        r"(?im)^[ \t]*(?:■[ \t]*)?"
        r"(?:フェーズ|時[ \t]*間|備[ \t]*考)"
        r"[ \t]*(?:■|[:：])"
    ),
)

# CHECK-02 (2026-10-30): 人数表現に依存せず、氏名境界ごとに3種類以上の
# 追加profile属性が揃う完全blockだけを数える。営業署名の氏名は
# 後続blockに属性が揃わないため対象外になる。
_COMPLETE_RESOURCE_PROFILE_LINE_PREFIX = (
    r"^[ \t]*(?:[0-9]{1,2}[ \t]*)?(?:[・■●◆◇□][ \t]*)?"
    r"(?:【[ \t]*)?"
)
_COMPLETE_RESOURCE_PROFILE_LINE_SUFFIX = r"(?:[ \t]*】)?[ \t]*[:：]?"
_COMPLETE_RESOURCE_PROFILE_NAME_RE = re.compile(
    _COMPLETE_RESOURCE_PROFILE_LINE_PREFIX
    + r"(?:氏[ \t]*名|名[ \t]*前|イニシャル)"
    + _COMPLETE_RESOURCE_PROFILE_LINE_SUFFIX,
    re.IGNORECASE | re.MULTILINE,
)
_COMPLETE_RESOURCE_PROFILE_ATTRIBUTE_RES = tuple(
    re.compile(
        _COMPLETE_RESOURCE_PROFILE_LINE_PREFIX
        + label
        + _COMPLETE_RESOURCE_PROFILE_LINE_SUFFIX,
        re.IGNORECASE | re.MULTILINE,
    )
    for label in (
        r"所[ \t]*属",
        r"最[ \t]*寄(?:駅|り駅)?",
        r"(?:希望[ \t]*)?単[ \t]*(?:価|金)",
        r"(?:稼[ \t]*(?:働|動)(?:日|開始)?|開始|入場日|参画時期)",
        r"年[ \t]*齢",
        r"国[ \t]*籍",
        r"性[ \t]*別",
        r"スキル",
    )
)


def _has_matching_target_detail(subject: str, body: str) -> bool:
    """個別profileまたは個別案件として扱う根拠があるかを確認する。"""
    combined = unicodedata.normalize("NFKC", "{}\n{}".format(subject, body))
    if _PROFILE_OR_PROJECT_RE.search(combined):
        return True
    if _ADDITIONAL_MATCHING_DETAIL_RE.search(combined):
        return True
    inline_count = sum(
        bool(pattern.search(combined)) for pattern in _INLINE_MATCHING_DETAIL_RES
    )
    return inline_count >= 2


def _has_list_portal_url(body: str, html_links: List) -> bool:
    """一覧ラベルが直接示す外部URLだけを一覧portalとして認識する。"""
    lines = body.splitlines()
    for index, line in enumerate(lines):
        label_match = _LIST_PORTAL_LABEL_RE.search(line)
        if not label_match:
            continue
        segments = [line[label_match.end():]]
        for next_line in lines[index + 1:index + 7]:
            segments.append(next_line)
        for segment_index, segment in enumerate(segments):
            url_match = _EXTERNAL_URL_RE.search(segment)
            if url_match:
                before_url = segments[:segment_index] + [segment[:url_match.start()]]
                if all(_LIST_LINK_SEPARATOR_RE.fullmatch(part) for part in before_url):
                    return True
                break
            if not _LIST_LINK_SEPARATOR_RE.fullmatch(segment):
                break

    for link in html_links:
        if not isinstance(link, dict):
            continue
        text = link.get("text")
        href = link.get("href")
        if (isinstance(text, str) and isinstance(href, str)
                and _LIST_PORTAL_LABEL_RE.search(text)
                and _EXTERNAL_URL_RE.fullmatch(href)):
            return True
    return False


def _is_external_list_portal_without_detail(
    record: Dict,
    subject: str,
    body: str,
) -> bool:
    """個別情報を持たず外部一覧だけを案内するメールを限定判定する。"""
    if record["attachments"]:
        return False
    combined = "{}\n{}".format(subject, body)
    if not _LIST_PORTAL_CONTEXT_RE.search(combined):
        return False
    if _has_matching_target_detail(subject, body):
        return False
    return _has_list_portal_url(body, record["html_links"])


def _repeated_resource_profile_count(subject: str, body: str) -> int:
    """明示人数/人物一覧とprofile label反復が一致する場合だけ人数を返す。"""
    profile_count = len(_REPEATED_PROFILE_NAME_RE.findall(body))
    if profile_count < 2:
        return 0

    attribute_counts = [
        len(pattern.findall(body)) for pattern in _REPEATED_PROFILE_ATTRIBUTE_RES
    ]
    if sum(count >= profile_count for count in attribute_counts) < 2:
        return 0

    count_expression = re.search(
        r"(?<![0-9]){}[ \t]*名".format(profile_count), subject
    )
    summary_count = len(_SUMMARY_PERSON_RE.findall(body))
    if not count_expression and summary_count < profile_count:
        return 0
    return profile_count


def _has_repeated_full_profile_blocks(body: str) -> bool:
    """完全な人材提案blockが異なるIDで2件以上連結された場合だけTrue。"""
    parts = body.split(_REPEATED_FULL_PROFILE_HEADER)
    blocks = parts[1:]
    if len(blocks) < 2:
        return False

    profile_ids = []
    for block in blocks:
        id_match = _REPEATED_FULL_PROFILE_ID_RE.search(block)
        if not id_match:
            return False
        if not all(pattern.search(block) for pattern in _REPEATED_FULL_PROFILE_LABEL_RES):
            return False
        profile_ids.append(id_match.group("id"))
    return len(set(profile_ids)) == len(profile_ids)


def _has_repeated_project_blocks(body: str) -> bool:
    """①〜⑤の案件列挙と、5つの案件構造blockが両方ある場合だけTrue。"""
    if not _MULTI_PROJECT_ENUMERATION_RE.search(body):
        return False
    body = unicodedata.normalize("NFKC", body)
    structured_blocks = 0
    for block in _MULTI_PROJECT_SEPARATOR_RE.split(body):
        labels = set(_MULTI_PROJECT_BLOCK_LABEL_RE.findall(block))
        if len(labels) >= 3:
            structured_blocks += 1
    return structured_blocks >= 5


def _has_repeated_complete_project_blocks(body: str) -> bool:
    """完全な【案件】blockが5件以上反復する場合だけTrue。"""
    normalized_body = unicodedata.normalize("NFKC", body)
    blocks = _COMPLETE_PROJECT_BLOCK_HEADER_RE.split(normalized_body)[1:]
    if len(blocks) >= 5 and all(
            all(pattern.search(block) for pattern in _COMPLETE_PROJECT_BLOCK_REQUIRED_LABEL_RES)
            for block in blocks):
        return True

    # 既存complete-block root causeの「■案件名■」等の表記差。
    return len(_complete_distinct_named_project_block_titles(
        normalized_body, _BOXED_PROJECT_NAME_BLOCK_HEADER_RE
    )) >= 2


def _complete_distinct_named_project_block_titles(
    body: str,
    header_re: re.Pattern,
) -> List[str]:
    """案件名と完全な属性が揃う、異なる案件blockの名称を返す。"""
    normalized_body = unicodedata.normalize("NFKC", body)
    headers = list(header_re.finditer(normalized_body))
    if len(headers) < 2:
        return []

    titles = []
    normalized_titles = []
    for index, header in enumerate(headers):
        block_end = headers[index + 1].start() if index + 1 < len(headers) else len(normalized_body)
        block = normalized_body[header.start():block_end]

        group_values = [value for value in header.groupdict().values() if value]
        title = group_values[0].strip() if group_values else ""
        if not title:
            following_lines = [
                line.strip()
                for line in normalized_body[header.end():block_end].splitlines()
                if line.strip()
            ]
            if not following_lines or following_lines[0].startswith(("■", "【")):
                return []
            title = following_lines[0]

        has_content = bool(_COMPLETE_NAMED_PROJECT_CONTENT_RE.search(block))
        has_rate = bool(_COMPLETE_NAMED_PROJECT_RATE_RE.search(block))
        other_attribute_count = sum(
            bool(pattern.search(block))
            for pattern in _COMPLETE_NAMED_PROJECT_OTHER_ATTRIBUTE_RES
        )
        if not (has_content and has_rate and other_attribute_count >= 2):
            return []

        normalized_title = re.sub(r"[ \t]+", "", title).lower()
        if not normalized_title:
            return []
        titles.append(title)
        normalized_titles.append(normalized_title)

    if len(set(normalized_titles)) != len(normalized_titles):
        return []
    return titles


def _has_counted_complete_project_blocks(subject: str, body: str) -> bool:
    """明示案件数と完全な案件block数が一致する場合だけTrue。"""
    normalized_subject = unicodedata.normalize("NFKC", subject)
    normalized_body = unicodedata.normalize("NFKC", body)

    subject_counts = {
        int(match.group("count"))
        for match in _COUNTED_PROJECT_SUBJECT_COUNT_RE.finditer(normalized_subject)
    }
    explicit_project_count_text = normalized_subject + "\n" + normalized_body
    subject_counts.update(
        int(match.group("count"))
        for match in _COUNTED_PROJECT_SUBJECT_PROJECT_COUNT_RE.finditer(
            explicit_project_count_text
        )
    )
    if len(subject_counts) != 1:
        return False
    expected_count = next(iter(subject_counts))

    blocks = _COUNTED_PROJECT_BLOCK_HEADER_RE.split(normalized_body)[1:]
    if len(blocks) == expected_count and all(
        all(pattern.search(block) for pattern in _COUNTED_PROJECT_BLOCK_REQUIRED_LABEL_RES)
        for block in blocks
    ):
        return True

    # 明示件数と、表記差を含む完全block数が一致する場合だけ補完。
    titles = _complete_distinct_named_project_block_titles(
        normalized_body, _FLEX_PROJECT_NAME_BLOCK_HEADER_RE
    )
    return len(titles) == expected_count


def _complete_resource_profile_block_count(body: str) -> int:
    """氏名境界と追加profile属性3種類以上が揃うblock数を返す。"""
    normalized_body = unicodedata.normalize("NFKC", body)
    profile_boundaries = list(_COMPLETE_RESOURCE_PROFILE_NAME_RE.finditer(normalized_body))
    complete_block_count = 0
    for index, boundary in enumerate(profile_boundaries):
        block_end = (
            profile_boundaries[index + 1].start()
            if index + 1 < len(profile_boundaries)
            else len(normalized_body)
        )
        block = normalized_body[boundary.start():block_end]
        attribute_count = sum(
            bool(pattern.search(block))
            for pattern in _COMPLETE_RESOURCE_PROFILE_ATTRIBUTE_RES
        )
        if attribute_count >= 3:
            complete_block_count += 1
    return complete_block_count


def _has_repeated_complete_resource_blocks(body: str) -> bool:
    """氏名境界と追加profile属性3種類以上が揃うblockが2件以上ならTrue。"""
    return _complete_resource_profile_block_count(body) >= 2


def _has_complete_project_structure(body: str) -> bool:
    """既存の案件block evidenceで、完全な案件構造が1件以上あるか判定する。"""
    normalized_body = unicodedata.normalize("NFKC", body)

    bracketed_blocks = _COMPLETE_PROJECT_BLOCK_HEADER_RE.split(normalized_body)[1:]
    if any(
        all(pattern.search(block) for pattern in _COMPLETE_PROJECT_BLOCK_REQUIRED_LABEL_RES)
        for block in bracketed_blocks
    ):
        return True

    headers = list(_FLEX_PROJECT_NAME_BLOCK_HEADER_RE.finditer(normalized_body))
    for index, header in enumerate(headers):
        block_end = headers[index + 1].start() if index + 1 < len(headers) else len(normalized_body)
        block = normalized_body[header.start():block_end]
        has_content = bool(_COMPLETE_NAMED_PROJECT_CONTENT_RE.search(block))
        has_rate = bool(_COMPLETE_NAMED_PROJECT_RATE_RE.search(block))
        other_attribute_count = sum(
            bool(pattern.search(block))
            for pattern in _COMPLETE_NAMED_PROJECT_OTHER_ATTRIBUTE_RES
        )
        if has_content and has_rate and other_attribute_count >= 2:
            return True
    return False


def _can_rescue_single_complete_resource_profile(
    record: Dict,
    p1_reason: Optional[str],
    template: Optional[Tuple[str, str]],
) -> bool:
    """manual除外より優先できる、完全な単一resource profileだけを判定する。"""
    if p1_reason or template:
        return False
    if not all(isinstance(record.get(k), str) for k in ("from", "subject", "body_text")):
        return False
    if not all(isinstance(record.get(k), list) for k in ("attachments", "html_links")):
        return False

    subject = record["subject"]
    body = record["body_text"]
    normalized_body = unicodedata.normalize("NFKC", body)
    profile_boundaries = list(_COMPLETE_RESOURCE_PROFILE_NAME_RE.finditer(normalized_body))

    # 完全blockが1件でも、別人物の氏名境界があれば単一人物とはみなさない。
    if len(profile_boundaries) != 1:
        return False
    if _complete_resource_profile_block_count(body) != 1:
        return False
    if _has_complete_project_structure(body):
        return False

    combined = "{}\n{}".format(subject, body)
    if (_LIST_PORTAL_CONTEXT_RE.search(combined)
            and _has_list_portal_url(body, record["html_links"])):
        return False
    return True


def detect_template_exclusion(record: Dict) -> Optional[Tuple[str, str]]:
    """送信元・件名・本文構造が揃う確認済みテンプレートのみ退避する。

    正規化は比較用コピーだけ。欠損・型不明は新規ルールで除外しない。
    一覧/通知は添付が一つでもあれば残す（処理対象添付を取りこぼさない）。
    """
    if not all(isinstance(record.get(k), str) for k in ("from", "subject", "body_text")):
        return None
    if not all(isinstance(record.get(k), list) for k in ("attachments", "html_links")):
        return None
    sender = extract_email(record["from"])
    subject = unicodedata.normalize("NFKC", record["subject"]).strip()
    raw_body = record["body_text"].replace("\r\n", "\n").strip()
    body = unicodedata.normalize("NFKC", raw_body)

    if (sender == "feng-h@aliplaza.co.jp"
            and re.fullmatch(r"【[0-9]{2}/[0-9]{2}】【JAVA\s+SE3名】【要件定義~】.+", subject)):
        blocks = re.split(r"(?m)^要員情報([123])\s*$", body)
        if (len(blocks) == 7 and blocks[1::2] == ["1", "2", "3"]
                and all(re.search(r"■\s*スキル\s*:", b)
                        and re.search(r"■\s*所\s*属\s*:", b)
                        and re.search(r"■\s*稼\s*働\s*:", b) for b in blocks[2::2])):
            return "multi_item_mail", "aliplaza_three_resource_blocks_v1"

    if sender == "feng-h@aliplaza.co.jp":
        subject_match = _ALIPLAZA_MULTI_RESOURCE_SUBJECT_RE.fullmatch(subject)
        if subject_match and _ALIPLAZA_MULTI_RESOURCE_INTRO_RE.search(body):
            parts = _ALIPLAZA_RESOURCE_BLOCK_RE.split(body)
            block_numbers = [int(number) for number in parts[1::2]]
            blocks = parts[2::2]
            profile_count = len(block_numbers)
            excel_skill_sheet_count = sum(
                1 for attachment in record["attachments"]
                if isinstance(attachment, dict)
                and isinstance(attachment.get("filename"), str)
                and Path(attachment["filename"]).suffix.lower()
                in _EXCEL_SKILL_SHEET_SUFFIXES
            )
            blocks_match = all(
                _ALIPLAZA_AFFILIATION_RE.search(block)
                and _ALIPLAZA_JAPANESE_RE.search(block)
                and (_ALIPLAZA_SKILL_RE.search(block)
                     or len(_ALIPLAZA_EXPERIENCE_RE.findall(block)) >= 2)
                and _ALIPLAZA_PHASE_RE.search(block)
                and _ALIPLAZA_AVAILABLE_RE.search(block)
                and _ALIPLAZA_COMMUTE_RE.search(block)
                for block in blocks
            )
            if (profile_count >= 2
                    and block_numbers == list(range(1, profile_count + 1))
                    and int(subject_match.group("count")) == profile_count
                    and blocks_match
                    and excel_skill_sheet_count == profile_count):
                return "multi_item_mail", "aliplaza_multi_resource_intro_v1"

    if (sender == "masaya.hayashi@hyperlinksolution.co.jp"
            and subject.startswith("【人材情報一覧】開発経験者3名のご紹介/")
            and "弊社プロパー人材情報のリストをお送り致します。" in body
            and re.search(r"【営業中プロパーリスト】\nhttps://x\.bmd\.jp/70/2209/\d+/255", body)):
        three = re.search(r"下記の3名を営業中。\s*\n(?P<rows>(?:・[^\n]+\n){3})", body)
        if three and all(re.search(r"要員\s+[0-9]+月~", row)
                         for row in three.group("rows").splitlines()):
            return "multi_item_mail", "hyperlink_three_resource_list_v1"

    if (sender == "atmaeda@radstate.co.jp"
            and _RADSTATE_MULTI_RESOURCE_SUBJECT_RE.fullmatch(subject)):
        profile_count = len(_RADSTATE_PROFILE_NUMBER_RE.findall(body))
        repeated_counts = (
            len(_RADSTATE_NAME_RE.findall(body)),
            len(_RADSTATE_STATION_RE.findall(body)),
            len(_RADSTATE_PRICE_RE.findall(body)),
            len(_RADSTATE_SKILL_SHEET_RE.findall(body)),
        )
        skill_sheet_url_count = len(_RADSTATE_SKILL_SHEET_URL_RE.findall(body))
        if (profile_count >= 2
                and all(count == profile_count for count in repeated_counts)
                and skill_sheet_url_count == profile_count):
            return "multi_item_mail", "radstate_multi_resource_list_v1"

    if (sender == "kmasuyama@webbolt.co.jp"
            and _WEBBOLT_EMPLOYEE_INTRO_SUBJECT_RE.fullmatch(subject)
            and _WEBBOLT_EMPLOYEE_INTRO_RE.search(body)):
        profile_count = len(_WEBBOLT_NAME_RE.findall(body))
        repeated_counts = (
            len(_WEBBOLT_AFFILIATION_RE.findall(body)),
            len(_WEBBOLT_STATION_RE.findall(body)),
            len(_WEBBOLT_AVAILABLE_RE.findall(body)),
            len(_WEBBOLT_PRICE_RE.findall(body)),
        )
        excel_skill_sheet_count = sum(
            1 for attachment in record["attachments"]
            if isinstance(attachment, dict)
            and isinstance(attachment.get("filename"), str)
            and Path(attachment["filename"]).suffix.lower() in _EXCEL_SKILL_SHEET_SUFFIXES
        )
        if (profile_count >= 2
                and all(count == profile_count for count in repeated_counts)
                and excel_skill_sheet_count == profile_count):
            return "multi_item_mail", "webbolt_multi_resource_employee_intro_v1"

    if (sender.endswith("@technication.co.jp")
            and _TECHNICATION_INTRO_RE.search(body)):
        profile_count = len(_TECHNICATION_NAME_RE.findall(body))
        repeated_counts = (
            len(_TECHNICATION_AGE_RE.findall(body)),
            len(_TECHNICATION_AFFILIATION_RE.findall(body)),
            len(_TECHNICATION_AVAILABLE_RE.findall(body)),
            len(_TECHNICATION_PRICE_RE.findall(body)),
            len(_TECHNICATION_PHASE_RE.findall(body)),
            len(_TECHNICATION_SKILL_RE.findall(body)),
            len(_TECHNICATION_INTRODUCTION_RE.findall(body)),
        )
        excel_skill_sheet_count = sum(
            1 for attachment in record["attachments"]
            if isinstance(attachment, dict)
            and isinstance(attachment.get("filename"), str)
            and Path(attachment["filename"]).suffix.lower() in _EXCEL_SKILL_SHEET_SUFFIXES
        )
        if (profile_count >= 2
                and all(count == profile_count for count in repeated_counts)
                and excel_skill_sheet_count == profile_count):
            return "multi_item_mail", "technication_multi_resource_intro_v1"

    if (sender == "sales@signpost-g.co.jp"
            and not record["attachments"]
            and _SIGNPOST_INTRO_RE.search(body)):
        profile_count = len(_SIGNPOST_NAME_RE.findall(body))
        repeated_counts = (
            len(_SIGNPOST_AFFILIATION_RE.findall(body)),
            len(_SIGNPOST_AGE_RE.findall(body)),
            len(_SIGNPOST_STATION_RE.findall(body)),
            len(_SIGNPOST_PRICE_RE.findall(body)),
            len(_SIGNPOST_PERIOD_RE.findall(body)),
            len(_SIGNPOST_WORK_STYLE_RE.findall(body)),
            len(_SIGNPOST_OVERVIEW_RE.findall(body)),
        )
        skill_sheet_url_count = len(_SIGNPOST_SKILL_SHEET_URL_RE.findall(body))
        if (profile_count >= 2
                and all(count == profile_count for count in repeated_counts)
                and 1 <= skill_sheet_url_count <= profile_count):
            return "multi_item_mail", "signpost_multi_resource_intro_v1"

    if (sender == "kinmh@t-e-system.com"
            and _T_E_SYSTEM_RESOURCE_SUBJECT_RE.fullmatch(subject)
            and _T_E_SYSTEM_RESOURCE_INTRO_RE.search(body)):
        profile_blocks = _T_E_SYSTEM_PROFILE_RE.split(body)[1:]
        profile_count = len(profile_blocks)
        blocks_match = all(
            _T_E_SYSTEM_AGE_RE.search(block)
            and _T_E_SYSTEM_EXPERIENCE_RE.search(block)
            and _T_E_SYSTEM_JAPANESE_RE.search(block)
            and _T_E_SYSTEM_SKILL_RE.search(block)
            and _T_E_SYSTEM_PHASE_RE.search(block)
            and _T_E_SYSTEM_PRICE_RE.search(block)
            and _T_E_SYSTEM_AVAILABLE_RE.search(block)
            and _T_E_SYSTEM_PR_RE.search(block)
            for block in profile_blocks
        )
        excel_skill_sheet_count = sum(
            1 for attachment in record["attachments"]
            if isinstance(attachment, dict)
            and isinstance(attachment.get("filename"), str)
            and Path(attachment["filename"]).suffix.lower()
            in _EXCEL_SKILL_SHEET_SUFFIXES
        )
        if (profile_count >= 2
                and blocks_match
                and excel_skill_sheet_count == profile_count):
            return "multi_item_mail", "t_e_system_multi_resource_intro_v1"

    if (sender == "sales@cair-n.co.jp"
            and not record["attachments"]
            and _CAIRN_MULTI_RESOURCE_INTRO_RE.search(body)):
        subject_match = _CAIRN_MULTI_RESOURCE_SUBJECT_RE.fullmatch(subject)
        profile_blocks = _CAIRN_PROFILE_RE.split(body)[1:]
        profile_count = len(profile_blocks)
        blocks_match = all(
            _CAIRN_STATION_RE.search(block)
            and _CAIRN_AVAILABLE_RE.search(block)
            and _CAIRN_AFFILIATION_RE.search(block)
            and _CAIRN_PRICE_RE.search(block)
            and _CAIRN_PHASE_RE.search(block)
            and _CAIRN_SKILL_RE.search(block)
            and _CAIRN_PREFERENCE_RE.search(block)
            and _CAIRN_SKILL_SHEET_URL_RE.search(block)
            for block in profile_blocks
        )
        skill_sheet_url_count = len(_CAIRN_SKILL_SHEET_URL_RE.findall(body))
        if (subject_match
                and profile_count >= 2
                and int(subject_match.group("count")) == profile_count
                and blocks_match
                and skill_sheet_url_count == profile_count):
            return "multi_item_mail", "cairn_multi_resource_intro_v1"

    # 既存の限定templateを先に評価し、同じメールの既存rule_idを変更しない。
    if _repeated_resource_profile_count(subject, body) >= 2:
        return "multi_item_mail", "repeated_resource_profiles_v1"

    if _has_repeated_full_profile_blocks(body):
        return "multi_item_mail", "repeated_full_profile_blocks_v1"

    if _has_repeated_project_blocks(raw_body):
        return "multi_item_mail", "five_project_blocks_v1"

    if _has_repeated_complete_project_blocks(raw_body):
        return "multi_item_mail", "repeated_complete_project_blocks_v1"

    if _has_counted_complete_project_blocks(subject, raw_body):
        return "multi_item_mail", "counted_two_project_blocks_v1"

    if _has_repeated_complete_resource_blocks(raw_body):
        return (
            "multi_item_mail",
            "repeated_complete_resource_blocks_without_accepted_count_v1",
        )

    if (sender == "kensuke.kiyota@sakya.jp"
            and subject == _SAKYA_PROCUREMENT_SERVICE_SUBJECT
            and not record["attachments"]
            and _SAKYA_PROCUREMENT_SERVICE_NAME in body
            and _SAKYA_PROCUREMENT_SERVICE_SCALE in body
            and _SAKYA_PROCUREMENT_SERVICE_CTA in body
            and not _SAKYA_INDIVIDUAL_STRUCTURE_RE.search(body)):
        return "service_notification", "sakya_procurement_service_notice_v1"

    if (sender == "koki.odake@sakya.jp"
            and subject == _SAKYA_RECIRCULATION_SERVICE_SUBJECT
            and not record["attachments"]
            and _SAKYA_RECIRCULATION_SERVICE_INTRO in body
            and _SAKYA_RECIRCULATION_RESOURCE_NOTICE in body
            and _SAKYA_RECIRCULATION_PROJECT_NOTICE in body
            and _SAKYA_RECIRCULATION_SERVICE_CTA in body
            and not _SAKYA_INDIVIDUAL_STRUCTURE_RE.search(body)):
        return "service_notification", "sakya_recirculation_service_notice_v1"

    if (sender == "takahashi@route-zero.com"
            and subject == _ROUTE_ZERO_INFORMATION_SHARE_SUBJECT
            and not record["attachments"]
            and _ROUTE_ZERO_INFORMATION_SHARE_REQUEST in body
            and _ROUTE_ZERO_INFORMATION_SHARE_CTA in body
            and _ROUTE_ZERO_INFORMATION_SHARE_OFFER in body
            and not _SAKYA_INDIVIDUAL_STRUCTURE_RE.search(body)):
        return "service_notification", "route_zero_information_share_notice_v1"

    if (sender == "sales@clarify.co.jp"
            and subject == _CLARIFY_DISTRIBUTION_REGISTRATION_SUBJECT
            and not record["attachments"]
            and not record["html_links"]
            and _CLARIFY_DISTRIBUTION_REGISTRATION_CONTEXT in body
            and _CLARIFY_DISTRIBUTION_REGISTRATION_REQUEST in body
            and _CLARIFY_DISTRIBUTION_REGISTRATION_ADDRESS in body
            and not _PROFILE_OR_PROJECT_RE.search(body)):
        return (
            "service_notification",
            "mail_distribution_registration_notice_v1",
        )

    if (sender == "esmc@ses.cre-co.jp"
            and _ESMC_NETWORKING_EVENT_SUBJECT_RE.fullmatch(subject)
            and not record["attachments"]
            and _ESMC_NETWORKING_EVENT_ORGANIZER in body
            and _ESMC_NETWORKING_EVENT_ANNOUNCEMENT in body
            and _ESMC_NETWORKING_EVENT_APPLICATION in body
            and not _PROFILE_OR_PROJECT_RE.search(body)):
        application_links = [
            link for link in record["html_links"]
            if isinstance(link, dict)
            and link.get("source") == "text/html"
            and link.get("text") == "参加申込はコチラから！"
            and isinstance(link.get("href"), str)
            and _ESMC_NETWORKING_EVENT_APPLICATION_URL_RE.fullmatch(link["href"])
        ]
        if len(application_links) == 1:
            return "service_notification", "esmc_networking_event_notice_v1"

    if record["attachments"] or _PROFILE_OR_PROJECT_RE.search(body):
        return None

    if sender == "bp@id-entity.jp":
        subject_match = _IDENTITY_WEB_LIST_SUBJECT_RE.fullmatch(subject)
        # 比較用コピーのみ空行・行頭末空白を正規化する。職種は2語のallowlist限定。
        main_body = "\n".join(line.strip() for line in body.partition("━━")[0].splitlines()
                              if line.strip())
        template = _IDENTITY_WEB_LIST_RE.fullmatch(main_body)
        if (subject_match and template
                and subject_match.group("role") == template.group("role")
                and any(
            isinstance(link, dict) and link.get("source") == "text/html"
            and link.get("href") == template.group("url")
            for link in record["html_links"]
        )):
            role = subject_match.group("role")
            return "list_or_portal_notice", _IDENTITY_WEB_LIST_RULE_IDS[role]

    if sender == "noreply@cho-tatsu.com":
        invite = _CHO_INVITE_BODY_RE.fullmatch(body)
        if (invite and subject == "{}の{}様よりチョータツでの連携のご依頼".format(
                invite.group("company"), invite.group("person"))):
            return "service_notification", "chotatsu_connection_invitation_v1"

    if (sender == "noreply@driven-x.com"
            and re.fullmatch(r"【チョータツブースト】 新着案件・人材\([0-9]{2}月[0-9]{2}日\)", subject)
            and body == "チョータツブーストの新着案件・人材情報"):
        hrefs = {link.get("href") for link in record["html_links"]
                 if isinstance(link, dict) and isinstance(link.get("href"), str)}
        if {"https://cho-tatsu.com/boost/talents", "https://cho-tatsu.com/boost/projects"} <= hrefs:
            return "service_notification", "chotatsu_boost_digest_v1"

    if (sender == "bp@id-entity.jp"
            and subject == "ご提案可能な営業中Python人材のご紹介"
            and _IDENTITY_LIST_MAIN_RE.fullmatch(body.partition("━━")[0].strip())):
        return "list_or_portal_notice", "identity_python_list_portal_v1"

    # 既存の個別templateを優先したうえで、職種名・会社名には依存せず、
    # 一覧ラベル + 外部URL + 個別情報不存在の構造だけをfallback判定する。
    if _is_external_list_portal_without_detail(record, subject, body):
        return (
            "list_or_portal_notice",
            "external_list_portal_without_individual_detail_v1",
        )
    return None


def build_detail_record(record: Dict, reason: str, rule_id: str) -> Dict:
    """元レコード全項目を保持する。監査キー衝突時は上書きせず停止する。"""
    if "reason" in record or "rule_id" in record:
        raise ValueError("mail master contains reserved audit keys: reason / rule_id")
    return dict(record, reason=reason, rule_id=rule_id)

RESOURCE_NOUN_PATTERN = r"(?:要員|人材|エンジニア|技術者|プロパー)"
RESOURCE_COUNT_PATTERNS = (
    re.compile(
        rf"(?P<count>[0-9]+)名の{RESOURCE_NOUN_PATTERN}"
        rf"|{RESOURCE_NOUN_PATTERN}(?:[ :])?(?P<count_after>[0-9]+)名"
    ),
    re.compile(
        rf"{RESOURCE_NOUN_PATTERN}[ ]*(?:×|x)[ ]*(?P<count_after>[0-9]+)名"
    ),
    re.compile(r"(?P<count>[0-9]+)名で1人月"),
)
RESOURCE_ROLE_SET_PATTERN = re.compile(
    r"(?<![a-z0-9])(?:pmo|pm|pl|pg|se)[ ]*(?P<count>[0-9]+)名セット"
)
RESOURCE_CONTEXT_PATTERN = re.compile(RESOURCE_NOUN_PATTERN)
PROJECT_COUNT_PATTERNS = (
    re.compile(r"案件(?P<count>[0-9]+)件"),
    re.compile(r"(?P<count>[0-9]+)件の案件"),
    re.compile(r"(?P<count>[0-9]+)案件[ ]*(?:ご?共有|ご?紹介)"),
)

RECRUITMENT_VETO_WORDS = (
    "募集",
    "募集枠",
    "増員",
    "必要人数",
    "急募",
    "求人",
    "採用",
)
MANAGEMENT_VETO_WORDS = (
    "マネジメント実績",
    "管理実績",
    "管理経験",
    "マネジメント経験",
)

_LEADING_REPLY_PREFIX_RE = re.compile(r"^(?:re|fw|fwd)\s*:\s*", re.IGNORECASE)
_LEADING_CATEGORY_TAG_RE = re.compile(r"^(?:【[^】]+】|\[[^\]]+\])\s*")
_LIST_DATE_PATTERN = (
    r"(?:(?:20[0-9]{2}[/-])?[0-9]{1,2}[/-][0-9]{1,2}"
    r"|(?:20[0-9]{2}年)?[0-9]{1,2}月[0-9]{1,2}日)"
)
_LIST_AS_PRIMARY_RE = re.compile(
    r"(?:弊社)?(?:営業中)?"
    r"(?:要員一覧|要員リスト|要員共有|人材一覧|人材リスト|人材共有|"
    r"案件一覧|案件リスト|案件・要員まとめ|案件/要員まとめ)"
    r"(?:の送付|送付|の共有|共有|のご案内|ご案内|案内|更新)?"
    rf"(?:\s*{_LIST_DATE_PATTERN})?"
)


def normalize(s: str) -> str:
    """比較用に正規化（NFKC + 小文字 + 前後空白除去）。"""
    return unicodedata.normalize("NFKC", s or "").strip().lower()


def normalize_detector_subject(subject: str) -> str:
    """P1 Subject判定用にNFKC正規化し、連続空白を1文字へ圧縮する。"""
    normalized = unicodedata.normalize("NFKC", subject or "").strip().lower()
    return re.sub(r"\s+", " ", normalized)


def _match_count(match: re.Match) -> int:
    """count/count_afterのうち一致したASCII数字をintで返す。"""
    value = match.groupdict().get("count") or match.groupdict().get("count_after")
    return int(value)


def _has_explicit_multiple_resource(subject: str) -> bool:
    """狭いSubject grammarで明示された複数要員提案だけを判定する。"""
    if any(word in subject for word in RECRUITMENT_VETO_WORDS):
        return False
    if any(word in subject for word in MANAGEMENT_VETO_WORDS):
        return False

    for pattern in RESOURCE_COUNT_PATTERNS:
        for match in pattern.finditer(subject):
            if _match_count(match) < 2:
                continue
            if subject[match.end() :].startswith(("規模", "参画中")):
                continue
            return True

    if RESOURCE_CONTEXT_PATTERN.search(subject):
        for match in RESOURCE_ROLE_SET_PATTERN.finditer(subject):
            if _match_count(match) >= 2:
                return True
    return False


def _strip_list_leading_markers(subject: str) -> str:
    """一覧判定時だけ、先頭に連続するreply prefix/category tagを除去する。"""
    stripped = subject
    while stripped:
        reply_match = _LEADING_REPLY_PREFIX_RE.match(stripped)
        if reply_match:
            stripped = stripped[reply_match.end() :].strip()
            continue

        tag_match = _LEADING_CATEGORY_TAG_RE.match(stripped)
        if tag_match:
            stripped = stripped[tag_match.end() :].strip()
            continue
        break
    return re.sub(r"\s+", " ", stripped)


def _list_as_primary_reason(subject: str) -> Optional[str]:
    """前処理後Subject全体が狭い一覧grammarへfullmatchした場合だけ理由を返す。"""
    list_subject = _strip_list_leading_markers(subject)
    if not _LIST_AS_PRIMARY_RE.fullmatch(list_subject):
        return None
    if "案件" in list_subject:
        return "project_list_as_primary_subject"
    return "resource_list_as_primary_subject"


def _has_explicit_multiple_project(subject: str) -> bool:
    """案件N件/N件の案件/N案件の共有・紹介で明示的な複数案件を判定する。"""
    for pattern in PROJECT_COUNT_PATTERNS:
        for match in pattern.finditer(subject):
            if _match_count(match) >= 2:
                return True
    return False


def detect_p1_exclusion_reason(subject: str) -> Optional[str]:
    """Subjectだけを使うconservativeなP1 detector。非該当(TYPE D)はNone。"""
    normalized_subject = normalize_detector_subject(subject)

    if _has_explicit_multiple_resource(normalized_subject):
        return "multiple_resource_explicit_subject_count"

    list_reason = _list_as_primary_reason(normalized_subject)
    if list_reason:
        return list_reason

    if _has_explicit_multiple_project(normalized_subject):
        return "multiple_project_explicit_subject_count"

    return None


def extract_email(s: str) -> str:
    """
    'Display Name <email@example.com>' や '<email@example.com>' から
    メールアドレス部分だけを抽出して正規化する。
    '<>' が存在しない場合はそのまま normalize する。
    """
    m = re.search(r"<([^>]+)>", s or "")
    if m:
        return normalize(m.group(1))
    return normalize(s)


def subject_matches(pattern: str, subject: str) -> bool:
    """
    subject の一致判定。
    pattern に * が含まれる場合は fnmatch によるワイルドカード一致。
    * が含まれない場合は完全一致。
    どちらも正規化済みの文字列を受け取ること。
    """
    if "*" in pattern:
        return fnmatch.fnmatch(subject, pattern)
    return pattern == subject


def load_exclude_list(path: str) -> Tuple[Set[str], List[Tuple[str, str]]]:
    """
    除外リストを読み込む。
    戻り値:
      from_only_set   : from のみ指定（正規化済みアドレスのセット）
      from_subj_rules : (from, subject_pattern) のリスト（正規化済み）
                        subject_pattern は * を含む場合はワイルドカードとして扱う
    """
    from_only_set: Set[str] = set()
    from_subj_rules: List[Tuple[str, str]] = []

    exclude_path = Path(path)
    if not exclude_path.exists():
        logger.warn(f"除外リストファイルが存在しません: {path}")
        return from_only_set, from_subj_rules

    with open(exclude_path, encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split(",", 1)
            if len(parts) == 1:
                # from のみ
                from_only_set.add(normalize(parts[0]))
            else:
                # from,subject（subject は * ありの場合はワイルドカードパターン）
                from_subj_rules.append((normalize(parts[0]), normalize(parts[1])))

    logger.info(
        f"除外リスト読み込み完了: fromのみ={len(from_only_set)}件, "
        f"from+subject={len(from_subj_rules)}件"
    )
    return from_only_set, from_subj_rules


def is_excluded(
    record: Dict,
    from_only_set: Set[str],
    from_subj_rules: List[Tuple[str, str]],
) -> bool:
    """レコードが除外対象かどうかを判定する。"""
    norm_from = extract_email(record.get("from") or "")
    norm_subject = normalize(record.get("subject") or "")

    if norm_from in from_only_set:
        return True
    for rule_from, rule_subj in from_subj_rules:
        if rule_from == norm_from and subject_matches(rule_subj, norm_subject):
            return True
    return False


def determine_exclusion_reason(
    record: Dict,
    from_only_set: Set[str],
    from_subj_rules: List[Tuple[str, str]],
) -> Optional[str]:
    """manual除外を保ち、完全な単一profileだけ構造根拠で救済する。"""
    manual_excluded = is_excluded(record, from_only_set, from_subj_rules)
    p1_reason = detect_p1_exclusion_reason(record.get("subject") or "")
    template = detect_template_exclusion(record)

    if manual_excluded:
        if _can_rescue_single_complete_resource_profile(record, p1_reason, template):
            return None
        return "manual_exclude_list"
    if p1_reason:
        return p1_reason
    return template[0] if template else None


def main() -> None:
    dirs = ensure_result_dirs(str(_STEP_DIR))
    result_dir = str(dirs["result"])

    start_time = time.time()
    filtered_records: List[Dict] = []
    removed_records: List[Dict] = []
    detail_records: List[Dict] = []
    exclusion_reason_counts: Dict[str, int] = {}

    try:
        # 除外リスト読み込み
        from_only_set, from_subj_rules = load_exclude_list(EXCLUDE_LIST_PATH)

        # メールマスタ読み込み（message_id → レコード）
        logger.info(f"メールマスタ読み込み: {INPUT_MASTER}")
        master = read_jsonl_as_dict(INPUT_MASTER, key="message_id")
        logger.info(f"メールマスタ件数: {len(master)}件")

        # 01-2 の出力（処理対象 message_id）読み込み
        logger.info(f"01-2 出力読み込み: {INPUT_PREV}")
        prev_records = read_jsonl_as_list(INPUT_PREV)
        input_count = len(prev_records)
        logger.info(f"01-2 入力件数: {input_count}件")

        not_found_count = 0
        for rec in prev_records:
            mid = rec.get("message_id", "")
            master_rec = master.get(mid)
            if master_rec is None:
                logger.warn(f"メールマスタに存在しない message_id: {mid}")
                not_found_count += 1
                # マスタになくても除外せずに残す（安全側）
                filtered_records.append({"message_id": mid})
                continue

            exclusion_reason = determine_exclusion_reason(
                master_rec, from_only_set, from_subj_rules
            )
            if exclusion_reason:
                removed_records.append({"message_id": mid})
                if exclusion_reason in TEMPLATE_REASONS:
                    reason, rule_id = detect_template_exclusion(master_rec)
                    detail_records.append(build_detail_record(master_rec, reason, rule_id))
                exclusion_reason_counts[exclusion_reason] = (
                    exclusion_reason_counts.get(exclusion_reason, 0) + 1
                )
            else:
                filtered_records.append({"message_id": mid})

        if not_found_count > 0:
            logger.warn(f"メールマスタに存在しなかった件数: {not_found_count}件")

        logger.info(
            f"個別除外: {input_count}件 → {len(filtered_records)}件 "
            f"（除外: {len(removed_records)}件）"
        )
        logger.info(f"除外理由別件数: {exclusion_reason_counts}")

        # 出力①: 除外後 message_id
        out_filtered = str(dirs["result"] / OUTPUT_FILTERED)
        write_jsonl(out_filtered, filtered_records)
        logger.ok(f"出力①書き込み完了: {out_filtered} ({len(filtered_records)}件)")

        # 出力②: 除外された message_id
        out_removed = str(dirs["result"] / OUTPUT_REMOVED)
        write_jsonl(out_removed, removed_records)
        logger.ok(f"出力②書き込み完了: {out_removed} ({len(removed_records)}件)")

        out_detail = str(dirs["result"] / OUTPUT_DETAIL)
        write_jsonl(out_detail, detail_records)
        logger.ok(f"出力③書き込み完了: {out_detail} ({len(detail_records)}件)")

    except Exception as e:
        write_error_log(result_dir, e, context=f"input={INPUT_PREV}")
        logger.error(f"処理失敗: {e}")
        sys.exit(1)

    finally:
        elapsed = time.time() - start_time
        write_execution_time(
            str(dirs["execution_time"]),
            STEP_NAME,
            elapsed,
            record_count=len(filtered_records),
        )

    logger.ok(
        f"Step完了: 入力={input_count}件 / 出力={len(filtered_records)}件 / "
        f"除外={len(removed_records)}件"
    )


if __name__ == "__main__":
    main()
