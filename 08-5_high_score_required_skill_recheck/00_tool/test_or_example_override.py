import importlib.util
import unittest
from copy import deepcopy
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("high_score_required_skill_recheck.py")
SPEC = importlib.util.spec_from_file_location("high_score_required_skill_recheck", MODULE_PATH)
target = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(target)


def check(skill, reason, evidence, original_match=False, confidence="human_review"):
    return {
        "skill": skill,
        "original_match": original_match,
        "recheck_match": confidence != "not_confirmed",
        "confidence": confidence,
        "reason": reason,
        "evidence": evidence,
    }


# 20260819 productionで旧OR・例示補正がfalse→confirmedにした27 pair / 28 checks。
# 人手確認結果（VALID=2 / INVALID=18 / AMBIGUOUS=7）を固定fixtureとして保持する。
PRODUCTION_CASES = [
    ("VALID", "1a017f765b3fe445", "1a01860b51df663c", [
        check("複数システム間の連携(API、バッチ等)を考慮したシステム設計経験", "バッチ処理の刷新経験はあるが、API連携の具体的な記載が不足", "受注処理を時間起動バッチからイベントドリブンへ刷新")]),
    ("INVALID", "1a017ee0d09b85eb", "1a0175dd9787f10f", [
        check("IF(API連携等)の設計・構築経験", "API連携の具体的な設計・構築経験が不明", "AWS (EC2, S3, RDS, Lambda, IAM, ECS)")]),
    ("INVALID", "1a017ee002ba9649", "1a0190a8b27641e9", [
        check("Java/SQLでの開発経験5年以上", "Java経験はあるが、年数が明確に5年以上と確認できない", "Java, Angular, KedoUI, MySQL, DB設計")]),
    ("INVALID", "1a017ee002ba9649", "1a01829e021a86af", [
        check("Java/SQLでの開発経験5年以上", "Java経験は3年10ヶ月、SQLは2年で合計5年以上の経験があるが、Java単体で5年に満たないため確認が必要", "Java | 3年10カ月, SQL | 2年")]),
    ("INVALID", "1a017ee002ba9649", "1a0179ed21d76b67", [
        check("Java/SQLでの開発経験5年以上", "Javaの経験はあるが、SQLの経験が明確でなく、年数も不明確", "Java 〇")]),
    ("AMBIGUOUS", "1a017accfd4b2894", "1a017b815091f7db", [
        check("生成AI、機械学習、外部AI APIのいずれかを用いた機能開発経験", "AIエージェントDevinを用いたAI駆動開発の記載があるが、詳細不明", "一部AIエージェントDevinを用いたAI駆動開発")]),
    ("INVALID", "1a0183ac8eb1aef0", "1a0175dd9787f10f", [
        check("Azure DevOPsを用いたCI/CDに関する基本知識", "Azureの経験はあるが、DevOpsやCI/CDの具体的な記載が不明確", "Azure基盤の設計・構築を特に得意")]),
    ("AMBIGUOUS", "1a017a8d344e2651", "1a0179441bfff43a", [
        check("AWSサーバレス/マネージドサービスの設計経験", "AWSの利用はあるが、サーバレス/マネージドサービスの設計経験が不明", "AWS　EC2、S3、RDS")]),
    ("AMBIGUOUS", "1a017f765b3fe445", "1a0183d71241f034", [
        check("AWS等クラウド環境でのネイティブアーキテクチャ設計経験", "AWS環境での経験はあるが、ネイティブアーキテクチャ設計経験の記載が不明確。", "AWS(EC2,S3)経験あり")]),
    ("AMBIGUOUS", "1a017f765b3fe445", "1a017c0502af2e43", [
        check("AWS等クラウド環境でのネイティブアーキテクチャ設計経験", "AWSの使用経験はあるが、ネイティブアーキテクチャ設計経験の記載が不明確", "AWS, ソーシャルゲームデータのツール作成")]),
    ("INVALID", "1a017ee0d09b85eb", "1a01908f91c4ac5c", [
        check("IF(API連携等)の設計・構築経験", "API連携の具体的な設計・構築経験の記載が不明確", "API：Seasar2, WildFly 18.0.1.Final")]),
    ("INVALID", "1a017ee0d09b85eb", "1a017e168af626ea", [
        check("IF(API連携等)の設計・構築経験", "API連携の具体的な設計・構築経験の記載が不明確", "Java, JSON, Node.js")]),
    ("INVALID", "1a017ee002ba9649", "1a0178f4decab100", [
        check("Java/SQLでの開発経験5年以上", "Javaの経験が不明確、SQLは経験あり", "SQL Server, MySQL 8.0")]),
    ("INVALID", "1a017ee002ba9649", "1a01783d735e6f1d", [
        check("Java/SQLでの開発経験5年以上", "Javaの経験はあるが、SQLの経験が明確でない。年数も不明確。", "Java | 〇")]),
    ("INVALID", "1a017735c71cec5f", "1a018d25f6369674", [
        check("LLM / RAGを組み込んだWebアプリ開発経験2年以上", "LLM / RAGの具体的な経験が不明", "AI駆動開発を基にしたプロジェクト経験あり")]),
    ("INVALID", "1a0183ac8eb1aef0", "1a017ec72a1b62f4", [
        check("Azure DevOPsを用いたCI/CDに関する基本知識", "Azureの経験はあるが、DevOpsやCI/CDの具体的な記載なし", "Azure（Virtual Machines、OpenAI Service）"),
        check("ネットワークに関する基礎知識(TCP/IP、HTTP/HTTPS)", "ネットワークスペシャリスト資格ありだが、具体的な業務経験の記載なし", "ネットワークスペシャリスト")]),
    ("INVALID", "1a017732c2b0cc1a", "1a01830b018ee2e9", [
        check("toB向け / toC向けプロダクトでのPdM経験", "PdM経験の直接的な記載なし。要件定義やチームリーダー経験はあるが、PdMとしての経験は不明確。", "要件定義からテストまでを1人称で実施")]),
    ("INVALID", "1a017732c2b0cc1a", "1a017b82b6f66fa9", [
        check("toB向け / toC向けプロダクトでのPdM経験", "PM/PL経験はあるが、PdM経験の明確な記載なし", "PM／PL／スクラムマスターとしてのプロジェクトマネジメント経験")]),
    ("INVALID", "1a017c3922f45c3f", "1a01909ad1d6137e", [
        check("CRM/LINE施策/メール施策などの経験", "LINEworkの経験はあるが、CRMやメール施策の経験は不明。", "LINEwork 1年6ヶ月")]),
    ("AMBIGUOUS", "1a01864cb9d41a70", "1a0179c5dd06e5bb", [
        check("toC向けWebサイト/モバイルアプリ開発におけるPMまたはPMO経験", "PMO経験はあるが、toC向けWebサイト/モバイルアプリ開発の明確な記載なし", "大手物流・電機メーカーのプロジェクトにおいて、PMOやPMO補佐として会議体運営、進捗・課題管理")]),
    ("VALID", "1a0184256dcf5510", "1a0190d06388b8ba", [
        check("WMS、在庫管理システム、出荷管理システムいずれかの経験", "在庫管理の経験はあるが、WMSや出荷管理システムの経験は不明", "在庫管理")]),
    ("INVALID", "1a0184256dcf5510", "1a017dc367e25463", [
        check("WMS、在庫管理システム、出荷管理システムいずれかの経験", "WMSや在庫管理システムの直接的な経験は記載されていないが、品質管理文書システムや免税販売システムの経験があるため、関連性を確認する必要がある", "品質管理文書システム、免税販売システム")]),
    ("INVALID", "1a0184256dcf5510", "1a017b0b529dceb1", [
        check("WMS、在庫管理システム、出荷管理システムいずれかの経験", "該当経験の記載なしだが、販売管理システムのリプレイス経験があるため関連性を確認する必要あり", "販売管理システムのリプレイス案件")]),
    ("AMBIGUOUS", "1a0180ef558c03fd", "1a018c07937d3fc0", [
        check("PMまたはPLとして要件定義~リリースまで一貫して担当した経験", "PM業務の記載はあるが、要件定義からリリースまで一貫して担当したかは不明", "受託事業全体のPM業務")]),
    ("INVALID", "1a018607f6bf6224", "1a01869d4f6deff9", [
        check("進捗管理/課題管理/スケジュール管理の実務のご経験", "該当経験の記載がないが、チーム開発経験があるため確認が必要", "チーム開発ではGit/GitHubを用いたブランチ運用、プルリクエスト、TeamsやBacklogでの情報共有を経験")]),
    ("INVALID", "1a017732c2b0cc1a", "1a01825e84762cbc", [
        check("toB向け / toC向けプロダクトでのPdM経験", "PdM経験の明確な記載なし。要件定義や顧客折衝の経験はあるが、PdMとしての役割が不明確。", "ビジネスサイドとの要件調整")]),
    ("AMBIGUOUS", "1a017732c2b0cc1a", "1a017d844bc1fa70", [
        check("toB向け / toC向けプロダクトでのPdM経験", "PdM経験の直接的な記載はないが、サービス企画や技術選定の経験があるため、確認が必要", "サービス企画から技術選定・設計・テスト方針の策定まで担当")]),
]


class OrExampleOverrideTest(unittest.TestCase):
    def test_explicit_or_with_direct_evidence_can_override(self):
        cases = [
            check(
                "JavaまたはKotlinでの開発経験",
                "Javaでの開発経験は明確だが、Kotlin経験は不明",
                "Javaによる業務システム開発",
            ),
            check(
                "JavaとKotlinのどちらかの開発経験",
                "Javaでの開発経験は明確だが、Kotlin経験は不明",
                "Javaによる業務システム開発",
            ),
        ]
        for fixture_check in cases:
            with self.subTest(skill=fixture_check["skill"]):
                checks = [fixture_check]
                self.assertEqual(target._apply_or_example_override(checks), 1)
                self.assertEqual(checks[0]["confidence"], "confirmed")
                self.assertTrue(checks[0]["reason"].endswith(target.EXPLICIT_OR_OVERRIDE_SUFFIX))

    def test_production_cases_match_human_labels(self):
        counts = {"VALID": 0, "INVALID": 0, "AMBIGUOUS": 0}
        for label, project_id, resource_id, fixture_checks in PRODUCTION_CASES:
            counts[label] += 1
            with self.subTest(label=label, project=project_id, resource=resource_id):
                replay_checks = deepcopy(fixture_checks)
                applied = target._apply_or_example_override(replay_checks)
                expected = 1 if label == "VALID" else 0
                self.assertEqual(applied, expected)
                expected_confidence = "confirmed" if label == "VALID" else "human_review"
                self.assertTrue(all(item["confidence"] == expected_confidence for item in replay_checks))
        self.assertEqual(counts, {"VALID": 2, "INVALID": 18, "AMBIGUOUS": 7})

    def test_slash_and_ambiguous_example_markers_never_suffice(self):
        cases = [
            ("Java/SQLでの開発経験", "Java経験はあるがSQLは不明", "Java開発"),
            ("Windows/Linux両環境の運用経験", "Windows経験はあるがLinuxは不明", "Windows運用"),
            ("toB/toCプロダクトでのPdM経験", "toB経験はあるがtoCは不明", "toB向け開発"),
            ("Hyper-Vホスト/ゲスト環境", "ホスト経験はあるがゲストは不明", "Hyper-Vホスト"),
            ("CI/CDの基本知識", "CI経験はあるがCDは不明", "CI構築"),
            ("TCP/IPの基礎知識", "TCP経験はあるがIPは不明", "TCP"),
            ("AWS等クラウド設計", "AWS経験はあるが他は不明", "AWS設計"),
            ("AWSなどクラウド設計", "AWS経験はあるが他は不明", "AWS設計"),
        ]
        for skill, reason, evidence in cases:
            with self.subTest(skill=skill):
                checks = [check(skill, reason, evidence)]
                self.assertEqual(target._apply_or_example_override(checks), 0)
                self.assertEqual(checks[0]["confidence"], "human_review")

    def test_deficiency_or_missing_evidence_blocks_override(self):
        cases = [
            check("JavaまたはKotlin経験5年以上", "Javaは3年でKotlinは不明", "Java 3年"),
            check("PMまたはPL経験", "PM経験はあるが求める役割が不明確", "PM補佐"),
            check("PMまたはPLとして一貫担当した経験", "PM補佐経験はあるが、PMまたはPLとして一貫担当したか不明", "PM補佐"),
            check("JavaまたはKotlin経験", "Java経験はあるがKotlinは不明", ""),
            check("JavaまたはKotlin経験", "Javaの根拠なし、Kotlinも不明", "C#開発"),
        ]
        for item in cases:
            with self.subTest(skill=item["skill"]):
                checks = [item]
                self.assertEqual(target._apply_or_example_override(checks), 0)
                self.assertEqual(checks[0]["confidence"], "human_review")

    def test_existing_ai_confirmed_and_unrelated_pairs_are_unchanged(self):
        checks = [
            check("Windows/Linux運用経験", "両方の経験あり", "WindowsとLinux", True, "confirmed"),
            check("Python開発経験", "Python経験が明確", "Python 5年", False, "confirmed"),
            check("COBOL経験", "根拠なし", "", False, "not_confirmed"),
        ]
        before = deepcopy(checks)
        self.assertEqual(target._apply_or_example_override(checks), 0)
        self.assertEqual(checks, before)

    def test_replay_is_idempotent_and_preserves_human_review(self):
        fixture_check = deepcopy(PRODUCTION_CASES[1][3][0])
        fixture_check["confidence"] = "confirmed"
        fixture_check["reason"] += target.LEGACY_OR_OVERRIDE_SUFFIX
        record = {
            "required_skill_checks": [fixture_check],
            "recheck_info": {
                "recheck_status": target.STATUS_CONFIRMED,
                "required_skill_count": 1,
                "confirmed_count": 1,
                "human_review_count": 0,
                "not_confirmed_count": 0,
            },
        }
        replayed, restored, applied = target._replay_postprocessing_record(record)
        self.assertEqual((restored, applied), (1, 0))
        self.assertEqual(replayed["recheck_info"]["recheck_status"], target.STATUS_HUMAN_REVIEW)
        replayed_again, restored_again, applied_again = target._replay_postprocessing_record(replayed)
        self.assertEqual((restored_again, applied_again), (0, 0))
        self.assertEqual(replayed_again, replayed)


if __name__ == "__main__":
    unittest.main()
