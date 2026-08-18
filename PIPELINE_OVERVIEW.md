# PIPELINE_OVERVIEW

## 01-1_fetch_gmail
- **目的**：Step 01-1: Gmail取得スクリプト
- **入力**：主入力なし
- **出力**：`fetch_gmail_mail_master.jsonl`, `01_result/fetch_gmail_mail_master.jsonl`
- **LLM使用**：無
- **備考**：JSONL前提の後続処理との整合に注意が必要です。 / `message_id` を主キーとする処理との整合が必要です。

## 01-2_remove_duplicate_emails
- **目的**：Step 01-2: メール重複除去スクリプト
- **入力**：`01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl`
- **出力**：`remove_duplicate_emails_raw.jsonl`, `99_duplicate_emails_raw.jsonl`, `01_result/remove_duplicate_emails_raw.jsonl  （重複除去後の message_id）`, `01_result/99_duplicate_emails_raw.jsonl      （除去された重複の message_id）`
- **LLM使用**：無
- **備考**：JSONL前提の後続処理との整合に注意が必要です。 / `message_id` を主キーとする処理との整合が必要です。

## 01-3_remove_individual_email
- **目的**：Step 01-3: 個別除外処理スクリプト
- **入力**：`01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl`, `01-2_remove_duplicate_emails/01_result/remove_duplicate_emails_raw.jsonl`
- **出力**：`remove_individual_emails_raw.jsonl`, `99_removed_individual_emails_raw.jsonl`, `01_result/remove_individual_emails_raw.jsonl  （除外後の message_id）`, `01_result/99_removed_individual_emails_raw.jsonl （除外された message_id）`
- **LLM使用**：無
- **備考**：JSONL前提の後続処理との整合に注意が必要です。 / `message_id` を主キーとする処理との整合が必要です。

## 01-4_cleanup_email_text
- **目的**：Step 01-4: メール本文クリーニングスクリプト
- **入力**：`01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl`, `01-3_remove_individual_email/01_result/remove_individual_emails_raw.jsonl`
- **出力**：`cleanup_email_text_emails_raw.jsonl`, `01_result/cleanup_email_text_emails_raw.jsonl`
- **LLM使用**：無
- **備考**：JSONL前提の後続処理との整合に注意が必要です。 / `message_id` を主キーとする処理との整合が必要です。

## 02-1_classify_type_project_resource
- **目的**：Step 02-1: メール種別分類スクリプト（案件 / 要員 / あいまい / 不明）
- **入力**：`01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl`, `01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl`, `01-3_remove_individual_email/01_result/remove_individual_emails_raw.jsonl`
- **出力**：`classify_types_project_resource.jsonl`, `99_no_classify_types_project_resource.jsonl`
- **LLM使用**：無
- **備考**：JSONL前提の後続処理との整合に注意が必要です。 / `message_id` を主キーとする処理との整合が必要です。

## 02-2_classify_output_file_project_resource
- **目的**：Step 02-2: 分類結果ファイル分割出力
- **入力**：`02-1_classify_type_project_resource/01_result/classify_types_project_resource.jsonl`, `02-1_classify_type_project_resource/01_result/99_no_classify_types_project_resource.jsonl`
- **出力**：`projects.jsonl`, `resources.jsonl`, `ambiguous.jsonl`, `unknown.jsonl`
- **LLM使用**：無
- **備考**：JSONL前提の後続処理との整合に注意が必要です。 / `message_id` を主キーとする処理との整合が必要です。

## 03-1_extract_project_budget
- **目的**：Step 03-1: 案件メールから単価（月額）をルールベースで抽出
- **入力**：`01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl`, `02-2_classify_output_file_project_resource/01_result/projects.jsonl`, `01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl`
- **出力**：`extract_project_budget.jsonl`, `99_price_null_extract_project_budget.jsonl`
- **LLM使用**：無
- **備考**：JSONL前提の後続処理との整合に注意が必要です。 / `message_id` を主キーとする処理との整合が必要です。

## 03-2_extract_project_age
- **目的**：Step 03-2: 案件メールから年齢制限をルールベースで抽出
- **入力**：`01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl`, `02-2_classify_output_file_project_resource/01_result/projects.jsonl`
- **出力**：`extract_project_age.jsonl`, `99_age_null_extract_project_age.jsonl`
- **LLM使用**：無
- **備考**：JSONL前提の後続処理との整合に注意が必要です。 / `message_id` を主キーとする処理との整合が必要です。

## 03-3_extract_project_remote
- **目的**：Step 03-3: 案件メールからリモート勤務条件をルールベースで抽出
- **入力**：`01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl`, `02-2_classify_output_file_project_resource/01_result/projects.jsonl`
- **出力**：`extract_project_remote.jsonl`, `99_remote_null_extract_project_remote.jsonl`
- **LLM使用**：無
- **備考**：JSONL前提の後続処理との整合に注意が必要です。 / `message_id` を主キーとする処理との整合が必要です。

## 03-4_extract_project_foreign
- **目的**：Step 03-4: 案件メールから外国籍制限をルールベースで抽出
- **入力**：`01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl`, `02-2_classify_output_file_project_resource/01_result/projects.jsonl`
- **出力**：`extract_project_foreign.jsonl`, `99_foreign_null_extract_project_foreign.jsonl`
- **LLM使用**：無
- **備考**：JSONL前提の後続処理との整合に注意が必要です。 / `message_id` を主キーとする処理との整合が必要です。

## 03-5_extract_project_freelance
- **目的**：Step 03-5: 案件メールから個人事業主制限をルールベースで抽出
- **入力**：`01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl`, `02-2_classify_output_file_project_resource/01_result/projects.jsonl`
- **出力**：`extract_project_freelance.jsonl`, `99_freelance_null_extract_project_freelance.jsonl`
- **LLM使用**：無
- **備考**：JSONL前提の後続処理との整合に注意が必要です。 / `message_id` を主キーとする処理との整合が必要です。

## 03-6_extract_project_workload
- **目的**：Step 03-6: 案件メールから稼働率をルールベースで抽出
- **入力**：`01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl`, `02-2_classify_output_file_project_resource/01_result/projects.jsonl`
- **出力**：`extract_project_workload.jsonl`, `99_workload_null_extract_project_workload.jsonl`
- **LLM使用**：無
- **備考**：JSONL前提の後続処理との整合に注意が必要です。 / `message_id` を主キーとする処理との整合が必要です。

## 03-7_extract_project_vendor_tiers
- **目的**：Step 03-7: 案件メールから商流制限をルールベースで抽出
- **入力**：`01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl`, `02-2_classify_output_file_project_resource/01_result/projects.jsonl`
- **出力**：`extract_project_vendor_tiers.jsonl`, `99_vendor_tiers_null_extract_project_vendor_tiers.jsonl`
- **LLM使用**：無
- **備考**：JSONL前提の後続処理との整合に注意が必要です。 / `message_id` を主キーとする処理との整合が必要です。

## 03-8_extract_project_skill_category
- **目的**：Step 03-8: 案件メールからスキル・カテゴリをルールベースで抽出
- **入力**：`01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl`, `02-2_classify_output_file_project_resource/01_result/projects.jsonl`, `03-50_extract_project_required_skills/01_result/extract_project_required_skills.jsonl`
- **出力**：`extract_project_skill_category.jsonl`, `99_skill_category_null_extract_project_skill_category.jsonl`
- **LLM使用**：無
- **備考**：JSONL前提の後続処理との整合に注意が必要です。 / `message_id` を主キーとする処理との整合が必要です。

## 03-9_extract_project_phase_category
- **目的**：Step 03-9: 案件メールから工程をルールベースで抽出
- **入力**：`01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl`, `02-2_classify_output_file_project_resource/01_result/projects.jsonl`
- **出力**：`extract_project_phase_category.jsonl`, `99_phase_null_extract_project_phase_category.jsonl`
- **LLM使用**：無
- **備考**：JSONL前提の後続処理との整合に注意が必要です。 / `message_id` を主キーとする処理との整合が必要です。

## 03-10_extract_project_location
- **目的**：Step 03-10: 案件メールから作業場所（ロケーション）をルールベースで抽出し地方に分類する
- **入力**：`01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl`, `02-2_classify_output_file_project_resource/01_result/projects.jsonl`, `03-10_extract_project_location/00_tool/location_dictionary.txt`
- **出力**：`extract_project_location.jsonl`, `99_location_null_extract_project_location.jsonl`, `01_result/extract_project_location.jsonl`, `01_result/99_location_null_extract_project_location.jsonl`
- **LLM使用**：無
- **備考**：JSONL前提の後続処理との整合に注意が必要です。 / `message_id` を主キーとする処理との整合が必要です。

## 03-30_extract_project_contract_type
- **目的**：Step 03-30: 案件メール本文から契約形態をルールベースで抽出する
- **入力**：`01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl`, `02-2_classify_output_file_project_resource/01_result/projects.jsonl`
- **出力**：`contract_type.jsonl`
- **LLM使用**：無
- **備考**：`dispatch` / `quasi_mandate` / `outsourcing` の3値で出力します。 / 根拠なしの既定値採用時は `contract_type_source=default` と `contract_type_raw=null` をセットで保持します。 / JSONL前提の後続処理との整合に注意が必要です。 / `message_id` を主キーとする処理との整合が必要です。

## 03-50_extract_project_required_skills
- **目的**：Step 03-50: 案件メールから必須スキル・尚可スキルをルールベースで抽出 （LLMはフォールバック限定）
- **入力**：`01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl`, `02-2_classify_output_file_project_resource/01_result/projects.jsonl`
- **出力**：`extract_project_required_skills.jsonl`, `99_skill_null_extract_project_required_skills.jsonl`, `99_rule_empty_extract_project_required_skills.jsonl`, `01_result/extract_project_required_skills.jsonl`
- **LLM使用**：無
- **備考**：JSONL前提の後続処理との整合に注意が必要です。 / `message_id` を主キーとする処理との整合が必要です。

## 03-51_extract_project_required_skills_list
- **目的**：Step 03-51: 案件の必須/尚可スキル文から辞書ベースでスキル語・工程語を抽出する LLM使用禁止。辞書ベース・文字列処理のみ。
- **入力**：`03-50_extract_project_required_skills/01_result/extract_project_required_skills.jsonl`, `03-8_extract_project_skill_category/00_tool/skill_dictionary.txt`, `03-9_extract_project_phase_category/00_tool/phase_dictionary.txt`, `01_result/extract_project_required_skills_list.jsonl`
- **出力**：`extract_project_required_skills_list.jsonl`, `99_required_skills_list_null.jsonl`
- **LLM使用**：無
- **備考**：JSONL前提の後続処理との整合に注意が必要です。 / `message_id` を主キーとする処理との整合が必要です。

## 04-1_fetch_skillsheets_text
- **目的**：04-1_fetch_skillsheets_text: 要員メールからスキルシートテキストを取得
- **入力**：`01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl`, `01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl`, `02-2_classify_output_file_project_resource/01_result/resources.jsonl`, `- 01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl  (添付ファイル参照)`
- **出力**：`04-1_fetch_skillsheets_text/01_result`, `04-1_fetch_skillsheets_text/01_result/99_no_fetch_skillsheets_text.jsonl`, `04-1_fetch_skillsheets_text/01_result/fetch_skillsheets_text.jsonl`, `- 04-1_fetch_skillsheets_text/01_result/fetch_skillsheets_text.jsonl`
- **LLM使用**：無
- **備考**：JSONL前提の後続処理との整合に注意が必要です。 / `message_id` を主キーとする処理との整合が必要です。

## 05-1_extract_resource_budget
- **目的**：Step 05-1: 要員メールから希望単価（月額）をルールベースで抽出
- **入力**：`01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl`, `02-2_classify_output_file_project_resource/01_result/resources.jsonl`, `01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl`
- **出力**：`extract_resource_budget.jsonl`, `99_price_null_extract_resource_budget.jsonl`
- **LLM使用**：無
- **備考**：JSONL前提の後続処理との整合に注意が必要です。 / `message_id` を主キーとする処理との整合が必要です。

## 05-2_extract_resource_age
- **目的**：Step 05-2: 要員メールから現在年齢をルールベースで抽出
- **入力**：`01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl`, `02-2_classify_output_file_project_resource/01_result/resources.jsonl`
- **出力**：`extract_resource_age.jsonl`, `99_age_null_extract_resource_age.jsonl`
- **LLM使用**：無
- **備考**：JSONL前提の後続処理との整合に注意が必要です。 / `message_id` を主キーとする処理との整合が必要です。

## 05-3_extract_resource_remote
- **目的**：Step 05-3: 要員メールからリモート希望をルールベースで抽出
- **入力**：`01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl`, `02-2_classify_output_file_project_resource/01_result/resources.jsonl`
- **出力**：`extract_resource_remote.jsonl`, `99_remote_null_extract_resource_remote.jsonl`
- **LLM使用**：無
- **備考**：JSONL前提の後続処理との整合に注意が必要です。 / `message_id` を主キーとする処理との整合が必要です。

## 05-4_extract_resource_foreign
- **目的**：Step 05-4: 要員メールから国籍をルールベースで抽出
- **入力**：`01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl`, `02-2_classify_output_file_project_resource/01_result/resources.jsonl`
- **出力**：`extract_resource_foreign.jsonl`, `99_foreign_null_extract_resource_foreign.jsonl`
- **LLM使用**：無
- **備考**：JSONL前提の後続処理との整合に注意が必要です。 / `message_id` を主キーとする処理との整合が必要です。

## 05-5_extract_resource_freelance
- **目的**：Step 05-5: 要員メールから雇用形態をルールベースで抽出
- **入力**：`01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl`, `02-2_classify_output_file_project_resource/01_result/resources.jsonl`
- **出力**：`extract_resource_freelance.jsonl`, `99_freelance_null_extract_resource_freelance.jsonl`
- **LLM使用**：無
- **備考**：JSONL前提の後続処理との整合に注意が必要です。 / `message_id` を主キーとする処理との整合が必要です。

## 05-6_extract_resource_workload
- **目的**：Step 05-6: 要員メールから稼働率をルールベースで抽出
- **入力**：`01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl`, `02-2_classify_output_file_project_resource/01_result/resources.jsonl`
- **出力**：`extract_resource_workload.jsonl`, `99_workload_null_extract_resource_workload.jsonl`
- **LLM使用**：無
- **備考**：JSONL前提の後続処理との整合に注意が必要です。 / `message_id` を主キーとする処理との整合が必要です。

## 05-7_extract_resource_vendor_tiers
- **目的**：Step 05-7: 要員メールから商流情報（vendor_flow）をルールベースで抽出
- **入力**：`01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl`, `01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl`, `02-2_classify_output_file_project_resource/01_result/resources.jsonl`
- **出力**：`extract_resource_vendor_tiers.jsonl`, `99_vendor_tiers_null_extract_resource_vendor_tiers.jsonl`
- **LLM使用**：無
- **備考**：JSONL前提の後続処理との整合に注意が必要です。 / `message_id` を主キーとする処理との整合が必要です。

## 05-8_extract_resource_skill_category
- **目的**：Step 05-8: 要員メールからスキル・カテゴリをルールベースで抽出
- **入力**：`01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl`, `02-2_classify_output_file_project_resource/01_result/resources.jsonl`
- **出力**：`extract_resource_skill_category.jsonl`, `99_skill_category_null_extract_resource_skill_category.jsonl`
- **LLM使用**：無
- **備考**：JSONL前提の後続処理との整合に注意が必要です。 / `message_id` を主キーとする処理との整合が必要です。

## 05-9_extract_resource_phase_category
- **目的**：Step 05-9: 要員メールから工程をルールベースで抽出
- **入力**：`01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl`, `02-2_classify_output_file_project_resource/01_result/resources.jsonl`
- **出力**：`extract_resource_phase_category.jsonl`, `99_phase_null_extract_resource_phase_category.jsonl`
- **LLM使用**：無
- **備考**：JSONL前提の後続処理との整合に注意が必要です。 / `message_id` を主キーとする処理との整合が必要です。

## 05-10_extract_resource_location
- **目的**：Step 05-10: 要員メールから居住地/最寄駅（ロケーション）をルールベースで抽出し地方に分類する
- **入力**：`02-2_classify_output_file_project_resource/01_result/resources.jsonl`, `01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl`, `05-10_extract_resource_location/00_tool/location_dictionary.txt`
- **出力**：`extract_resource_location.jsonl`, `99_location_null_extract_resource_location.jsonl`, `01_result/extract_resource_location.jsonl`, `01_result/99_location_null_extract_resource_location.jsonl`
- **LLM使用**：無
- **備考**：JSONL前提の後続処理との整合に注意が必要です。 / `message_id` を主キーとする処理との整合が必要です。

## 06-0_match_all_message_id
- **目的**：Step 06-0: 全案件×全要員の総当たりペアを生成（06系の起点）
- **入力**：`02-2_classify_output_file_project_resource/01_result/projects.jsonl`, `02-2_classify_output_file_project_resource/01_result/resources.jsonl`
- **出力**：`matched_pairs_all.jsonl`
- **LLM使用**：無
- **備考**：JSONL前提の後続処理との整合に注意が必要です。 / `message_id` を主キーとする処理との整合が必要です。

## 06-1_match_budget
- **目的**：06-1_match_budget 案件の単価と要員の希望単価を比較してマッチ判定する。
- **入力**：`06-0_match_all_message_id/01_result/matched_pairs_all.jsonl`, `03-1_extract_project_budget/01_result/extract_project_budget.jsonl`, `05-1_extract_resource_budget/01_result/extract_resource_budget.jsonl`
- **出力**：`06-1_match_budget/01_result/matched_pairs_budget.jsonl`, `06-1_match_budget/01_result/99_no_matched_pairs_budget.jsonl`
- **LLM使用**：無
- **備考**：JSONL前提の後続処理との整合に注意が必要です。 / `message_id` を主キーとする処理との整合が必要です。

## 06-2_match_age
- **目的**：06-2_match_age 案件の年齢制限と要員の年齢を比較してマッチ判定する。
- **入力**：`06-1_match_budget/01_result/matched_pairs_budget.jsonl`, `03-2_extract_project_age/01_result/extract_project_age.jsonl`, `05-2_extract_resource_age/01_result/extract_resource_age.jsonl`
- **出力**：`06-2_match_age/01_result/matched_pairs_age.jsonl`, `06-2_match_age/01_result/99_no_matched_pairs_age.jsonl`
- **LLM使用**：無
- **備考**：JSONL前提の後続処理との整合に注意が必要です。 / `message_id` を主キーとする処理との整合が必要です。

## 06-3_match_remote
- **目的**：06-3_match_remote 案件のリモート条件と要員のリモート希望を比較してマッチ判定する。
- **入力**：`06-2_match_age/01_result/matched_pairs_age.jsonl`, `03-3_extract_project_remote/01_result/extract_project_remote.jsonl`, `05-3_extract_resource_remote/01_result/extract_resource_remote.jsonl`
- **出力**：`06-3_match_remote/01_result/matched_pairs_remote.jsonl`, `06-3_match_remote/01_result/99_no_matched_pairs_remote.jsonl`
- **LLM使用**：無
- **備考**：JSONL前提の後続処理との整合に注意が必要です。 / `message_id` を主キーとする処理との整合が必要です。

## 06-4_match_foreign
- **目的**：06-4_match_foreign 案件の外国籍制限と要員の国籍を比較してマッチ判定する。
- **入力**：`06-3_match_remote/01_result/matched_pairs_remote.jsonl`, `03-4_extract_project_foreign/01_result/extract_project_foreign.jsonl`, `05-4_extract_resource_foreign/01_result/extract_resource_foreign.jsonl`
- **出力**：`06-4_match_foreign/01_result/matched_pairs_foreign.jsonl`, `06-4_match_foreign/01_result/99_no_matched_pairs_foreign.jsonl`
- **LLM使用**：無
- **備考**：JSONL前提の後続処理との整合に注意が必要です。 / `message_id` を主キーとする処理との整合が必要です。

## 06-5_match_freelance
- **目的**：06-5_match_freelance 案件の個人事業主制限と要員の雇用形態を比較してマッチ判定する。
- **入力**：`06-4_match_foreign/01_result/matched_pairs_foreign.jsonl`, `03-5_extract_project_freelance/01_result/extract_project_freelance.jsonl`, `05-5_extract_resource_freelance/01_result/extract_resource_freelance.jsonl`
- **出力**：`06-5_match_freelance/01_result/matched_pairs_freelance.jsonl`, `06-5_match_freelance/01_result/99_no_matched_pairs_freelance.jsonl`
- **LLM使用**：無
- **備考**：JSONL前提の後続処理との整合に注意が必要です。 / `message_id` を主キーとする処理との整合が必要です。

## 06-6_match_workload
- **目的**：06-6_match_workload 案件の稼働率制限と要員の稼働率を比較してマッチ判定する。
- **入力**：`06-5_match_freelance/01_result/matched_pairs_freelance.jsonl`, `03-6_extract_project_workload/01_result/extract_project_workload.jsonl`, `05-6_extract_resource_workload/01_result/extract_resource_workload.jsonl`
- **出力**：`06-6_match_workload/01_result/matched_pairs_workload.jsonl`, `06-6_match_workload/01_result/99_no_matched_pairs_workload.jsonl`
- **LLM使用**：無
- **備考**：JSONL前提の後続処理との整合に注意が必要です。 / `message_id` を主キーとする処理との整合が必要です。

## 06-7_match_vendor_tiers
- **目的**：06-7_match_vendor_tiers 案件の商流制限と要員の商流を比較してマッチ判定する。
- **入力**：`06-6_match_workload/01_result/matched_pairs_workload.jsonl`, `03-7_extract_project_vendor_tiers/01_result/extract_project_vendor_tiers.jsonl`, `05-7_extract_resource_vendor_tiers/01_result/extract_resource_vendor_tiers.jsonl`
- **出力**：`06-7_match_vendor_tiers/01_result/matched_pairs_vendor_tiers.jsonl`, `06-7_match_vendor_tiers/01_result/99_no_matched_pairs_vendor_tiers.jsonl`
- **LLM使用**：無
- **備考**：JSONL前提の後続処理との整合に注意が必要です。 / `message_id` を主キーとする処理との整合が必要です。

## 06-8_match_skill_category
- **目的**：06-8_match_skill_category 案件と要員のスキルカテゴリを比較してマッチ判定する。
- **入力**：`06-7_match_vendor_tiers/01_result/matched_pairs_vendor_tiers.jsonl`, `03-8_extract_project_skill_category/01_result/extract_project_skill_category.jsonl`, `05-8_extract_resource_skill_category/01_result/extract_resource_skill_category.jsonl`
- **出力**：`06-8_match_skill_category/01_result/matched_pairs_skill_category.jsonl`, `06-8_match_skill_category/01_result/99_no_matched_pairs_skill_category.jsonl`
- **LLM使用**：無
- **備考**：JSONL前提の後続処理との整合に注意が必要です。 / `message_id` を主キーとする処理との整合が必要です。

## 06-9_match_phase_category
- **目的**：06-9_match_phase_category 案件と要員の工程（phases）を比較してマッチ判定する。
- **入力**：`06-8_match_skill_category/01_result/matched_pairs_skill_category.jsonl`, `03-9_extract_project_phase_category/01_result/extract_project_phase_category.jsonl`, `05-9_extract_resource_phase_category/01_result/extract_resource_phase_category.jsonl`
- **出力**：`06-9_match_phase_category/01_result/matched_pairs_phase_category.jsonl`, `06-9_match_phase_category/01_result/99_no_matched_pairs_phase_category.jsonl`
- **LLM使用**：無
- **備考**：JSONL前提の後続処理との整合に注意が必要です。 / `message_id` を主キーとする処理との整合が必要です。

## 06-10_match_location
- **目的**：06-10_match_location 案件と要員のlocation（勤務地）を比較してマッチ判定する。
- **入力**：`06-9_match_phase_category/01_result/matched_pairs_phase_category.jsonl`, `03-3_extract_project_remote/01_result/extract_project_remote.jsonl`, `03-10_extract_project_location/01_result/extract_project_location.jsonl`, `05-10_extract_resource_location/01_result/extract_resource_location.jsonl`
- **出力**：`06-10_match_location/01_result/matched_pairs_location.jsonl`, `06-10_match_location/01_result/99_not_matched_pairs_location.jsonl`
- **LLM使用**：無
- **備考**：JSONL前提の後続処理との整合に注意が必要です。 / `message_id` を主キーとする処理との整合が必要です。

## 06-11_match_required_skills_list
- **目的**：06-11_match_required_skills_list 03-51 で抽出した required_skill_keywords を主軸とし、
- **入力**：`06-10_match_location/01_result/matched_pairs_location.jsonl`, `03-51_extract_project_required_skills_list/01_result/extract_project_required_skills_list.jsonl`, `04-1_fetch_skillsheets_text/01_result/fetch_skillsheets_text.jsonl`, `01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl`
- **出力**：`06-11_match_required_skills_list/01_result/matched_pairs_required_skills_list.jsonl`, `06-11_match_required_skills_list/01_result/99_not_matched_pairs_required_skills_list.jsonl`
- **LLM使用**：無
- **備考**：JSONL前提の後続処理との整合に注意が必要です。 / `message_id` を主キーとする処理との整合が必要です。

## 06-12_filter_required_skills_noise
- **目的**：06-12_filter_required_skills_noise 06-11 通過ペアに対して、広く一致しやすい語・短語・文脈依存語を追加で除外する。
- **入力**：`06-11_match_required_skills_list/01_result/matched_pairs_required_skills_list.jsonl`, `03-51_extract_project_required_skills_list/01_result/extract_project_required_skills_list.jsonl`, `04-1_fetch_skillsheets_text/01_result/fetch_skillsheets_text.jsonl`, `01-4_cleanup_email_text/01_result/cleanup_email_text_emails_raw.jsonl`
- **出力**：`06-12_filter_required_skills_noise/01_result/matched_pairs_required_skills_noise_filtered.jsonl`, `06-12_filter_required_skills_noise/01_result/99_not_matched_pairs_required_skills_noise_filtered.jsonl`
- **LLM使用**：無
- **備考**：補助モジュールのため、単体では外部入出力を持たない場合があります。 / JSONL前提の後続処理との整合に注意が必要です。 / `message_id` を主キーとする処理との整合が必要です。

## 06-80_duplicate_proposal_check
- **目的**：06-80_duplicate_proposal_check 06-30 通過ペアの comparison_key を Success Cache と照合し、Cache HIT（既評価済み・07-1へ送らない）/ Cache MISS（新規・07-1対象）に仕分けする。identity は comparison_key（project_from / project_subject / resource_from / resource_subject）であり `message_id` ではない。
- **入力**：`06-30_match_contract_type/01_result/matched_pairs_contract_type.jsonl`, `01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl`, `08-1_restore_and_merge_requirement_skill_ai_matching/01_result/success_cache_requirement_skill_ai_matching.jsonl`（判定の正本 / read-only）
- **出力**：`06-80_duplicate_proposal_check/01_result/duplicate_proposal_check.jsonl`, `06-80_duplicate_proposal_check/01_result/99_duplicate_duplicate_proposal_check.jsonl`, `06-80_duplicate_proposal_check/01_result/duplicate_proposal_check_diff_file.jsonl`, `06-80_duplicate_proposal_check/01_result/bk_duplicate_proposal_check_diff_file.jsonl`
- **LLM使用**：無
- **備考**：JSONL前提の後続処理との整合に注意が必要です。 / comparison_key の空値・cacheのschema不整合・重複キーは Cache MISS へ逃がさず fail-fast します。 / `bk_duplicate_proposal_check_diff_file.jsonl` は監査用途のみで判定には使いません。

## 07-1_requirement_skill_ai_matching
- **目的**：07-1_requirement_skill_ai_matching 06-80 の Cache MISS ペアに対し、案件の required_skills / optional_skills を
- **入力**：`06-80_duplicate_proposal_check/01_result/duplicate_proposal_check.jsonl`, `03-50_extract_project_required_skills/01_result/extract_project_required_skills.jsonl`, `04-1_fetch_skillsheets_text/01_result/fetch_skillsheets_text.jsonl`
- **出力**：`07-1_requirement_skill_ai_matching/01_result/requirement_skill_ai_matching.jsonl`, `07-1_requirement_skill_ai_matching/01_result/99_error_requirement_skill_ai_matching.jsonl`, `07-1_requirement_skill_ai_matching/01_result/run_metadata.json`
- **LLM使用**：無
- **備考**：JSONL前提の後続処理との整合に注意が必要です。 / `message_id` を主キーとする処理との整合が必要です。

## 08-1_restore_and_merge_requirement_skill_ai_matching
- **目的**：08-1_restore_and_merge_requirement_skill_ai_matching Cache HIT ペアを Success Cache の評価結果から復元して current run の `message_id` へ rebind し、07-1 の新規成功結果とマージして全件完成版を作る。その後、今回の 07-1 正常結果だけを comparison_key 単位で Success Cache へ upsert する（error は登録しない）。
- **入力**：`06-80_duplicate_proposal_check/01_result/duplicate_proposal_check.jsonl`, `06-80_duplicate_proposal_check/01_result/99_duplicate_duplicate_proposal_check.jsonl`, `06-80_duplicate_proposal_check/01_result/duplicate_proposal_check_diff_file.jsonl`, `07-1_requirement_skill_ai_matching/01_result/requirement_skill_ai_matching.jsonl`, `08-1_restore_and_merge_requirement_skill_ai_matching/01_result/success_cache_requirement_skill_ai_matching.jsonl`
- **出力**：`08-1_restore_and_merge_requirement_skill_ai_matching/01_result/restored_requirement_skill_ai_matching.jsonl`, `08-1_restore_and_merge_requirement_skill_ai_matching/01_result/merged_requirement_skill_ai_matching.jsonl`, `08-1_restore_and_merge_requirement_skill_ai_matching/01_result/99_error_restore_requirement_skill_ai_matching.jsonl`, `08-1_restore_and_merge_requirement_skill_ai_matching/01_result/success_cache_requirement_skill_ai_matching.jsonl`（upsert）
- **LLM使用**：無
- **備考**：JSONL前提の後続処理との整合に注意が必要です。 / identity は comparison_key（4フィールド）で、`message_id` は identity ではなく cache の `source_message_ids` として保持します。 / 07-1 error の comparison_key は cache へ登録されないため、次回 run では Cache MISS として再評価されます。

## 08-2_match_score_aggregation
- **目的**：08-2_match_score_aggregation 必須スキル一致率・尚可スキル一致率・合計スコアを算出する。
- **入力**：`08-1_restore_and_merge_requirement_skill_ai_matching/01_result/merged_requirement_skill_ai_matching.jsonl`
- **出力**：`08-2_match_score_aggregation/01_result/match_score_aggregation.jsonl`
- **LLM使用**：無
- **備考**：JSONL前提の後続処理との整合に注意が必要です。

## 08-3_match_score_partition
- **目的**：08-3_match_score_partition 必須スキル一致率でファイルを7分割する。
- **入力**：`08-2_match_score_aggregation/01_result/match_score_aggregation.jsonl`
- **出力**：`08-3_match_score_partition/01_result`
- **LLM使用**：無
- **備考**：JSONL前提の後続処理との整合に注意が必要です。

## 08-4_match_score_sort
- **目的**：08-4_match_score_sort 各パーティションファイルをtotal_skills_match_rateの降順でソートする。
- **入力**：`08-3_match_score_partition/01_result`
- **出力**：`08-4_match_score_sort/01_result`
- **LLM使用**：無
- **備考**：実装根拠はこのPythonファイル本体を優先してください。

## 09-1_mail_display_format
- **目的**：09-1_mail_display_format マッチペアを人間可読形式で1ペア1ファイル出力し、S3に保存する。
- **入力**：`08-4_match_score_sort/01_result`, `01-1_fetch_gmail/01_result/fetch_gmail_mail_master.jsonl`
- **出力**：`09-1_mail_display_format/01_result/mail_display_format_YYYYMMDD/`, `09-1_mail_display_format/01_result/mail_display_format_YYYYMMDD.zip`
- **LLM使用**：無
- **備考**：JSONL前提の後続処理との整合に注意が必要です。 / `message_id` を主キーとする処理との整合が必要です。 / 生成ZIPをS3へ保存します。

## （削除済み）bk_08-1_duplicate_proposal_check
旧 restore backup 方式（前回完成版 `bk_merged_*` と `99_reference/` 配下の成果物コピーを読む方式）の step。
現在は存在せず、その役割は **06-80（Success Cache 照合）+ 08-1（restore / merge / upsert）** が担う。
当時の設計資料は `99_reference/archive/` を参照（historical。現行仕様として引用しない）。
