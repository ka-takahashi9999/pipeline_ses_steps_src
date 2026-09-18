"""価格欄にある固定円/日のみを追加するFocused Test。"""
import unittest

import extract_project_budget as extractor


class FixedDailyYenTest(unittest.TestCase):
    def test_fixed_daily_and_existing_conversion(self):
        for text in ("単価：13,000円／日", "■単価：13,000円／日", "【単価】13000円/日",
                     "単金：１３，０００円／日"):
            with self.subTest(text=text):
                rec = extractor.build_record("test", text)
                self.assertEqual(rec["unit_price"], 13000 * extractor.DAILY_TO_MONTHLY)
                self.assertEqual(rec["unit_price"], 260000)
                self.assertEqual(rec["unit_price_sub_infor"]["reason"], "daily-yen-slash")
                self.assertEqual(rec["unit_price_sub_infor"]["kind"], "monthly")
                self.assertIsNone(rec["unit_price_sub_infor"]["range"])

    def test_existing_monthly_hourly_daily(self):
        for text, price in (("単価：80万円", 800000), ("単価：80～90万円", 850000),
                            ("単価：時給3000円", 480000), ("単価：2000円/h", 320000),
                            ("単価：日給13000円", 260000), ("単価：日給2万円", 400000),
                            ("単価：2万円/日", 400000), ("単価：80万円/月", 800000)):
            with self.subTest(text=text):
                self.assertEqual(extractor.rule_extract(text)[0], price)

    def test_daily_requires_price_field(self):
        for text in ("13,000円／日", "交通費：13,000円／日", "サービス案内：13,000円／日",
                     "【単化】13,000円／日", "", "単価記載なし"):
            with self.subTest(text=text):
                self.assertIsNone(extractor.rule_extract(text)[0])

    def test_no_representative_price_for_ranges_or_tables(self):
        for text in ("単価：13,000円／日、16,000円／日", "単価：13,000～16,000円／日",
                     "単価：\n初日13,000円／日\n2日目16,000円／日",
                     "単価：\n13,000円／日\n16,000円／日",
                     "単価：13,000円／日以上", "単価：13,000円／日程度"):
            with self.subTest(text=text):
                self.assertIsNone(extractor.rule_extract(text)[0])


if __name__ == "__main__":
    unittest.main()
