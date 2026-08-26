# 99-1 P1 Inline Multi-resource Adapter

This directory is a test-only lab. It is not imported by the production pipeline.

The shared `inline_summary` adapter reads company-specific structure from JSON config.
For NetWisdom, two `技術者:` anchors and long separators delimit two resource blocks.
Each anchor identifier must match exactly one normalized attachment filename. Any mail-level
count mismatch becomes `PARTIAL`; ambiguous attachment mapping becomes `HUMAN_REVIEW`.
Only `PARSED` items are emitted to the canonical overlay.

Logical identity hashes company, item type, and the normalized block identifier. Content
identity hashes the normalized item body. The derived ID hashes both, so an unchanged resend
is deduplicated while a content change creates another deterministic version. Audit output
retains every delivery occurrence; the mail-master-compatible overlay retains one record per
derived version.
