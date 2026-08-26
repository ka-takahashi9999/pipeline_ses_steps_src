# P4 LINK_BUNDLE Variable-N contract

This is a test-only 99-1 Lab parser. It does not integrate with production.

- The saved `html_links` snapshot is scanned once in source order. No URL is fetched.
- Resource and project boundaries come from their configured headers, never from a fixed count.
- Every link is classified as header, item, action, shared, non-item, or unknown.
- Unknown links, duplicate item locators, invalid header structure, empty titles, incomplete
  snapshots, candidate parse failures, and identity collisions fail the source-atomic gate.
- A structurally complete section can contain zero items. Both headers are still required.
- Each canonical body contains only the section projection of the saved source context,
  its saved section header, and one saved link title. Each canonical `html_links` contains
  only that item's saved URL.
- Logical identity is provisionally durable and is based on section plus the stable
  `/boost/{talents|projects}/{id}` locator. Title-only changes preserve logical identity and
  create a new version; locator changes create a new logical identity.
- The version fingerprint covers the section, normalized saved title, and saved locator.
  It represents the mail-observed list-item version (`MAIL_SNAPSHOT_LIST_ITEM`), not external
  WEB_PAGE content.
- LINK_BUNDLE is `SOURCE_EVIDENCE`; each item WEB_PAGE locator is `PRIMARY` evidence.

Known production-only backlog remains unchanged: `identity_schema_version` and stale artifact
rejection. Production integration remains on hold.
