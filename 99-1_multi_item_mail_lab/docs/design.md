# 99-1 Variable Multi-item Core

This directory is a test-only lab. It is not imported by the production pipeline.

The lab contract is `Source -> Container -> 0..N ItemCandidate -> Completeness Gate ->
Identity / Version -> Canonical Item`. Runtime company config does not declare a fixed item
count. NetWisdom uses complete inline structure as its cardinality authority. Ichi-R reads a
declared count from the subject and cross-checks complete inline structure. Unknown or
incomplete cardinality emits no canonical items.

`INLINE_BODY` and `ATTACHMENT_FILE` containers are implemented. Other container kinds are
contract values only and have no parser in this issue. NetWisdom and Ichi-R retain their
exact filename relation as the `ONE_ARTIFACT_PER_ITEM_EXACT_KEY` strategy; it is one strategy
on top of the shared 0..N item-artifact relation model.

The source-atomic completeness gate checks acquisition, required containers, enumeration,
cardinality agreement, candidate parsing, artifact relations, and delivery-local identity
collisions. Only `PARSED` emits all candidates; every other status emits zero items.

Logical identity hashes company, item type, and the normalized block identifier. Version
identity combines the normalized item-body fingerprint with a deterministically sorted
artifact-set fingerprint. Artifact order therefore does not affect the version, while a
version-relevant artifact content change does. Audit output retains Source, Container,
cardinality, completeness, identity evidence, and every delivery occurrence. The canonical
overlay remains an ordinary mail equivalent with item-only body, 0..N attachments, and
item-specific links.
