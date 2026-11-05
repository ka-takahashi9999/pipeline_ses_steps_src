"""Declarative skill canonical registry loader and deterministic matcher."""

from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import yaml


SUPPORTED_SCHEMA_VERSION = 2
VALID_MATCH_MODES = frozenset({"boundary", "phrase", "exact"})
VALID_OWNERSHIP_MODES = frozenset({"production", "shadow", "legacy"})
VALID_MODES = frozenset({"legacy", "shadow", "registry_overlay"})
VALID_REQUIRED_SEARCH_MODES = frozenset({"legacy_substring", "registry"})
TOP_LEVEL_FIELDS = frozenset({"schema_version", "allowed_categories", "skills"})
ENTRY_REQUIRED_FIELDS = frozenset(
    {
        "canonical", "category", "aliases", "match_mode", "case_sensitive",
        "enabled", "ownership",
    }
)
ENTRY_OPTIONAL_FIELDS = frozenset({"source", "note"})


class SkillRegistryError(ValueError):
    """Raised when registry data or execution context is invalid."""


class _UniqueKeyLoader(yaml.SafeLoader):
    pass


def _construct_unique_mapping(loader: yaml.Loader, node: yaml.Node, deep: bool = False) -> dict:
    mapping: Dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in mapping:
            raise SkillRegistryError("duplicate YAML key: {!r}".format(key))
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


_UniqueKeyLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_unique_mapping,
)


def normalize_text(value: str, case_sensitive: bool = False) -> str:
    normalized = unicodedata.normalize("NFKC", value or "")
    return normalized if case_sensitive else normalized.casefold()


def normalized_surface_key(value: str) -> str:
    return normalize_text(value.strip(), case_sensitive=False)


def _validate_ownership_filter(
    ownerships: Optional[Iterable[str]],
) -> Optional[frozenset]:
    if ownerships is None:
        return None
    values = frozenset(ownerships)
    invalid = values - VALID_OWNERSHIP_MODES
    if invalid:
        raise SkillRegistryError(
            "invalid ownership mode(s): {}".format(sorted(invalid))
        )
    return values


@dataclass(frozen=True)
class RegistryEntry:
    canonical: str
    ownership: str
    category: str
    aliases: Tuple[str, ...]
    match_mode: str
    case_sensitive: bool
    enabled: bool
    source: Optional[str]
    note: Optional[str]
    declaration_index: int

    @property
    def surfaces(self) -> Tuple[str, ...]:
        return (self.canonical,) + self.aliases


@dataclass(frozen=True)
class RegistrySnapshot:
    schema_version: int
    allowed_categories: Tuple[str, ...]
    entries: Tuple[RegistryEntry, ...]
    registry_sha256: str
    raw_bytes: bytes
    source_path: str

    @property
    def canonical_names(self) -> Tuple[str, ...]:
        return tuple(entry.canonical for entry in self.entries if entry.enabled)

    def canonical_names_for_ownership(self, ownership: str) -> Tuple[str, ...]:
        if ownership not in VALID_OWNERSHIP_MODES:
            raise SkillRegistryError("invalid ownership mode: {!r}".format(ownership))
        return tuple(
            entry.canonical
            for entry in self.entries
            if entry.enabled and entry.ownership == ownership
        )

    @property
    def production_canonical_names(self) -> Tuple[str, ...]:
        return self.canonical_names_for_ownership("production")

    @property
    def shadow_canonical_names(self) -> Tuple[str, ...]:
        return self.canonical_names_for_ownership("shadow")

    @property
    def ownership_map(self) -> Dict[str, str]:
        return {entry.canonical: entry.ownership for entry in self.entries}

    def entry_for_canonical(self, canonical: str) -> Optional[RegistryEntry]:
        for entry in self.entries:
            if entry.canonical == canonical:
                return entry
        return None

    def resolve_surface(
        self,
        value: str,
        ownerships: Optional[Iterable[str]] = None,
    ) -> Optional[str]:
        raw = value or ""
        ownership_set = _validate_ownership_filter(ownerships)
        for entry in self.entries:
            if not entry.enabled or (
                ownership_set is not None and entry.ownership not in ownership_set
            ):
                continue
            candidate = normalize_text(raw, entry.case_sensitive)
            for surface in entry.surfaces:
                if candidate == normalize_text(surface, entry.case_sensitive):
                    return entry.canonical
        return None

    def aliases_for(
        self,
        value: str,
        ownerships: Optional[Iterable[str]] = None,
    ) -> Optional[Tuple[str, ...]]:
        canonical = self.resolve_surface(value, ownerships=ownerships)
        if canonical is None:
            return None
        entry = self.entry_for_canonical(canonical)
        return entry.surfaces if entry is not None else None


@dataclass(frozen=True)
class SurfaceSpec:
    surface: str
    canonical: str
    category: str
    match_mode: str
    case_sensitive: bool
    source: str
    declaration_index: int
    alias_index: int


@dataclass(frozen=True)
class SkillHit:
    canonical: str
    category: str
    matched_surface: str
    start: int
    end: int
    match_mode: str
    source: str

    def to_dict(self) -> dict:
        return {
            "canonical": self.canonical,
            "category": self.category,
            "matched_surface": self.matched_surface,
            "span": [self.start, self.end],
            "match_mode": self.match_mode,
            "source": self.source,
        }


@dataclass(frozen=True)
class OwnershipOverride:
    surface: str
    legacy_canonical: str
    legacy_category: str
    registry_canonical: str
    registry_category: str

    def to_dict(self) -> dict:
        return {
            "surface": self.surface,
            "legacy_canonical": self.legacy_canonical,
            "legacy_category": self.legacy_category,
            "registry_canonical": self.registry_canonical,
            "registry_category": self.registry_category,
        }


def _require_string(value: Any, label: str, allow_empty: bool = False) -> str:
    if not isinstance(value, str):
        raise SkillRegistryError("{} must be a string".format(label))
    if not allow_empty and not value.strip():
        raise SkillRegistryError("{} must not be empty".format(label))
    if value != value.strip():
        raise SkillRegistryError("{} must not have surrounding whitespace".format(label))
    return value


def parse_registry_bytes(raw_bytes: bytes, source_path: str = "<memory>") -> RegistrySnapshot:
    try:
        data = yaml.load(raw_bytes.decode("utf-8"), Loader=_UniqueKeyLoader)
    except (UnicodeDecodeError, yaml.YAMLError) as exc:
        raise SkillRegistryError("invalid registry YAML: {}".format(exc)) from exc
    if not isinstance(data, dict):
        raise SkillRegistryError("registry root must be a mapping")
    unknown_top = set(data) - TOP_LEVEL_FIELDS
    if unknown_top:
        raise SkillRegistryError("unknown registry fields: {}".format(sorted(unknown_top)))
    missing_top = TOP_LEVEL_FIELDS - set(data)
    if missing_top:
        raise SkillRegistryError("missing registry fields: {}".format(sorted(missing_top)))

    schema_version = data.get("schema_version")
    if type(schema_version) is not int or schema_version != SUPPORTED_SCHEMA_VERSION:
        raise SkillRegistryError("unsupported schema_version: {!r}".format(schema_version))

    categories = data.get("allowed_categories")
    if not isinstance(categories, list) or not categories:
        raise SkillRegistryError("allowed_categories must be a non-empty list")
    validated_categories: List[str] = []
    category_keys = set()
    for index, category in enumerate(categories):
        category = _require_string(category, "allowed_categories[{}]".format(index))
        key = normalized_surface_key(category)
        if key in category_keys:
            raise SkillRegistryError("duplicate allowed category: {}".format(category))
        category_keys.add(key)
        validated_categories.append(category)

    raw_entries = data.get("skills")
    if not isinstance(raw_entries, list) or not raw_entries:
        raise SkillRegistryError("skills must be a non-empty list")

    entries: List[RegistryEntry] = []
    canonical_keys: Dict[str, str] = {}
    surface_owners: Dict[str, str] = {}
    allowed_set = set(validated_categories)

    for index, raw_entry in enumerate(raw_entries):
        if not isinstance(raw_entry, dict):
            raise SkillRegistryError("skills[{}] must be a mapping".format(index))
        unknown = set(raw_entry) - ENTRY_REQUIRED_FIELDS - ENTRY_OPTIONAL_FIELDS
        missing = ENTRY_REQUIRED_FIELDS - set(raw_entry)
        if unknown:
            raise SkillRegistryError(
                "skills[{}] has unknown fields: {}".format(index, sorted(unknown))
            )
        if missing:
            raise SkillRegistryError(
                "skills[{}] is missing fields: {}".format(index, sorted(missing))
            )

        canonical = _require_string(raw_entry.get("canonical"), "canonical")
        ownership = _require_string(
            raw_entry.get("ownership"), "ownership for {}".format(canonical)
        )
        if ownership not in VALID_OWNERSHIP_MODES:
            raise SkillRegistryError(
                "invalid ownership for {}: {!r}".format(canonical, ownership)
            )
        category = _require_string(raw_entry.get("category"), "category")
        if category not in allowed_set:
            raise SkillRegistryError(
                "invalid category for {}: {}".format(canonical, category)
            )
        aliases = raw_entry.get("aliases")
        if not isinstance(aliases, list):
            raise SkillRegistryError("aliases for {} must be a list".format(canonical))
        match_mode = raw_entry.get("match_mode")
        if match_mode not in VALID_MATCH_MODES:
            raise SkillRegistryError(
                "invalid match_mode for {}: {!r}".format(canonical, match_mode)
            )
        case_sensitive = raw_entry.get("case_sensitive")
        enabled = raw_entry.get("enabled")
        if type(case_sensitive) is not bool:
            raise SkillRegistryError("case_sensitive for {} must be bool".format(canonical))
        if type(enabled) is not bool:
            raise SkillRegistryError("enabled for {} must be bool".format(canonical))
        source = raw_entry.get("source")
        note = raw_entry.get("note")
        if source is not None:
            source = _require_string(source, "source for {}".format(canonical))
        if note is not None:
            note = _require_string(note, "note for {}".format(canonical))

        canonical_key = normalized_surface_key(canonical)
        if canonical_key in canonical_keys:
            raise SkillRegistryError(
                "duplicate canonical: {} / {}".format(canonical_keys[canonical_key], canonical)
            )
        canonical_keys[canonical_key] = canonical

        validated_aliases: List[str] = []
        local_alias_keys = {canonical_key}
        for alias_index, alias in enumerate(aliases):
            alias = _require_string(alias, "alias {} for {}".format(alias_index, canonical))
            # Ownership validation is always NFKC + casefold. Matching may be
            # case-sensitive, but ambiguous ownership may not depend on case.
            alias_key = normalized_surface_key(alias)
            if alias_key in local_alias_keys:
                raise SkillRegistryError(
                    "duplicate/canonical alias for {}: {}".format(canonical, alias)
                )
            local_alias_keys.add(alias_key)
            validated_aliases.append(alias)

        for surface in (canonical,) + tuple(validated_aliases):
            key = normalized_surface_key(surface)
            owner = surface_owners.get(key)
            if owner is not None and owner != canonical:
                raise SkillRegistryError(
                    "surface belongs to multiple canonicals: {} -> {}, {}".format(
                        surface, owner, canonical
                    )
                )
            surface_owners[key] = canonical

        entries.append(
            RegistryEntry(
                canonical=canonical,
                ownership=ownership,
                category=category,
                aliases=tuple(validated_aliases),
                match_mode=match_mode,
                case_sensitive=case_sensitive,
                enabled=enabled,
                source=source,
                note=note,
                declaration_index=index,
            )
        )

    # A canonical may not be another entry's alias, including case-folded forms.
    for key, canonical in canonical_keys.items():
        owner = surface_owners.get(key)
        if owner != canonical:
            raise SkillRegistryError(
                "canonical is owned as another alias: {} -> {}".format(canonical, owner)
            )

    return RegistrySnapshot(
        schema_version=schema_version,
        allowed_categories=tuple(validated_categories),
        entries=tuple(entries),
        registry_sha256=hashlib.sha256(raw_bytes).hexdigest(),
        raw_bytes=raw_bytes,
        source_path=source_path,
    )


def load_registry(path: Path) -> RegistrySnapshot:
    path = Path(path)
    return parse_registry_bytes(path.read_bytes(), str(path.resolve()))


def implementation_identity(paths: Iterable[Path]) -> str:
    digest = hashlib.sha256()
    for path in sorted((Path(p).resolve() for p in paths), key=str):
        digest.update(str(path).encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def execution_fingerprint(
    snapshot: Optional[RegistrySnapshot],
    mode: str,
    required_search_mode: str,
    implementation_id: str,
) -> str:
    if mode not in VALID_MODES:
        raise SkillRegistryError("invalid registry mode: {}".format(mode))
    if required_search_mode not in VALID_REQUIRED_SEARCH_MODES:
        raise SkillRegistryError(
            "invalid required_search_mode: {}".format(required_search_mode)
        )
    if mode == "legacy":
        payload = {
            "mode": "legacy",
            "required_search_mode": required_search_mode,
            "implementation_identity": implementation_id,
            "registry_sha256": None,
            "schema_version": None,
        }
    else:
        if snapshot is None:
            raise SkillRegistryError("registry snapshot is required for {}".format(mode))
        payload = {
            "mode": mode,
            "required_search_mode": required_search_mode,
            "implementation_identity": implementation_id,
            "registry_sha256": snapshot.registry_sha256,
            "schema_version": snapshot.schema_version,
        }
    serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def load_registry_for_mode(mode: str, path: Path) -> Optional[RegistrySnapshot]:
    if mode == "legacy":
        return None
    if mode not in VALID_MODES:
        raise SkillRegistryError("invalid registry mode: {}".format(mode))
    return load_registry(path)


class SkillMatcher:
    def __init__(self, surfaces: Sequence[SurfaceSpec]):
        self.surfaces = tuple(
            sorted(
                surfaces,
                key=lambda item: (
                    -len(normalize_text(item.surface, item.case_sensitive)),
                    0 if item.source == "registry" else 1,
                    item.declaration_index,
                    item.alias_index,
                ),
            )
        )
        self._patterns = tuple(self._compile(item) for item in self.surfaces)

    @staticmethod
    def _compile(item: SurfaceSpec) -> Optional[re.Pattern]:
        if item.match_mode == "exact":
            return None
        escaped = re.escape(unicodedata.normalize("NFKC", item.surface))
        if item.match_mode == "boundary":
            expression = r"(?<![A-Za-z0-9\-.])" + escaped + r"(?![A-Za-z0-9\-.])"
        else:
            expression = escaped
        flags = 0 if item.case_sensitive else re.IGNORECASE
        return re.compile(expression, flags)

    @staticmethod
    def _exact_matches(text: str, surface: str, case_sensitive: bool) -> Iterable[Tuple[int, int]]:
        offset = 0
        normalized_surface = normalize_text(surface, case_sensitive)
        for line in text.splitlines(keepends=True):
            content = line.rstrip("\r\n")
            stripped = content.strip()
            if stripped and normalize_text(stripped, case_sensitive) == normalized_surface:
                leading = len(content) - len(content.lstrip())
                yield offset + leading, offset + leading + len(stripped)
            offset += len(line)

    def find_hits(self, text: str) -> List[SkillHit]:
        normalized_text = unicodedata.normalize("NFKC", text or "")
        matched_spans: List[Tuple[int, int]] = []
        raw_hits: List[SkillHit] = []

        def contained(start: int, end: int) -> bool:
            return any(old_start <= start and end <= old_end for old_start, old_end in matched_spans)

        for item, pattern in zip(self.surfaces, self._patterns):
            if pattern is None:
                matches = self._exact_matches(
                    normalized_text, item.surface, item.case_sensitive
                )
            else:
                matches = ((match.start(), match.end()) for match in pattern.finditer(normalized_text))
            for start, end in matches:
                if contained(start, end):
                    continue
                matched_spans.append((start, end))
                raw_hits.append(
                    SkillHit(
                        canonical=item.canonical,
                        category=item.category,
                        matched_surface=normalized_text[start:end],
                        start=start,
                        end=end,
                        match_mode=item.match_mode,
                        source=item.source,
                    )
                )

        raw_hits.sort(key=lambda hit: (hit.start, hit.end))
        seen = set()
        result: List[SkillHit] = []
        for hit in raw_hits:
            if hit.canonical in seen:
                continue
            seen.add(hit.canonical)
            result.append(hit)
        return result


def _legacy_match_mode(surface: str) -> str:
    if re.match(r"^[A-Za-z0-9. #+\-/]+$", surface):
        return "boundary"
    return "phrase"


def build_effective_matcher(
    snapshot: RegistrySnapshot,
    legacy_dictionary_path: Path,
    legacy_canonical_map: Mapping[str, str],
    ownerships: Iterable[str],
) -> Tuple[SkillMatcher, Tuple[OwnershipOverride, ...]]:
    legacy_data = yaml.safe_load(Path(legacy_dictionary_path).read_text(encoding="utf-8"))
    if not isinstance(legacy_data, dict):
        raise SkillRegistryError("legacy skill dictionary must be a mapping")

    ownership_set = _validate_ownership_filter(ownerships)
    if not ownership_set:
        raise SkillRegistryError("effective matcher ownerships must not be empty")
    surfaces: List[SurfaceSpec] = []
    claimed: Dict[str, RegistryEntry] = {}
    for entry in snapshot.entries:
        if not entry.enabled or entry.ownership not in ownership_set:
            continue
        for alias_index, surface in enumerate(entry.surfaces):
            key = normalized_surface_key(surface)
            claimed[key] = entry
            surfaces.append(
                SurfaceSpec(
                    surface=surface,
                    canonical=entry.canonical,
                    category=entry.category,
                    match_mode=entry.match_mode,
                    case_sensitive=entry.case_sensitive,
                    source="registry",
                    declaration_index=entry.declaration_index,
                    alias_index=alias_index,
                )
            )

    overrides: List[OwnershipOverride] = []
    legacy_index = 0
    for category, values in legacy_data.items():
        if not isinstance(values, list):
            continue
        for value in values:
            if not value:
                continue
            surface = str(value).strip()
            key = normalized_surface_key(surface)
            owner = claimed.get(key)
            legacy_canonical = legacy_canonical_map.get(surface, surface)
            if owner is not None:
                overrides.append(
                    OwnershipOverride(
                        surface=surface,
                        legacy_canonical=legacy_canonical,
                        legacy_category=str(category),
                        registry_canonical=owner.canonical,
                        registry_category=owner.category,
                    )
                )
                legacy_index += 1
                continue
            surfaces.append(
                SurfaceSpec(
                    surface=surface,
                    canonical=legacy_canonical,
                    category=str(category),
                    match_mode=_legacy_match_mode(surface),
                    case_sensitive=False,
                    source="legacy",
                    declaration_index=legacy_index,
                    alias_index=0,
                )
            )
            legacy_index += 1

    return SkillMatcher(surfaces), tuple(overrides)


def build_registry_matcher(
    snapshot: RegistrySnapshot,
    ownerships: Iterable[str],
) -> SkillMatcher:
    """Build a matcher containing only registry-owned surfaces.

    Required-text primary detection must not let a longer, unrelated legacy
    dictionary entry hide a registry alias that occurs inside that entry.
    """
    ownership_set = _validate_ownership_filter(ownerships)
    if not ownership_set:
        raise SkillRegistryError("registry matcher ownerships must not be empty")
    surfaces: List[SurfaceSpec] = []
    for entry in snapshot.entries:
        if not entry.enabled or entry.ownership not in ownership_set:
            continue
        for alias_index, surface in enumerate(entry.surfaces):
            surfaces.append(
                SurfaceSpec(
                    surface=surface,
                    canonical=entry.canonical,
                    category=entry.category,
                    match_mode=entry.match_mode,
                    case_sensitive=entry.case_sensitive,
                    source="registry",
                    declaration_index=entry.declaration_index,
                    alias_index=alias_index,
                )
            )
    return SkillMatcher(surfaces)


def hits_to_skill_output(hits: Sequence[SkillHit]) -> Tuple[List[str], Dict[str, List[str]]]:
    skills: List[str] = []
    by_category: Dict[str, List[str]] = {}
    for hit in hits:
        skills.append(hit.canonical)
        by_category.setdefault(hit.category, []).append(hit.canonical)
    return skills, by_category


def hits_to_skills_raw(text: str, hits: Sequence[SkillHit]) -> Optional[str]:
    if not hits:
        return None
    normalized_text = unicodedata.normalize("NFKC", text or "")
    first_start = max(0, min(hit.start for hit in hits) - 30)
    last_end = min(len(normalized_text), max(hit.end for hit in hits) + 50)
    value = normalized_text[first_start:last_end].strip()
    return value[:200] if value else None


def canonicalize_skill_values(
    values: Sequence[str],
    snapshot: RegistrySnapshot,
    ownerships: Iterable[str],
) -> List[str]:
    """Canonicalize only registry-owned surfaces and preserve order/dedup."""
    result: List[str] = []
    seen = set()
    for value in values or []:
        canonical = snapshot.resolve_surface(value, ownerships=ownerships) or value
        if canonical in seen:
            continue
        seen.add(canonical)
        result.append(canonical)
    return result


def shadow_primary_skills(
    body_hits: Sequence[SkillHit],
    required_texts: Sequence[str],
    matcher: SkillMatcher,
    registry_canonicals: Iterable[str],
    legacy_primary_skills: Sequence[str],
) -> Tuple[List[str], Dict[str, List[dict]]]:
    if not required_texts:
        return [hit.canonical for hit in body_hits], {}

    registry_set = set(registry_canonicals)
    required_hits: Dict[str, List[dict]] = {}
    for text in required_texts:
        for hit in matcher.find_hits(text):
            if hit.canonical in registry_set:
                required_hits.setdefault(hit.canonical, []).append(hit.to_dict())

    legacy_primary_set = set(legacy_primary_skills or [])
    primary: List[str] = []
    for hit in body_hits:
        if hit.canonical in registry_set:
            if hit.canonical in required_hits:
                primary.append(hit.canonical)
        elif hit.canonical in legacy_primary_set:
            primary.append(hit.canonical)
    return primary, required_hits
