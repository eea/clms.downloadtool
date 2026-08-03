"""Extract frontend BYOC metadata from a Copernicus Browser checkout."""

import ast
from copy import deepcopy
import json
from pathlib import Path
import re
import subprocess


HANDLER = Path(
    "src/Tools/SearchPanel/dataSourceHandlers/CLMSDataSourceHandler.jsx"
)
CONFIGURATION = Path("src/assets/cache/configuration.json")
UUID = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
    r"[0-9a-f]{4}-[0-9a-f]{12}$",
    re.I,
)
CONSTANT = re.compile(
    r"(?:^|\n)\s*(?:export\s+)?const\s+"
    r"(?P<name>[A-Za-z_$][\w$]*)\s*=",
    re.M,
)
MEMBER = re.compile(
    r"^(?P<name>[A-Za-z_$][\w$]*)"
    r"(?P<parts>(?:\.[A-Za-z_$][\w$]*)*)$"
)
UNKNOWN = object()


class BYOCExtractionError(RuntimeError):
    """The source or deployment mapping cannot be read safely."""


def normalize_collection_id(value):
    """Validate and normalize a BYOC collection UUID."""
    value = str(value or "").strip().lower()
    if value.startswith("byoc-"):
        value = value[5:]
    if not UUID.fullmatch(value):
        raise BYOCExtractionError("Invalid BYOC collection ID: " + value)
    return value


def _code_characters(text):
    """Yield source characters outside JavaScript strings and comments."""
    depth = 0
    quote = None
    escaped = False
    line_comment = False
    block_comment = False
    index = 0
    while index < len(text):
        char = text[index]
        following = text[index + 1] if index + 1 < len(text) else ""
        if line_comment:
            line_comment = char != "\n"
            index += 1
            continue
        if block_comment:
            if char == "*" and following == "/":
                block_comment = False
                index += 2
            else:
                index += 1
            continue
        if quote:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote:
                quote = None
            index += 1
            continue
        if char == "/" and following == "/":
            line_comment = True
            index += 2
            continue
        if char == "/" and following == "*":
            block_comment = True
            index += 2
            continue
        if char in "'\"`":
            quote = char
            index += 1
            continue
        if char in "([{":
            yield index, char, depth
            depth += 1
        elif char in ")]}" :
            depth -= 1
            yield index, char, depth
        else:
            yield index, char, depth
        index += 1


def _read_expression(text, start, terminators=(";",)):
    fragment = text[start:]
    for index, char, depth in _code_characters(fragment):
        if depth == 0 and char in terminators:
            return fragment[:index].strip()
    return fragment.strip()


def _split_top_level(text, delimiter=","):
    parts = []
    start = 0
    for index, char, depth in _code_characters(text):
        if depth == 0 and char == delimiter:
            parts.append(text[start:index].strip())
            start = index + 1
    parts.append(text[start:].strip())
    return [part for part in parts if part]


def _split_property(text):
    for index, char, depth in _code_characters(text):
        if depth == 0 and char == ":":
            return text[:index].strip(), text[index + 1:].strip()
    return None, None


def _is_wrapped(text, opener, closer):
    if not text.startswith(opener) or not text.endswith(closer):
        return False
    depth = 0
    for index, char, _current_depth in _code_characters(text):
        if char == opener:
            depth += 1
        elif char == closer:
            depth -= 1
            if depth == 0 and index != len(text) - 1:
                return False
    return depth == 0


def _without_leading_comments(text):
    text = text.lstrip()
    while text.startswith(("//", "/*")):
        if text.startswith("//"):
            end = text.find("\n")
            text = "" if end < 0 else text[end + 1:].lstrip()
        else:
            end = text.find("*/", 2)
            text = "" if end < 0 else text[end + 2:].lstrip()
    return text


class _LiteralResolver:
    """Resolve the literal constants used by the CLMS source files."""

    def __init__(self):
        self.expressions = {}
        self.cache = {}
        self.active = set()

    def add_source(self, source):
        """Index top-level JavaScript and TypeScript constants."""
        for match in CONSTANT.finditer(source):
            self.expressions[match.group("name")] = _read_expression(
                source,
                match.end(),
            )

    def resolve_name(self, name):
        """Resolve a previously indexed constant."""
        if name in self.cache:
            return self.cache[name]
        if name in self.active or name not in self.expressions:
            return UNKNOWN
        self.active.add(name)
        value = self.resolve(self.expressions[name])
        self.active.remove(name)
        self.cache[name] = value
        return value

    # pylint: disable=too-many-branches,too-many-return-statements
    def resolve(self, expression):
        """Resolve the literal subset needed by the Browser metadata."""
        expression = re.sub(
            r"\s+as\s+const\s*$",
            "",
            expression.strip(),
        )
        while _is_wrapped(expression, "(", ")"):
            expression = expression[1:-1].strip()
        if (
            expression.startswith("Object.freeze(")
            and expression.endswith(")")
        ):
            expression = expression[len("Object.freeze("):-1].strip()
        if not expression:
            return UNKNOWN
        if expression[0] in "'\"" and expression[-1] == expression[0]:
            try:
                return ast.literal_eval(expression)
            except (SyntaxError, ValueError):
                return expression[1:-1]
        if expression.startswith("`") and expression.endswith("`"):
            if "${" in expression:
                return UNKNOWN
            return expression[1:-1]
        if expression in ("null", "undefined"):
            return None
        if expression in ("true", "false"):
            return expression == "true"
        if re.fullmatch(r"[-+]?\d+(?:\.\d+)?", expression):
            return float(expression) if "." in expression else int(expression)
        if _is_wrapped(expression, "[", "]"):
            result = []
            for item in _split_top_level(expression[1:-1]):
                if item.startswith("..."):
                    value = self.resolve(item[3:])
                    if isinstance(value, list):
                        result.extend(value)
                else:
                    value = self.resolve(item)
                    if value is not UNKNOWN:
                        result.append(value)
            return result
        if _is_wrapped(expression, "{", "}"):
            result = {}
            for item in _split_top_level(expression[1:-1]):
                if item.startswith("..."):
                    value = self.resolve(item[3:])
                    if isinstance(value, dict):
                        result.update(value)
                    continue
                key, raw_value = _split_property(item)
                key = _without_leading_comments(key or "")
                if not key:
                    continue
                if _is_wrapped(key, "[", "]"):
                    key = self.resolve(key[1:-1])
                elif key[0] in "'\"`":
                    key = self.resolve(key)
                value = self.resolve(raw_value)
                if key is not UNKNOWN and value is not UNKNOWN:
                    result[str(key)] = value
            return result
        match = MEMBER.fullmatch(expression)
        if not match:
            return UNKNOWN
        value = self.resolve_name(match.group("name"))
        for part in match.group("parts").split("."):
            if not part:
                continue
            if not isinstance(value, dict) or part not in value:
                return UNKNOWN
            value = value[part]
        return value


def _browser_root(path):
    """Locate a Copernicus Browser checkout from a configured path."""
    path = Path(path).expanduser().resolve()
    if path.name == HANDLER.name and path.is_file():
        root = path
        for _part in HANDLER.parts:
            root = root.parent
        return root
    for root in (path, path / "src/addons/copernicus-browser"):
        if (root / HANDLER).is_file():
            return root
    raise BYOCExtractionError("CLMSDataSourceHandler.jsx was not found")


def _assignment(source, name):
    match = re.search(
        r"(?:^|\n)\s*(?:this\.)?" + re.escape(name) + r"\s*=",
        source,
        re.M,
    )
    if not match:
        raise BYOCExtractionError(name + " was not found")
    return _read_expression(source, match.end())


def _source_collections(root):
    """Read known collections and low-resolution relationships."""
    handler_path = root / HANDLER
    source_dir = handler_path.parent
    source_paths = (
        source_dir / "dataSourceConstants.ts",
        source_dir / "CLMSVLCCSpecificConst.ts",
        handler_path,
    )

    resolver = _LiteralResolver()
    handler_source = handler_path.read_text(encoding="utf-8")
    for path in source_paths:
        if not path.is_file():
            raise BYOCExtractionError(
                "Required Copernicus Browser source was not found: "
                + path.name
            )
        source = path.read_text(encoding="utf-8")
        resolver.add_source(source)

    known = resolver.resolve(_assignment(handler_source, "KNOWN_COLLECTIONS"))
    alternatives = resolver.resolve_name(
        "LOW_RESOLUTION_ALTERNATIVE_COLLECTIONS"
    )
    if not isinstance(known, dict):
        raise BYOCExtractionError("KNOWN_COLLECTIONS is not a literal object")
    if not isinstance(alternatives, dict):
        raise BYOCExtractionError(
            "LOW_RESOLUTION_ALTERNATIVE_COLLECTIONS is not a literal object"
        )

    datasets = {}
    for dataset_id, values in known.items():
        if not isinstance(values, list):
            continue
        for value in values:
            try:
                collection_id = normalize_collection_id(value)
            except BYOCExtractionError:
                continue
            datasets[collection_id] = str(dataset_id)

    low_resolution = {}
    for high_value, value in alternatives.items():
        if not isinstance(value, dict):
            continue
        try:
            high_id = normalize_collection_id(high_value)
            low_id = normalize_collection_id(
                value.get("lowResolutionCollectionId")
            )
        except BYOCExtractionError:
            continue
        threshold = value.get("lowResolutionMetersPerPixelThreshold")
        if not isinstance(threshold, (int, float)):
            raise BYOCExtractionError(
                "Missing low-resolution threshold for " + high_id
            )
        if high_id not in datasets:
            raise BYOCExtractionError(
                "Low-resolution mapping references an unknown collection: "
                + high_id
            )
        low_resolution[high_id] = {
            "collectionId": low_id,
            "metersPerPixelThreshold": threshold,
        }
    if not datasets:
        raise BYOCExtractionError("No CLMS BYOC collections were found")
    return datasets, low_resolution


def _iter_configured_layers(value):
    """Yield layer dictionaries from configuration cache shapes."""
    if isinstance(value, dict):
        if value.get("collectionId") and isinstance(value.get("styles"), list):
            yield value
        for child in value.values():
            for layer in _iter_configured_layers(child):
                yield layer
    elif isinstance(value, list):
        for child in value:
            for layer in _iter_configured_layers(child):
                yield layer


def _match_collection(value, collection_ids):
    """Match exact and Browser placeholder collection identifiers."""
    raw = str(value or "").strip().lower()
    if raw.startswith("byoc-"):
        raw = raw[5:]
    if UUID.fullmatch(raw):
        return raw if raw in collection_ids else None
    marker = "-your-instanceid-here"
    prefix = raw.split(marker, 1)[0].rstrip("-")
    if not prefix:
        return None
    matches = [item for item in collection_ids if item.startswith(prefix)]
    if len(matches) > 1:
        raise BYOCExtractionError(
            "Ambiguous cached collection ID: " + str(value)
        )
    return matches[0] if matches else None


def _default_evalscript(layer):
    """Return the configured default visualization evalscript."""
    preferred = layer.get("defaultStyleName") or "default"
    styles = layer.get("styles") or []
    for style in styles:
        if style.get("name") == preferred and style.get("evalScript"):
            return style["evalScript"]
    for style in styles:
        if style.get("evalScript"):
            return style["evalScript"]
    return None


def _scale(meters_per_pixel):
    """Convert a Web Mercator resolution threshold to map scale."""
    if meters_per_pixel is None:
        return None
    level_zero_resolution = 156543.033928
    level_zero_scale = 591657527.591555
    level = min(
        range(31),
        key=lambda value: abs(
            level_zero_resolution / (2 ** value) - meters_per_pixel
        ),
    )
    scale = level_zero_scale / (2 ** level)
    return int(round(scale / 10000) * 10000)


def _source_commit(root):
    """Return the source checkout commit when it is a Git checkout."""
    try:
        result = subprocess.run(
            ("git", "-C", str(root), "rev-parse", "HEAD"),
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return result.stdout.strip() or None


# pylint: disable=too-many-locals
def extract_browser_configuration(path):
    """Extract Browser collection pairs and their visualization layers."""
    root = _browser_root(path)
    datasets, alternatives = _source_collections(root)
    configuration_path = root / CONFIGURATION
    if not configuration_path.is_file():
        raise BYOCExtractionError("configuration.json was not found")
    try:
        configuration = json.loads(
            configuration_path.read_text(encoding="utf-8")
        )
    except (OSError, ValueError) as error:
        raise BYOCExtractionError(
            "configuration.json could not be read: " + str(error)
        )

    selected = {}
    for layer in _iter_configured_layers(configuration):
        collection_id = _match_collection(layer.get("collectionId"), datasets)
        evalscript = _default_evalscript(layer)
        layer_id = layer.get("id")
        if not collection_id or not layer_id or not evalscript:
            continue
        key = (collection_id, str(layer_id))
        candidate = deepcopy(layer)
        candidate["evalscript"] = evalscript
        current = selected.get(key)
        rank = (layer.get("lastUpdated") or "", layer.get("title") or "")
        current_rank = (
            (current or {}).get("lastUpdated") or "",
            (current or {}).get("title") or "",
        )
        if current is None or rank > current_rank:
            selected[key] = candidate

    collections = {}
    for collection_id, dataset_id in datasets.items():
        alternative = alternatives.get(collection_id)
        threshold = (
            alternative["metersPerPixelThreshold"] if alternative else None
        )
        scale = _scale(threshold)
        layers = []
        for (candidate_id, layer_id), layer in selected.items():
            if candidate_id != collection_id:
                continue
            high_resolution = {
                "collectionId": "byoc-" + collection_id,
                "minScale": scale + 1 if scale is not None else 0,
                "maxScale": 0,
                "evalscript": layer["evalscript"],
            }
            low_resolution = None
            if alternative:
                low_resolution = {
                    "collectionId": "byoc-" + alternative["collectionId"],
                    "minScale": 0,
                    "maxScale": scale,
                    "evalscript": layer["evalscript"],
                }
            layers.append(
                {
                    "layerId": layer_id,
                    "title": layer.get("title") or layer_id,
                    "description": layer.get("description"),
                    "type": "dual" if alternative else "single",
                    "hasLowRes": bool(alternative),
                    "thresholdMetersPerPixel": threshold,
                    "thresholdScale": scale,
                    "highRes": high_resolution,
                    "lowRes": low_resolution,
                }
            )
        layers.sort(key=lambda item: (item["title"], item["layerId"]))
        collections[collection_id] = {
            "browserCollectionId": "byoc-" + collection_id,
            "datasetId": dataset_id,
            "layers": layers,
        }

    return {
        "version": 1,
        "source": {
            "repository": "eu-cdse/copernicus-browser",
            "commit": _source_commit(root),
        },
        "collections": collections,
    }


def load_collection_mapping(path):
    """Load the deployment mapping from local/proxy IDs to Browser IDs."""
    path = Path(path).expanduser().resolve()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise BYOCExtractionError(
            "BYOC collection mapping could not be read: " + str(error)
        )
    entries = payload.get("collections") if isinstance(payload, dict) else None
    if not isinstance(entries, list):
        raise BYOCExtractionError(
            "BYOC mapping must contain a collections array"
        )

    mapping = {}
    browser_ids = set()
    for entry in entries:
        if not isinstance(entry, dict):
            raise BYOCExtractionError("Invalid BYOC mapping entry")
        local_id = normalize_collection_id(entry.get("localCollectionId"))
        browser_id = normalize_collection_id(
            entry.get("browserCollectionId")
        )
        if local_id in mapping:
            raise BYOCExtractionError(
                "Duplicate local BYOC mapping: " + local_id
            )
        if browser_id in browser_ids:
            raise BYOCExtractionError(
                "Duplicate Browser BYOC mapping: " + browser_id
            )
        mapping[local_id] = browser_id
        browser_ids.add(browser_id)
    if not mapping:
        raise BYOCExtractionError("BYOC collection mapping is empty")
    return mapping


def build_resolved_snapshot(browser_path, mapping_path):
    """Build a validated snapshot keyed by local/proxy collection UUID."""
    extracted = extract_browser_configuration(browser_path)
    mapping = load_collection_mapping(mapping_path)
    resolved = {}
    for local_id, browser_id in mapping.items():
        configuration = extracted["collections"].get(browser_id)
        if configuration is None:
            raise BYOCExtractionError(
                "Mapped Browser collection was not extracted: " + browser_id
            )
        if not configuration.get("layers"):
            raise BYOCExtractionError(
                "Mapped Browser collection has no evalscript: " + browser_id
            )
        resolved[local_id] = deepcopy(configuration)
    return {
        "version": extracted["version"],
        "source": extracted["source"],
        "collections": resolved,
    }
