from __future__ import annotations

import json
import re
import unicodedata
from functools import lru_cache
from pathlib import Path
from typing import Any

try:
    import cmudict
except ImportError:  # Safe fallback: chart still works without name sounds.
    cmudict = None


REGISTRY_PATH = Path(__file__).with_name("verified_name_sounds.json")

# Generic organisation prefixes are skipped only when choosing the first
# distinctive word for an exact dictionary lookup.
GENERIC_PREFIXES = {
    "fc", "afc", "cf", "sc", "ac", "cd", "fk", "nk", "sk", "sv",
    "vfb", "vfl", "club", "football", "futbol", "futebol",
}

ARPABET_VOWELS = {
    "AA", "AE", "AH", "AO", "AW", "AY", "EH", "ER",
    "EY", "IH", "IY", "OW", "OY", "UH", "UW",
}

VOWEL_MAP = {
    "AA": "a",
    "AE": "a",
    "AH": "a",
    "AO": "o",
    "AW": "au",
    "AY": "ai",
    "EH": "e",
    "ER": "a",
    "EY": "e",
    "IH": "i",
    "IY": "i",
    "OW": "o",
    "OY": "o",
    "UH": "u",
    "UW": "u",
}

CONSONANT_MAP = {
    "B": "b",
    "CH": "ch",
    "D": "d",
    "DH": "dh",
    "F": "ph",
    "G": "g",
    "HH": "h",
    "JH": "j",
    "K": "k",
    "L": "l",
    "M": "m",
    "N": "n",
    "NG": "ng",
    "P": "p",
    "R": "r",
    "S": "s",
    "SH": "sh",
    "T": "t",
    "TH": "th",
    "V": "v",
    "W": "w",
    "Y": "y",
    "Z": "j",
    "ZH": "j",
}

# The resolver returns only sounds compatible with the Chapter 7 comparison
# vocabulary. It does not use spelling as pronunciation evidence.
BOOK_COMPATIBLE_SOUNDS = {
    "a", "i", "u", "e", "o", "ai", "au",
    "ka", "ki", "ku", "ke", "ko",
    "kha", "khi", "khu", "khe", "kho",
    "ga", "gi", "gu", "ge", "go",
    "gha", "ghi", "ghu", "ghe", "gho",
    "cha", "chi", "chu", "che", "cho",
    "ja", "ji", "ju", "je", "jo",
    "jha", "jhi", "jhu", "jhe", "jho",
    "ta", "ti", "tu", "te", "to",
    "tha", "thi", "thu", "the", "tho",
    "da", "di", "du", "de", "do",
    "dha", "dhi", "dhu", "dhe", "dho",
    "na", "ni", "nu", "ne", "no",
    "pa", "pi", "pu", "pe", "po",
    "pha", "phi", "phu", "phe", "pho",
    "ba", "bi", "bu", "be", "bo",
    "ma", "mi", "mu", "me", "mo",
    "ya", "yi", "yu", "ye", "yo",
    "ra", "ri", "ru", "re", "ro",
    "la", "li", "lu", "le", "lo",
    "va", "vi", "vu", "ve", "vo",
    "wa", "wi", "wu", "we", "wo",
    "sha", "shi", "shu", "she", "sho",
    "sa", "si", "su", "se", "so",
    "ha", "hi", "hu", "he", "ho",
    "nga", "ngi", "ngu", "nge", "ngo",
}


def _normalize_key(value: str) -> str:
    raw = unicodedata.normalize("NFKD", str(value))
    raw = "".join(ch for ch in raw if not unicodedata.combining(ch))
    raw = raw.lower().replace("&", " and ")
    return " ".join(re.findall(r"[a-z0-9]+", raw))


def _word_tokens(value: str) -> list[str]:
    normalized = _normalize_key(value)
    return re.findall(r"[a-z]+", normalized)


def _first_distinctive_word(team_name: str) -> str | None:
    for token in _word_tokens(team_name):
        if token not in GENERIC_PREFIXES:
            return token
    return None


@lru_cache(maxsize=1)
def _manual_registry() -> dict[str, dict[str, Any]]:
    if not REGISTRY_PATH.exists():
        return {}

    try:
        payload = json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}

    output: dict[str, dict[str, Any]] = {}
    for item in payload.get("teams", []):
        if not isinstance(item, dict):
            continue
        names = [item.get("official_name"), *item.get("aliases", [])]
        for name in names:
            key = _normalize_key(name or "")
            if key:
                output[key] = item
    return output


@lru_cache(maxsize=1)
def _cmu_dictionary() -> dict[str, list[list[str]]]:
    if cmudict is None:
        return {}
    try:
        return cmudict.dict()
    except Exception:
        return {}


def _strip_stress(phoneme: str) -> str:
    return re.sub(r"\d", "", str(phoneme).upper())


def _arpabet_to_book_sound(pronunciation: list[str]) -> str | None:
    phonemes = [_strip_stress(item) for item in pronunciation]

    vowel_index = None
    for index, phoneme in enumerate(phonemes):
        if phoneme in ARPABET_VOWELS:
            vowel_index = index
            break

    if vowel_index is None:
        return None

    vowel = VOWEL_MAP.get(phonemes[vowel_index])
    if not vowel:
        return None

    onset_phonemes = phonemes[:vowel_index]
    if not onset_phonemes:
        sound = vowel
    else:
        # Conservatively use the first pronounced onset. This prevents
        # spelling clusters from being treated as extra evidence.
        consonant = CONSONANT_MAP.get(onset_phonemes[0])
        if not consonant:
            return None
        sound = consonant + vowel

    return sound if sound in BOOK_COMPATIBLE_SOUNDS else None


def _manual_resolution(team_name: str) -> dict[str, Any] | None:
    item = _manual_registry().get(_normalize_key(team_name))
    if not item:
        return None

    sounds = [
        str(sound).strip().lower()
        for sound in item.get("book_compatible_opening_sounds", [])
        if str(sound).strip().lower() in BOOK_COMPATIBLE_SOUNDS
    ]
    eligible = bool(sounds) and bool(item.get("decision_eligible", False))

    return {
        "status": "Pass" if eligible else "Unverified",
        "team_name": team_name,
        "opening_sounds": sounds if eligible else [],
        "decision_eligible": eligible,
        "source_type": "manual_verified_registry",
        "source_reference": item.get("source_reference"),
        "reviewed_at": item.get("reviewed_at"),
        "matched_registry_name": item.get("official_name"),
        "automatic": True,
        "raw_spelling_used_as_pronunciation": False,
        "reason": None if eligible else "Registry entry is not decision-eligible.",
    }


def resolve_verified_team_opening_sounds(team_name: str) -> dict[str, Any]:
    """
    Resolve an optional book-compatible opening sound.

    Priority:
    1. Exact manually verified registry entry.
    2. Exact CMU Pronouncing Dictionary word with an unambiguous mapping.
    3. Unverified: return no sound and never affect chart validity.
    """
    manual = _manual_resolution(team_name)
    if manual is not None:
        return manual

    word = _first_distinctive_word(team_name)
    if not word:
        return {
            "status": "Unverified",
            "team_name": team_name,
            "opening_sounds": [],
            "decision_eligible": False,
            "source_type": None,
            "automatic": True,
            "raw_spelling_used_as_pronunciation": False,
            "reason": "No distinctive dictionary word was found.",
        }

    pronunciations = _cmu_dictionary().get(word, [])
    mapped = {
        sound
        for pronunciation in pronunciations
        if (sound := _arpabet_to_book_sound(pronunciation))
    }

    if not pronunciations:
        reason = "No exact CMUdict entry exists for the distinctive word."
    elif len(mapped) != 1:
        reason = "Dictionary variants do not produce one unambiguous book sound."
    else:
        sound = next(iter(mapped))
        return {
            "status": "Pass",
            "team_name": team_name,
            "opening_sounds": [sound],
            "decision_eligible": True,
            "source_type": "cmudict_exact_word",
            "source_reference": "CMU Pronouncing Dictionary",
            "matched_dictionary_word": word,
            "arpabet_pronunciations": pronunciations,
            "mapping_policy": "first pronounced onset plus first vowel",
            "automatic": True,
            "raw_spelling_used_as_pronunciation": False,
            "reason": None,
        }

    return {
        "status": "Unverified",
        "team_name": team_name,
        "opening_sounds": [],
        "decision_eligible": False,
        "source_type": "cmudict_exact_word" if pronunciations else None,
        "matched_dictionary_word": word,
        "automatic": True,
        "raw_spelling_used_as_pronunciation": False,
        "reason": reason,
    }


def name_sound_resolver_health() -> dict[str, Any]:
    registry = _manual_registry()
    return {
        "status": "ok",
        "optional": True,
        "cmudict_available": cmudict is not None,
        "manual_registry_alias_count": len(registry),
        "missing_sound_blocks_chart": False,
        "raw_spelling_is_decision_grade": False,
    }
