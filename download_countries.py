"""
Download country data from REST Countries API into a pandas DataFrame.

API: https://restcountries.com/ - free public service without an API key.
It is safer to pin endpoint version in code (v3.1), because the field schema may change.
"""

from __future__ import annotations
import json
from typing import Any

import pandas as pd
import requests

# API version is pinned. Important: public endpoint requires `fields` query and no more
# than 10 fields per request (otherwise HTTP 400). Data is fetched in batches
# and then merged by `cca3`.
API_BASE = "https://restcountries.com/v3.1/all"
# Exactly up to 10 fields per query (including cca3), or API returns an error.
FIELD_BATCHES: tuple[str, ...] = (
    "cca3,name,flags,population,area,region,subregion,capital,latlng,landlocked",
    "cca3,tld,cca2,ccn3,cioc,currencies,languages,borders,timezones,continents",
    "cca3,independent,status,unMember,altSpellings,maps,idd,coatOfArms,car,gini",
    "cca3,demonyms,flag,startOfWeek,postalCode,capitalInfo,fifa",
)
REQUEST_TIMEOUT_SEC = 60


def _join_list(values: list[Any] | None, sep: str = ", ") -> str | None:
    if not values:
        return None
    return sep.join(str(x) for x in values)


def _currencies_str(currencies: dict[str, Any] | None) -> str | None:
    if not currencies:
        return None
    parts = []
    for code, info in currencies.items():
        if isinstance(info, dict):
            name = info.get("name", "")
            sym = info.get("symbol", "")
            parts.append(f"{code}: {name} ({sym})".strip())
        else:
            parts.append(f"{code}: {info}")
    return "; ".join(parts)


def _languages_str(languages: dict[str, str] | None) -> str | None:
    if not languages:
        return None
    return ", ".join(f"{k} ({v})" for k, v in sorted(languages.items()))


def _native_names_str(name_block: dict[str, Any] | None) -> str | None:
    if not name_block:
        return None
    parts = []
    for lang, n in name_block.items():
        if isinstance(n, dict):
            parts.append(f"{lang}: {n.get('common', '')} / {n.get('official', '')}")
    return " | ".join(parts) if parts else None


def _flatten_country(c: dict[str, Any]) -> dict[str, Any]:
    """Flatten nested country JSON into a table-friendly dictionary."""
    name = c.get("name") or {}
    flags = c.get("flags") or {}
    maps = c.get("maps") or {}
    idd = c.get("idd") or {}
    car = c.get("car") or {}
    demonyms = c.get("demonyms") or {}
    capital_info = c.get("capitalInfo") or {}
    postal = c.get("postalCode") or {}
    coat = c.get("coatOfArms") or {}
    gini = c.get("gini")
    gini_str = json.dumps(gini, ensure_ascii=False) if gini is not None else None

    latlng = c.get("latlng") or []
    lat = latlng[0] if len(latlng) > 0 else None
    lng = latlng[1] if len(latlng) > 1 else None

    capital_list = c.get("capital")
    capital_main = capital_list[0] if capital_list else None

    row: dict[str, Any] = {
        "cca2": c.get("cca2"),
        "cca3": c.get("cca3"),
        "ccn3": c.get("ccn3"),
        "cioc": c.get("cioc"),
        "independent": c.get("independent"),
        "status": c.get("status"),
        "un_member": c.get("unMember"),
        "name_common": name.get("common"),
        "name_official": name.get("official"),
        "name_native": _native_names_str(name.get("nativeName")),
        "tld": _join_list(c.get("tld")),
        "capital": capital_main,
        "capitals_all": _join_list(capital_list),
        "alt_spellings": _join_list(c.get("altSpellings")),
        "region": c.get("region"),
        "subregion": c.get("subregion"),
        "continents": _join_list(c.get("continents")),
        "languages": _languages_str(c.get("languages")),
        "currencies": _currencies_str(c.get("currencies")),
        "population": c.get("population"),
        "area": c.get("area"),
        "borders": _join_list(c.get("borders")),
        "landlocked": c.get("landlocked"),
        "lat": lat,
        "lng": lng,
        "flag_emoji": c.get("flag"),
        "flags_png": flags.get("png"),
        "flags_svg": flags.get("svg"),
        "flags_alt": flags.get("alt"),
        "maps_openstreetmap": maps.get("openStreetMaps"),
        "maps_google": maps.get("googleMaps"),
        "idd_root": idd.get("root"),
        "idd_suffixes": _join_list(idd.get("suffixes")),
        "start_of_week": c.get("startOfWeek"),
        "timezones": _join_list(c.get("timezones")),
        "car_side": car.get("side"),
        "car_signs": _join_list(car.get("signs")),
        "demonyms_m": (demonyms.get("eng") or {}).get("m") if demonyms else None,
        "demonyms_f": (demonyms.get("eng") or {}).get("f") if demonyms else None,
        "coat_of_arms_png": coat.get("png"),
        "coat_of_arms_svg": coat.get("svg"),
        "capital_latlng": _join_list(capital_info.get("latlng")),
        "postal_code_format": postal.get("format"),
        "gini": gini_str,
        "fifa": c.get("fifa"),
    }
    return row


def _fetch_raw_countries_merged() -> list[dict[str, Any]]:
    """Run several GET /all?fields=... calls and merge country objects by cca3."""
    merged: dict[str, dict[str, Any]] = {}
    with requests.Session() as session:
        for fields in FIELD_BATCHES:
            response = session.get(
                API_BASE,
                params={"fields": fields},
                timeout=REQUEST_TIMEOUT_SEC,
            )
            response.raise_for_status()
            batch = response.json()
            if not isinstance(batch, list):
                raise ValueError("Unexpected API response: expected a JSON array")
            for item in batch:
                cca3 = item.get("cca3")
                if not cca3:
                    continue
                if cca3 not in merged:
                    merged[cca3] = {}
                merged[cca3].update(item)
    return list(merged.values())


def fetch_countries_df() -> pd.DataFrame:
    """
    Request /v3.1/all multiple times with different `fields` and return DataFrame.

    Note: for transient API failures, retry with backoff is recommended
    (tenacity / urllib3 Retry).
    """
    data = _fetch_raw_countries_merged()
    rows = [_flatten_country(c) for c in data]
    df = pd.DataFrame(rows)
    # Stable column order helps readability and keeps DB schema predictable.
    df = df.reindex(sorted(df.columns), axis=1)
    return df


if __name__ == "__main__":
    df_main = fetch_countries_df()
    print(f"Rows: {len(df_main)}, columns: {len(df_main.columns)}")
    print(df_main.head(3).to_string())
