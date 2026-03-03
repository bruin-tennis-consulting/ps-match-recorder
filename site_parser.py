from __future__ import annotations

import argparse
import copy
import json
import os
import plistlib
import re
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

MANUAL_STREAM_URL_REQUIRED = "MANUAL_STREAM_URL_REQUIRED"
DEFAULT_CONFIG = {
    "schedule_urls": [
        "https://bigten.org/mten/schedule/",
        "https://nextgen.bigten.org/mten/schedule/",
    ],
    "schedule_timezone": "America/New_York",
    "output_root": "recordings",
    "catalog_path": "data/bigten_matches.json",
    "launchd_output_dir": "launchd",
    "logs_dir": "logs",
    "lead_time_minutes": 10,
    "stream_retry_minutes": 45,
    "poll_interval_seconds": 60,
    "default_duration_minutes": 300,
    "request_timeout_seconds": 20,
    "ffmpeg_path": "ffmpeg",
    "headers": {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/133.0.0.0 Safari/537.36"
        )
    },
    "youtube": {
        "enabled": False,
        "client_secrets_file": "config/youtube_client_secrets.json",
        "token_file": "config/youtube_token.json",
        "privacy_status": "unlisted",
        "category_id": "17",
        "title_template": "{match_title} | {start_date}",
        "description_template": (
            "Recorded from {stream_page_url}\n"
            "Scheduled match time: {start_iso}\n"
            "Source schedule: {schedule_url}"
        ),
        "tags": ["college tennis", "Big Ten", "men's tennis"],
    },
    "match_overrides": {},
}
DIRECT_STREAM_SUFFIXES = (".m3u8", ".mp4", ".mpd")
MONTH_PATTERN = (
    r"(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|"
    r"Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|"
    r"Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"
)
TIME_PATTERN = r"\b\d{1,2}(?::\d{2})?\s*[APap]\.?\s*[Mm]\.?\b"
MATCHUP_PATTERN = re.compile(
    r"([A-Za-z0-9.'&()/-]+(?:\s+[A-Za-z0-9.'&()/-]+){0,5})\s+"
    r"(vs\.?|at)\s+"
    r"([A-Za-z0-9.'&()/-]+(?:\s+[A-Za-z0-9.'&()/-]+){0,5})"
)

PLAYSIGHT_EMAIL = "Playsight@ucla.edu"
PLAYSIGHT_PASSWORD = "UCLAtennis2025*"


@dataclass
class MatchRecord:
    match_id: str
    title: str
    start_iso: str
    timezone: str
    schedule_url: str
    duration_minutes: int
    home_team: str | None = None
    away_team: str | None = None
    location: str | None = None
    watch_page_url: str | None = None
    stream_page_url: str | None = None
    provider: str | None = None
    notes: list[str] = field(default_factory=list)

    def start_datetime(self) -> datetime:
        return datetime.fromisoformat(self.start_iso)

    def to_dict(self) -> dict[str, Any]:
        return {
            "match_id": self.match_id,
            "title": self.title,
            "start_iso": self.start_iso,
            "timezone": self.timezone,
            "schedule_url": self.schedule_url,
            "duration_minutes": self.duration_minutes,
            "home_team": self.home_team,
            "away_team": self.away_team,
            "location": self.location,
            "watch_page_url": self.watch_page_url,
            "stream_page_url": self.stream_page_url,
            "provider": self.provider,
            "notes": self.notes,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "MatchRecord":
        return cls(
            match_id=payload["match_id"],
            title=payload["title"],
            start_iso=payload["start_iso"],
            timezone=payload["timezone"],
            schedule_url=payload["schedule_url"],
            duration_minutes=payload["duration_minutes"],
            home_team=payload.get("home_team"),
            away_team=payload.get("away_team"),
            location=payload.get("location"),
            watch_page_url=payload.get("watch_page_url"),
            stream_page_url=payload.get("stream_page_url"),
            provider=payload.get("provider"),
            notes=list(payload.get("notes", [])),
        )


@dataclass
class StreamTarget:
    label: str
    page_url: str
    media_url: str


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = copy.deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")


def load_config(config_path: Path) -> dict[str, Any]:
    payload = load_json(config_path, {})
    config = deep_merge(DEFAULT_CONFIG, payload)

    for key in ("catalog_path", "output_root", "launchd_output_dir", "logs_dir"):
        config[key] = str(resolve_path(config_path, config[key]))

    youtube = config["youtube"]
    youtube["client_secrets_file"] = str(
        resolve_path(config_path, youtube["client_secrets_file"])
    )
    youtube["token_file"] = str(resolve_path(
        config_path, youtube["token_file"]))
    return config


def resolve_path(config_path: Path, value: str) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return (config_path.parent / path).resolve()


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def slugify(value: str) -> str:
    lowered = normalize_text(value).lower()
    lowered = lowered.replace("&", "and")
    lowered = re.sub(r"[^a-z0-9]+", "-", lowered)
    return lowered.strip("-")


def safe_filename(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", normalize_text(value))
    return cleaned.strip("_") or "match"


def infer_provider(url: str | None) -> str | None:
    if not url:
        return None
    host = urlparse(url).netloc.lower()
    if "playsight" in host:
        return "playsight"
    if "youtube" in host or "youtu.be" in host:
        return "youtube"
    if "sidearm" in host:
        return "sidearm"
    if "stretchinternet" in host:
        return "stretchinternet"
    if "bigtenplus" in host or "bigtenplus.com" in host:
        return "bigtenplus"
    return host or None


def is_playsight_url(url: str | None) -> bool:
    if not url:
        return False
    return "playsight.com" in urlparse(url).netloc.lower()


def is_playsight_facility_or_multi_page(url: str | None) -> bool:
    if not is_playsight_url(url):
        return False
    path = urlparse(url or "").path.lower()
    return "/facility/" in path or "/live/" in path


def parse_datetime(value: str, timezone_name: str) -> datetime | None:
    if not value:
        return None

    parsed = None
    try:
        from dateutil import parser as date_parser

        parsed = date_parser.parse(value)
    except Exception:
        for fmt in (
            "%B %d %Y %I:%M %p",
            "%b %d %Y %I:%M %p",
            "%B %d %I:%M %p",
            "%b %d %I:%M %p",
        ):
            try:
                parsed = datetime.strptime(value, fmt)
                break
            except ValueError:
                continue

    if parsed is None:
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=ZoneInfo(timezone_name))
    return parsed


def split_matchup(title: str, location: str | None = None) -> tuple[str | None, str | None]:
    title = normalize_text(title)
    matchup = MATCHUP_PATTERN.search(title)
    if not matchup:
        return None, None

    first_team, separator, second_team = matchup.groups()
    if separator.lower().startswith("at"):
        return second_team.strip(), first_team.strip()

    if location:
        location_lower = location.lower()
        if second_team.lower() in location_lower:
            return second_team.strip(), first_team.strip()
        if first_team.lower() in location_lower:
            return first_team.strip(), second_team.strip()

    return None, None


def build_match_id(start_dt: datetime, title: str) -> str:
    return f"{start_dt.date().isoformat()}_{slugify(title)}"


def fetch_html(url: str, config: dict[str, Any], rendered: bool = False) -> str:
    if rendered:
        return fetch_html_with_selenium(url)

    response = requests.get(
        url,
        headers=config["headers"],
        timeout=config["request_timeout_seconds"],
    )
    response.raise_for_status()
    return response.text


def fetch_html_with_selenium(url: str) -> str:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service as ChromeService
    from webdriver_manager.chrome import ChromeDriverManager

    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    options.add_argument("--no-sandbox")

    driver = webdriver.Chrome(
        service=ChromeService(ChromeDriverManager().install()),
        options=options,
    )
    try:
        driver.get(url)
        time.sleep(8)
        return driver.page_source
    finally:
        driver.quit()


def try_fetch_schedule_html(config: dict[str, Any]) -> tuple[str, str]:
    last_error = None
    for schedule_url in config["schedule_urls"]:
        for rendered in (False, True):
            try:
                html = fetch_html(schedule_url, config, rendered=rendered)
                if normalize_text(html):
                    return schedule_url, html
            except Exception as exc:
                last_error = exc

    catalog_parent = Path(config["catalog_path"]).parent
    debug_path = catalog_parent / "schedule_debug.html"
    if debug_path.exists():
        return config["schedule_urls"][0], debug_path.read_text(encoding="utf-8")

    raise RuntimeError(
        f"Unable to fetch any configured schedule URL. Last error: {last_error}"
    )


def walk_for_events(value: Any) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    if isinstance(value, dict):
        value_type = value.get("@type")
        if value_type in {"SportsEvent", "Event"}:
            events.append(value)
        for nested in value.values():
            events.extend(walk_for_events(nested))
    elif isinstance(value, list):
        for item in value:
            events.extend(walk_for_events(item))
    return events


def parse_json_scripts(soup: BeautifulSoup) -> list[Any]:
    payloads = []
    for script in soup.find_all("script"):
        script_type = script.get("type", "")
        raw = script.string or script.get_text()
        if not raw:
            continue
        if "ld+json" not in script_type and raw.count("{") == 0:
            continue
        raw = raw.strip()
        if not raw:
            continue
        try:
            payloads.append(json.loads(raw))
        except json.JSONDecodeError:
            continue
    return payloads


def gather_url_candidates(value: Any) -> list[str]:
    candidates: list[str] = []
    if isinstance(value, dict):
        for nested_key, nested_value in value.items():
            if nested_key.lower() == "url" and isinstance(nested_value, str):
                candidates.append(nested_value)
            elif nested_key.lower() == "href" and isinstance(nested_value, str):
                candidates.append(nested_value)
            else:
                candidates.extend(gather_url_candidates(nested_value))
    elif isinstance(value, list):
        for item in value:
            candidates.extend(gather_url_candidates(item))
    return candidates


def pick_watch_url(urls: list[str], schedule_url: str) -> str | None:
    ranked = []
    for url in urls:
        absolute = urljoin(schedule_url, url)
        lowered = absolute.lower()
        score = 0
        if any(keyword in lowered for keyword in ("watch", "live", "stream", "video")):
            score += 3
        if "playsight" in lowered:
            score += 4
        if absolute.endswith(".pdf"):
            score -= 5
        if absolute.startswith("http"):
            score += 1
        ranked.append((score, absolute))

    ranked.sort(reverse=True)
    return ranked[0][1] if ranked and ranked[0][0] > 0 else None


def parse_json_ld_matches(
    soup: BeautifulSoup,
    schedule_url: str,
    timezone_name: str,
    default_duration_minutes: int,
) -> list[MatchRecord]:
    matches: list[MatchRecord] = []
    for payload in parse_json_scripts(soup):
        for event in walk_for_events(payload):
            title = normalize_text(event.get("name", ""))
            if not title:
                continue

            start_dt = parse_datetime(
                event.get("startDate", "") or event.get("start_date", ""),
                timezone_name,
            )
            if not start_dt:
                continue

            location_name = None
            location = event.get("location")
            if isinstance(location, dict):
                location_name = normalize_text(location.get("name", ""))
            elif isinstance(location, str):
                location_name = normalize_text(location)

            home_team = None
            away_team = None
            if isinstance(event.get("homeTeam"), dict):
                home_team = normalize_text(event["homeTeam"].get("name", ""))
            if isinstance(event.get("awayTeam"), dict):
                away_team = normalize_text(event["awayTeam"].get("name", ""))
            if not home_team and not away_team:
                home_team, away_team = split_matchup(title, location_name)

            watch_page_url = pick_watch_url(
                gather_url_candidates(event), schedule_url)
            match_id = build_match_id(start_dt, title)
            matches.append(
                MatchRecord(
                    match_id=match_id,
                    title=title,
                    start_iso=start_dt.isoformat(),
                    timezone=timezone_name,
                    schedule_url=schedule_url,
                    duration_minutes=default_duration_minutes,
                    home_team=home_team,
                    away_team=away_team,
                    location=location_name,
                    watch_page_url=watch_page_url,
                    stream_page_url=watch_page_url,
                    provider=infer_provider(watch_page_url),
                )
            )
    return matches


def normalize_team_name(team: dict[str, Any] | None) -> str | None:
    if not isinstance(team, dict):
        return None

    school = team.get("school")
    if isinstance(school, dict):
        for key in ("title", "name", "market"):
            value = normalize_text(school.get(key, ""))
            if value:
                return value

    for key in ("market", "name", "title", "alias"):
        value = normalize_text(team.get(key, ""))
        if value:
            if value.endswith("team"):
                value = value[:-4].strip()
            return value
    return None


def extract_next_data_fallback(soup: BeautifulSoup) -> dict[str, Any]:
    script = soup.find("script", id="__NEXT_DATA__")
    if not script or not script.string:
        return {}
    try:
        payload = json.loads(script.string)
    except json.JSONDecodeError:
        return {}

    return (
        payload.get("props", {})
        .get("pageProps", {})
        .get("fallback", {})
    )


def find_schedule_cache_entries(fallback: dict[str, Any]) -> list[list[dict[str, Any]]]:
    entries: list[list[dict[str, Any]]] = []
    for key, value in fallback.items():
        if not isinstance(value, list):
            continue
        lowered_key = key.lower()
        if "contenttypeuid:\"schedule\"" not in lowered_key:
            continue
        if value and isinstance(value[0], dict) and value[0].get("_content_type_uid") == "game":
            entries.append(value)
    return entries


def build_title_from_teams(
    away_team: str | None, home_team: str | None, fallback_title: str
) -> str:
    if away_team and home_team:
        return f"{away_team} vs. {home_team}"
    cleaned = normalize_text(fallback_title)
    cleaned = re.sub(r"\s+Men[’']s Tennis game.*$", "", cleaned)
    cleaned = re.sub(r"\s+#\d+$", "", cleaned)
    return cleaned


def extract_watch_links_from_game_links(links: dict[str, Any], schedule_url: str) -> str | None:
    prioritized_groups = ["streaming", "tv"]
    candidates: list[str] = []
    for group in prioritized_groups:
        candidates.extend(gather_url_candidates(links.get(group, [])))

    if not candidates:
        for group in ("live_stats", "radio"):
            for url in gather_url_candidates(links.get(group, [])):
                lowered = url.lower()
                if any(
                    keyword in lowered
                    for keyword in ("watch", "live", "stream", "video", "playsight")
                ):
                    candidates.append(url)

    return pick_watch_url(candidates, schedule_url)


def build_match_record_from_game(
    game: dict[str, Any],
    schedule_url: str,
    timezone_name: str,
    default_duration_minutes: int,
) -> MatchRecord | None:
    datetime_info = game.get("datetime") or {}
    scheduled = datetime_info.get("date_scheduled")
    if not isinstance(scheduled, str) or not scheduled:
        return None

    try:
        start_dt = datetime.fromisoformat(scheduled.replace("Z", "+00:00"))
    except ValueError:
        return None

    teams = game.get("teams") or {}
    away_team = normalize_team_name((teams.get("away_team") or [None])[0])
    home_team = normalize_team_name((teams.get("home_team") or [None])[0])
    title = build_title_from_teams(away_team, home_team, game.get("title", ""))
    if not title:
        return None

    watch_page_url = extract_watch_links_from_game_links(
        game.get("links", {}), schedule_url)
    location_name = normalize_text(
        (game.get("location") or {}).get("name", ""))
    match_id = build_match_id(start_dt, title)

    return MatchRecord(
        match_id=match_id,
        title=title,
        start_iso=start_dt.isoformat(),
        timezone=timezone_name,
        schedule_url=schedule_url,
        duration_minutes=default_duration_minutes,
        home_team=home_team,
        away_team=away_team,
        location=location_name or None,
        watch_page_url=watch_page_url,
        stream_page_url=watch_page_url,
        provider=infer_provider(watch_page_url),
        notes=[],
    )


def parse_next_data_matches(
    soup: BeautifulSoup,
    schedule_url: str,
    timezone_name: str,
    default_duration_minutes: int,
) -> list[MatchRecord]:
    matches: list[MatchRecord] = []
    fallback = extract_next_data_fallback(soup)
    for entry in find_schedule_cache_entries(fallback):
        for game in entry:
            match = build_match_record_from_game(
                game,
                schedule_url,
                timezone_name,
                default_duration_minutes,
            )
            if match is not None:
                matches.append(match)
    return matches


def parse_dom_matches(
    soup: BeautifulSoup,
    schedule_url: str,
    timezone_name: str,
    default_duration_minutes: int,
) -> list[MatchRecord]:
    matches: list[MatchRecord] = []
    seen_signatures: set[str] = set()
    today_year = datetime.now(ZoneInfo(timezone_name)).year

    selectors = [
        "tr",
        "li",
        "article",
        "div.sidearm-schedule-game",
        "div.schedule-game",
        "div.event",
    ]
    for selector in selectors:
        for node in soup.select(selector):
            text = normalize_text(node.get_text(" ", strip=True))
            if len(text) < 20 or len(text) > 450:
                continue
            if not re.search(r"\b(vs\.?|at)\b", text, re.IGNORECASE):
                continue
            if not re.search(MONTH_PATTERN, text):
                continue

            signature = text[:200]
            if signature in seen_signatures:
                continue

            matchup = MATCHUP_PATTERN.search(text)
            if not matchup:
                continue

            date_match = re.search(
                rf"{MONTH_PATTERN}\s+\d{{1,2}}(?:,\s*\d{{4}})?", text, re.IGNORECASE
            )
            time_match = re.search(TIME_PATTERN, text)
            if not date_match or not time_match:
                continue

            date_text = date_match.group(0)
            if "," not in date_text:
                date_text = f"{date_text}, {today_year}"

            title = f"{matchup.group(1)} {matchup.group(2)} {matchup.group(3)}"
            start_dt = parse_datetime(
                f"{date_text} {time_match.group(0)}",
                timezone_name,
            )
            if not start_dt:
                continue

            links = []
            for link in node.find_all("a", href=True):
                href = urljoin(schedule_url, link["href"])
                anchor_text = normalize_text(link.get_text(" ", strip=True))
                links.append((href, anchor_text))

            watch_page_url = None
            for href, anchor_text in links:
                lowered = f"{href} {anchor_text}".lower()
                if any(
                    keyword in lowered
                    for keyword in ("watch", "live", "stream", "video", "playsight")
                ):
                    watch_page_url = href
                    break

            home_team, away_team = split_matchup(title)
            match_id = build_match_id(start_dt, title)
            matches.append(
                MatchRecord(
                    match_id=match_id,
                    title=title,
                    start_iso=start_dt.isoformat(),
                    timezone=timezone_name,
                    schedule_url=schedule_url,
                    duration_minutes=default_duration_minutes,
                    home_team=home_team,
                    away_team=away_team,
                    watch_page_url=watch_page_url,
                    stream_page_url=watch_page_url,
                    provider=infer_provider(watch_page_url),
                )
            )
            seen_signatures.add(signature)

    return matches


def choose_better_match(existing: MatchRecord, candidate: MatchRecord) -> MatchRecord:
    existing_score = score_match(existing)
    candidate_score = score_match(candidate)
    if candidate_score >= existing_score:
        merged = existing.to_dict()
        for key, value in candidate.to_dict().items():
            if value not in (None, "", [], {}):
                merged[key] = value
        return MatchRecord.from_dict(merged)
    return existing


def score_match(match: MatchRecord) -> int:
    score = 0
    if match.watch_page_url:
        score += 3
    if match.stream_page_url:
        score += 3
    if match.home_team:
        score += 1
    if match.away_team:
        score += 1
    if match.location:
        score += 1
    return score


def dedupe_matches(matches: list[MatchRecord]) -> list[MatchRecord]:
    deduped: dict[str, MatchRecord] = {}
    for match in matches:
        existing = deduped.get(match.match_id)
        if existing is None:
            deduped[match.match_id] = match
        else:
            deduped[match.match_id] = choose_better_match(existing, match)
    return sorted(deduped.values(), key=lambda item: item.start_iso)


def merge_overrides(
    matches: list[MatchRecord], overrides: dict[str, dict[str, Any]]
) -> list[MatchRecord]:
    merged_matches = []
    for match in matches:
        payload = match.to_dict()
        override = overrides.get(match.match_id, {})
        payload.update(override)
        payload["provider"] = infer_provider(payload.get("stream_page_url")) or payload.get(
            "provider"
        )
        merged_matches.append(MatchRecord.from_dict(payload))
    return merged_matches


def sync_schedule(config_path: Path) -> list[MatchRecord]:
    config = load_config(config_path)
    schedule_url, html = try_fetch_schedule_html(config)
    debug_path = Path(config["catalog_path"]).parent / "schedule_debug.html"
    debug_path.parent.mkdir(parents=True, exist_ok=True)
    debug_path.write_text(html, encoding="utf-8")

    soup = BeautifulSoup(html, "html.parser")
    matches = dedupe_matches(
        parse_next_data_matches(
            soup,
            schedule_url,
            config["schedule_timezone"],
            config["default_duration_minutes"],
        )
        + parse_json_ld_matches(
            soup,
            schedule_url,
            config["schedule_timezone"],
            config["default_duration_minutes"],
        )
        + parse_dom_matches(
            soup,
            schedule_url,
            config["schedule_timezone"],
            config["default_duration_minutes"],
        )
    )

    if not matches:
        raise RuntimeError(
            f"No matches were parsed from {schedule_url}. "
            f"Inspect {debug_path} and adjust the parser."
        )

    matches = merge_overrides(matches, config["match_overrides"])
    for match in matches:
        if not match.stream_page_url:
            match.stream_page_url = MANUAL_STREAM_URL_REQUIRED

    save_json(
        Path(config["catalog_path"]),
        {
            "generated_at": datetime.now().astimezone().isoformat(),
            "schedule_url": schedule_url,
            "matches": [match.to_dict() for match in matches],
        },
    )
    return matches


def load_catalog(config: dict[str, Any]) -> list[MatchRecord]:
    catalog_path = Path(config["catalog_path"])
    payload = load_json(catalog_path, {})
    matches = [MatchRecord.from_dict(item)
               for item in payload.get("matches", [])]
    return merge_overrides(matches, config["match_overrides"])


def fetch_page_links(url: str, config: dict[str, Any]) -> list[str]:
    try:
        html = fetch_html(url, config)
    except Exception:
        return []

    soup = BeautifulSoup(html, "html.parser")
    ranked_links: list[tuple[int, str]] = []
    for link in soup.find_all("a", href=True):
        absolute = urljoin(url, link["href"])
        lowered = absolute.lower()
        score = 0
        if "playsight" in lowered:
            score += 5
        if any(
            keyword in lowered
            for keyword in ("live", "stream", "watch", "video", "youtube", "youtu.be")
        ):
            score += 2
        if score:
            ranked_links.append((score, absolute))

    ranked_links.sort(reverse=True)
    deduped = list(dict.fromkeys(link for _, link in ranked_links))
    return deduped[:8]


def extract_court_label(text: str, fallback: str) -> str:
    normalized = normalize_text(text)
    match = re.search(r"\bCourt\s*0*(\d+)\b", normalized, re.IGNORECASE)
    if match:
        return f"Court {match.group(1)}"
    if "stadium" in normalized.lower():
        return "Stadium"
    return fallback


def extract_playsight_watch_pages(page_url: str, config: dict[str, Any]) -> list[tuple[str, str]]:
    try:
        html = fetch_html(page_url, config)
    except Exception:
        try:
            html = fetch_html(page_url, config, rendered=True)
        except Exception:
            return []

    soup = BeautifulSoup(html, "html.parser")
    candidates: list[tuple[str, str]] = []
    default_label = extract_court_label(
        soup.get_text(" ", strip=True), "Main Court")

    if "/live/" in urlparse(page_url).path.lower():
        candidates.append((default_label, page_url))

    seen_live_links: set[str] = set()
    for link in soup.find_all("a", href=True):
        href = urljoin(page_url, link["href"])
        lowered_href = href.lower()
        anchor_text = normalize_text(link.get_text(" ", strip=True))

        if not is_playsight_url(href):
            continue
        if "/live/" not in urlparse(href).path.lower():
            continue
        if href in seen_live_links:
            continue
        if "watch" not in anchor_text.lower() and "/facility/" not in urlparse(page_url).path.lower():
            continue

        container = link
        label = anchor_text or default_label
        for _ in range(4):
            container_text = normalize_text(
                container.get_text(" ", strip=True))
            candidate_label = extract_court_label(container_text, "")
            if candidate_label:
                label = candidate_label
                break
            parent = getattr(container, "parent", None)
            if parent is None:
                break
            container = parent

        candidates.append((label or default_label, href))
        seen_live_links.add(href)

    deduped: list[tuple[str, str]] = []
    seen_urls: set[str] = set()
    for label, href in candidates:
        if href in seen_urls:
            continue
        deduped.append((label, href))
        seen_urls.add(href)
    return deduped


def extract_media_url_with_yt_dlp(page_url: str) -> str | None:
    try:
        from yt_dlp import YoutubeDL
    except ImportError:
        return None

    try:
        with YoutubeDL({"quiet": True, "no_warnings": True}) as ydl:
            info = ydl.extract_info(page_url, download=False)
    except Exception:
        return None

    if isinstance(info, dict):
        if isinstance(info.get("url"), str):
            return info["url"]
        for format_info in info.get("formats", []):
            format_url = format_info.get("url")
            if isinstance(format_url, str) and any(
                token in format_url.lower() for token in (".m3u8", ".mpd", ".mp4")
            ):
                return format_url
    return None


def extract_media_url_with_requests(page_url: str, config: dict[str, Any]) -> str | None:
    try:
        html = fetch_html(page_url, config)
    except Exception:
        return None

    media_match = re.search(
        r"https?://[^\"' ]+\.(?:m3u8|mpd|mp4)[^\"' ]*", html)
    if media_match:
        return media_match.group(0)
    return None


def sign_into_playsight_if_needed(driver: Any, page_url: str) -> None:
    email = os.getenv("PLAYSIGHT_EMAIL")
    password = os.getenv("PLAYSIGHT_PASSWORD")
    if not email or not password:
        return

    from selenium.webdriver.common.by import By

    try:
        email_field = driver.find_element(
            By.XPATH, '//input[@type="email" and @autocomplete="email"]'
        )
        password_field = driver.find_element(
            By.XPATH, '//input[@type="password" and @autocomplete="current-password"]'
        )
        submit_button = driver.find_element(
            By.XPATH, '//button[@type="submit"]')
    except Exception:
        return

    email_field.clear()
    email_field.send_keys(email)
    password_field.clear()
    password_field.send_keys(password)
    submit_button.click()
    time.sleep(5)
    driver.get(page_url)
    time.sleep(5)


def extract_media_url_with_playsight(page_url: str) -> str | None:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service as ChromeService
    from webdriver_manager.chrome import ChromeDriverManager

    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    options.add_argument("--no-sandbox")
    options.set_capability("goog:loggingPrefs", {"performance": "ALL"})

    driver = webdriver.Chrome(
        service=ChromeService(ChromeDriverManager().install()),
        options=options,
    )
    try:
        driver.get(page_url)
        sign_into_playsight_if_needed(driver, page_url)
        deadline = time.time() + 25
        while time.time() < deadline:
            for entry in reversed(driver.get_log("performance")):
                try:
                    message = json.loads(entry["message"])
                    inner = message.get("message", {})
                    if inner.get("method") != "Network.responseReceived":
                        continue
                    response = inner.get("params", {}).get("response", {})
                    url = response.get("url", "")
                    if ".m3u8" in url or ".mpd" in url or ".mp4" in url:
                        return url
                except Exception:
                    continue
            time.sleep(2)
    finally:
        driver.quit()
    return None


def uniquify_stream_targets(targets: list[StreamTarget]) -> list[StreamTarget]:
    counts: dict[str, int] = {}
    unique_targets: list[StreamTarget] = []
    for target in targets:
        base_label = normalize_text(target.label) or "Court"
        counts[base_label] = counts.get(base_label, 0) + 1
        label = base_label
        if counts[base_label] > 1:
            label = f"{base_label} {counts[base_label]}"
        unique_targets.append(
            StreamTarget(label=label, page_url=target.page_url,
                         media_url=target.media_url)
        )
    return unique_targets


def resolve_stream_targets(page_url: str, config: dict[str, Any]) -> list[StreamTarget]:
    if not page_url or page_url == MANUAL_STREAM_URL_REQUIRED:
        return []

    if page_url.lower().endswith(DIRECT_STREAM_SUFFIXES):
        return [StreamTarget(label="Main Court", page_url=page_url, media_url=page_url)]

    if is_playsight_facility_or_multi_page(page_url):
        watch_pages = extract_playsight_watch_pages(page_url, config)
        if watch_pages:
            resolved_targets: list[StreamTarget] = []
            for label, watch_page in watch_pages:
                media_url = resolve_stream_url(watch_page, config, depth=1)
                if media_url:
                    resolved_targets.append(
                        StreamTarget(
                            label=label, page_url=watch_page, media_url=media_url)
                    )
            if resolved_targets:
                return uniquify_stream_targets(resolved_targets)

    resolved = resolve_stream_url(page_url, config)
    if not resolved:
        return []

    label = "Main Court"
    if is_playsight_url(page_url):
        label = extract_court_label(page_url, "Main Court")
    return [StreamTarget(label=label, page_url=page_url, media_url=resolved)]


def resolve_stream_url(page_url: str, config: dict[str, Any], depth: int = 0) -> str | None:
    if not page_url or page_url == MANUAL_STREAM_URL_REQUIRED or depth > 2:
        return None

    lowered = page_url.lower()
    if lowered.endswith(DIRECT_STREAM_SUFFIXES):
        return page_url

    if is_playsight_url(page_url) and "/live/" in urlparse(page_url).path.lower():
        resolved = extract_media_url_with_playsight(page_url)
        if resolved:
            return resolved

    embedded_links = fetch_page_links(page_url, config)
    for embedded_link in embedded_links:
        resolved = resolve_stream_url(embedded_link, config, depth + 1)
        if resolved:
            return resolved

    for resolver in (
        lambda: extract_media_url_with_requests(page_url, config),
        lambda: extract_media_url_with_yt_dlp(page_url),
    ):
        resolved = resolver()
        if resolved:
            return resolved
    if "playsight" in lowered:
        return extract_media_url_with_playsight(page_url)
    return None


def build_output_path(
    match: MatchRecord, config: dict[str, Any], stream_label: str | None = None
) -> Path:
    start_dt = match.start_datetime()
    base_dir = Path(config["output_root"]) / start_dt.strftime("%Y-%m-%d")
    match_dir = base_dir / safe_filename(match.title)
    match_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{start_dt.strftime('%Y%m%d_%H%M')}_{safe_filename(match.title)}"
    if stream_label:
        filename = f"{filename}_{safe_filename(stream_label)}"
    filename = f"{filename}.mp4"
    return match_dir / filename


def wait_for_live_streams(match: MatchRecord, config: dict[str, Any]) -> list[StreamTarget]:
    now = datetime.now(ZoneInfo(match.timezone))
    start_dt = match.start_datetime().astimezone(ZoneInfo(match.timezone))
    window_start = start_dt - timedelta(minutes=config["lead_time_minutes"])
    if now < window_start:
        seconds_until_start = (window_start - now).total_seconds()
        if seconds_until_start > 0:
            time.sleep(seconds_until_start)

    deadline = start_dt + timedelta(minutes=config["stream_retry_minutes"])
    while datetime.now(ZoneInfo(match.timezone)) <= deadline:
        resolved_targets = resolve_stream_targets(
            match.stream_page_url or "", config)
        if resolved_targets:
            return resolved_targets
        time.sleep(config["poll_interval_seconds"])

    raise RuntimeError(
        f"Unable to resolve a live stream for {match.match_id} before {deadline.isoformat()}."
    )


def build_ffmpeg_command(
    media_url: str, output_path: Path, duration_minutes: int, config: dict[str, Any]
) -> list[str]:
    duration_seconds = duration_minutes * 60
    return [
        config["ffmpeg_path"],
        "-y",
        "-i",
        media_url,
        "-t",
        str(duration_seconds),
        "-c",
        "copy",
        "-movflags",
        "+faststart",
        str(output_path),
    ]


def record_stream_targets(
    match: MatchRecord, targets: list[StreamTarget], config: dict[str, Any]
) -> list[tuple[StreamTarget, Path]]:
    recordings: list[tuple[StreamTarget, Path, subprocess.Popen[str]]] = []
    multiple_targets = len(targets) > 1

    for target in targets:
        output_path = build_output_path(
            match,
            config,
            stream_label=target.label if multiple_targets else None,
        )
        command = build_ffmpeg_command(
            target.media_url,
            output_path,
            match.duration_minutes,
            config,
        )
        process = subprocess.Popen(command)
        recordings.append((target, output_path, process))

    failures: list[str] = []
    completed: list[tuple[StreamTarget, Path]] = []
    for target, output_path, process in recordings:
        if process.wait() != 0:
            failures.append(target.label)
        else:
            completed.append((target, output_path))

    if failures:
        raise RuntimeError(f"ffmpeg failed for: {', '.join(failures)}")
    return completed


def maybe_upload_to_youtube(
    match: MatchRecord,
    output_path: Path,
    config: dict[str, Any],
    stream_label: str | None = None,
) -> None:
    youtube_config = config["youtube"]
    if not youtube_config["enabled"]:
        return

    from youtube_uploader import upload_video

    title = youtube_config["title_template"].format(
        match_id=match.match_id,
        match_title=match.title,
        start_iso=match.start_iso,
        start_date=match.start_datetime().date().isoformat(),
        schedule_url=match.schedule_url,
        stream_page_url=match.stream_page_url or "",
        output_path=str(output_path),
    )
    if stream_label:
        title = f"{title} | {stream_label}"

    context = {
        "match_id": match.match_id,
        "match_title": match.title,
        "start_iso": match.start_iso,
        "start_date": match.start_datetime().date().isoformat(),
        "schedule_url": match.schedule_url,
        "stream_page_url": match.stream_page_url or "",
        "output_path": str(output_path),
    }
    upload_video(
        file_path=output_path,
        title=title,
        description=youtube_config["description_template"].format(**context),
        client_secrets_file=Path(youtube_config["client_secrets_file"]),
        token_file=Path(youtube_config["token_file"]),
        privacy_status=youtube_config["privacy_status"],
        category_id=youtube_config["category_id"],
        tags=list(youtube_config.get("tags", [])),
    )


def record_match(config_path: Path, match_id: str) -> list[Path]:
    config = load_config(config_path)
    matches = load_catalog(config)
    if not matches:
        matches = sync_schedule(config_path)

    selected = next(
        (match for match in matches if match.match_id == match_id), None)
    if selected is None:
        raise KeyError(f"Unknown match id: {match_id}")
    if not selected.stream_page_url or selected.stream_page_url == MANUAL_STREAM_URL_REQUIRED:
        raise RuntimeError(
            f"{match_id} still needs a stream page URL. Add it under match_overrides in the config."
        )

    stream_targets = wait_for_live_streams(selected, config)
    recordings = record_stream_targets(selected, stream_targets, config)
    output_paths: list[Path] = []
    for target, output_path in recordings:
        maybe_upload_to_youtube(
            selected,
            output_path,
            config,
            stream_label=target.label if len(recordings) > 1 else None,
        )
        output_paths.append(output_path)
    return output_paths


def launchd_label(match: MatchRecord) -> str:
    return f"com.psmatchrecorder.{slugify(match.match_id)}"


def generate_launchd_plists(config_path: Path) -> list[Path]:
    config = load_config(config_path)
    matches = load_catalog(config)
    if not matches:
        matches = sync_schedule(config_path)

    output_dir = Path(config["launchd_output_dir"])
    logs_dir = Path(config["logs_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)

    local_zone = datetime.now().astimezone().tzinfo
    generated: list[Path] = []

    for match in matches:
        if not match.stream_page_url or match.stream_page_url == MANUAL_STREAM_URL_REQUIRED:
            continue

        run_time = (
            match.start_datetime().astimezone(local_zone)
            - timedelta(minutes=config["lead_time_minutes"])
        )
        if run_time <= datetime.now().astimezone():
            continue

        label = launchd_label(match)
        plist_path = output_dir / f"{label}.plist"
        plist_payload = {
            "Label": label,
            "ProgramArguments": [
                sys.executable,
                str(Path(__file__).resolve()),
                "record",
                "--config",
                str(config_path.resolve()),
                "--match-id",
                match.match_id,
            ],
            "StartCalendarInterval": {
                "Year": run_time.year,
                "Month": run_time.month,
                "Day": run_time.day,
                "Hour": run_time.hour,
                "Minute": run_time.minute,
            },
            "StandardOutPath": str(logs_dir / f"{label}.out.log"),
            "StandardErrorPath": str(logs_dir / f"{label}.err.log"),
            "WorkingDirectory": str(Path(__file__).resolve().parent),
        }
        with plist_path.open("wb") as handle:
            plistlib.dump(plist_payload, handle)
        generated.append(plist_path)

    return generated


def print_match_summary(matches: list[MatchRecord]) -> None:
    unresolved = 0
    for match in matches:
        stream_value = match.stream_page_url or MANUAL_STREAM_URL_REQUIRED
        if stream_value == MANUAL_STREAM_URL_REQUIRED:
            unresolved += 1
        print(f"{match.match_id} | {match.start_iso} | {match.title} | {stream_value}")
    print(
        f"\nParsed {len(matches)} matches. {unresolved} still need a stream URL override.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Big Ten men's tennis schedule parser, recorder, and scheduler."
    )
    parser.add_argument(
        "--config",
        default="config/bigten_recorder.json",
        help="Path to the recorder config JSON file.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser(
        "sync", help="Fetch the conference schedule and build the local catalog.")
    subparsers.add_parser(
        "list", help="Print the current local catalog. Runs sync first if the catalog is missing."
    )

    record_parser = subparsers.add_parser(
        "record", help="Record a single match by match id.")
    record_parser.add_argument("--match-id", required=True)

    subparsers.add_parser(
        "schedule-jobs",
        help="Generate launchd plists for future matches with resolved stream URLs.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config_path = Path(args.config).resolve()

    try:
        if args.command == "sync":
            matches = sync_schedule(config_path)
            print_match_summary(matches)
            return 0

        if args.command == "list":
            config = load_config(config_path)
            matches = load_catalog(config)
            if not matches:
                matches = sync_schedule(config_path)
            print_match_summary(matches)
            return 0

        if args.command == "record":
            output_paths = record_match(config_path, args.match_id)
            for output_path in output_paths:
                print(f"Recording saved to {output_path}")
            return 0

        if args.command == "schedule-jobs":
            generated = generate_launchd_plists(config_path)
            if not generated:
                print("No future matches with resolved stream links were available.")
                return 0
            for plist_path in generated:
                print(plist_path)
            return 0
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
