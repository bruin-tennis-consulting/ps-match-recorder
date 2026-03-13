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
from urllib.parse import parse_qs, urljoin, urlparse
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
NON_PAGE_SUFFIXES = (
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".svg",
    ".ico",
    ".css",
    ".js",
    ".json",
    ".webmanifest",
    ".xml",
)
PLAYSIGHT_LINK_PATTERN = re.compile(
    r"(?:https?:)?//[^\s\"'<>]*playsight\.com/(?:live|livestreaming|facility)/[^\s\"'<>]+",
    re.IGNORECASE,
)
URL_ATTRIBUTE_KEYS = ("href", "src", "data-src", "data-href", "data-url")
GENERIC_URL_PATTERN = re.compile(r"(?:https?:)?//[^\s\"'<>]+", re.IGNORECASE)
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


def log_status(message: str) -> None:
    timestamp = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
    print(f"[{timestamp}] {message}", flush=True)


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


def extract_urls_from_text(value: str) -> list[str]:
    if not value:
        return []

    normalized = value.replace("\\/", "/")
    urls: list[str] = []
    for raw in GENERIC_URL_PATTERN.findall(normalized):
        cleaned = raw.strip(" \t\r\n'\"),;\\")
        if cleaned.startswith("//"):
            cleaned = f"https:{cleaned}"
        if cleaned:
            urls.append(cleaned)
    return urls


def parse_url_or_none(value: str | None) -> Any | None:
    if not value:
        return None
    try:
        return urlparse(value)
    except ValueError:
        return None


def join_url_or_none(base_url: str, candidate_url: str) -> str | None:
    try:
        return urljoin(base_url, candidate_url)
    except ValueError:
        return None


def is_navigable_page_url(url: str) -> bool:
    parsed = parse_url_or_none(url)
    if parsed is None or parsed.scheme not in {"http", "https"}:
        return False
    path = parsed.path.lower()
    return not path.endswith(NON_PAGE_SUFFIXES)


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
    parsed = parse_url_or_none(url)
    if parsed is None:
        return None
    host = parsed.netloc.lower()
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
    parsed = parse_url_or_none(url)
    if parsed is None:
        return False
    return "playsight.com" in parsed.netloc.lower()


def is_track_tennis_url(url: str | None) -> bool:
    if not url:
        return False
    parsed = parse_url_or_none(url)
    if parsed is None:
        return False
    return "track.tennis" in parsed.netloc.lower()


def is_nusports_url(url: str | None) -> bool:
    if not url:
        return False
    parsed = parse_url_or_none(url)
    if parsed is None:
        return False
    return "nusports.com" in parsed.netloc.lower()


def is_nusports_watch_url(url: str | None) -> bool:
    if not is_nusports_url(url):
        return False
    parsed = parse_url_or_none(url or "")
    if parsed is None:
        return False
    return parsed.path.lower().startswith("/watch")


def is_fightingillini_url(url: str | None) -> bool:
    if not url:
        return False
    parsed = parse_url_or_none(url)
    if parsed is None:
        return False
    return "fightingillini.com" in parsed.netloc.lower()


def is_illinois_mten_stream_hub_url(url: str | None) -> bool:
    if not is_fightingillini_url(url):
        return False
    parsed = parse_url_or_none(url or "")
    if parsed is None:
        return False
    return "mtennis_livestatsvideo" in parsed.path.lower()


def is_fightingillini_watch_url(url: str | None) -> bool:
    if not is_fightingillini_url(url):
        return False
    parsed = parse_url_or_none(url or "")
    if parsed is None:
        return False
    return parsed.path.lower().startswith("/watch")


def is_fightingillini_embed_url(url: str | None) -> bool:
    if not is_fightingillini_url(url):
        return False
    parsed = parse_url_or_none(url or "")
    if parsed is None:
        return False
    return parsed.path.lower().endswith("/showcase/embed.aspx")


def extract_live_query_id(url: str | None) -> str | None:
    parsed = parse_url_or_none(url)
    if parsed is None:
        return None
    try:
        query = parse_qs(parsed.query)
    except Exception:
        return None
    values = query.get("Live") or query.get("live")
    if not values:
        return None
    value = normalize_text(values[0])
    return value or None


def build_fightingillini_watch_url(live_id: str) -> str:
    return f"https://fightingillini.com/watch/?Live={live_id}&type=Live"


def build_fightingillini_embed_url(live_id: str) -> str:
    return f"https://fightingillini.com/showcase/embed.aspx?Live={live_id}"


def is_playsight_intent_url(url: str | None) -> bool:
    if not url:
        return False
    if is_playsight_url(url):
        return True

    parsed = parse_url_or_none(url)
    if parsed is None:
        return False
    host = parsed.netloc.lower()
    if "youtube.com" in host or "youtu.be" in host:
        return False
    return "playsight" in url.lower()


def is_playsight_facility_or_multi_page(url: str | None) -> bool:
    if not is_playsight_url(url):
        return False
    parsed = parse_url_or_none(url or "")
    if parsed is None:
        return False
    path = parsed.path.lower()
    return "/facility/" in path or "/live/" in path or "/livestreaming/" in path


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


def build_chrome_options(enable_performance_logs: bool = False) -> Any:
    from selenium.webdriver.chrome.options import Options

    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    options.add_argument("--no-sandbox")
    if enable_performance_logs:
        options.set_capability("goog:loggingPrefs", {"performance": "ALL"})
    return options


def create_chrome_driver(options: Any) -> Any:
    from selenium import webdriver

    startup_errors: list[str] = []
    try:
        return webdriver.Chrome(options=options)
    except Exception as exc:
        startup_errors.append(f"selenium-manager: {exc}")

    try:
        from selenium.webdriver.chrome.service import Service as ChromeService
        from webdriver_manager.chrome import ChromeDriverManager

        return webdriver.Chrome(
            service=ChromeService(ChromeDriverManager().install()),
            options=options,
        )
    except Exception as exc:
        startup_errors.append(f"webdriver-manager: {exc}")
        raise RuntimeError(
            "Unable to start Chrome WebDriver. " + " | ".join(startup_errors)
        ) from exc


def fetch_html_with_selenium(url: str) -> str:
    options = build_chrome_options(enable_performance_logs=False)
    driver = create_chrome_driver(options)

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
    parsed_base = parse_url_or_none(url)
    if parsed_base is None or parsed_base.scheme not in {"http", "https"}:
        return []

    try:
        html = fetch_html(url, config)
    except Exception:
        return []

    soup = BeautifulSoup(html, "html.parser")
    ranked_links: list[tuple[int, str]] = []
    seen_links: set[str] = set()

    def maybe_add(candidate: str) -> None:
        absolute = join_url_or_none(url, candidate.strip())
        if not absolute:
            return
        if absolute in seen_links:
            return
        if not is_navigable_page_url(absolute):
            return
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
            seen_links.add(absolute)

    for tag in soup.find_all(True):
        for attr, value in tag.attrs.items():
            if not isinstance(value, str):
                continue
            if attr in URL_ATTRIBUTE_KEYS:
                maybe_add(value)
            if "playsight" in value.lower():
                for discovered_url in extract_urls_from_text(value):
                    maybe_add(discovered_url)

    for match in PLAYSIGHT_LINK_PATTERN.findall(html):
        maybe_add(match)
    for discovered_url in extract_urls_from_text(html):
        maybe_add(discovered_url)

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


def extract_playsight_candidates(
    page_url: str, config: dict[str, Any]
) -> list[tuple[str, str]]:
    candidates: list[tuple[str, str]] = []
    seen_urls: set[str] = set()
    has_livestream_candidate = False

    def add_candidate(raw_url: str, label_hint: str = "", default_label: str = "Main Court") -> None:
        nonlocal has_livestream_candidate
        absolute = join_url_or_none(page_url, raw_url.strip())
        if not absolute:
            return
        absolute = absolute.strip().rstrip("\\")
        if not absolute or absolute in seen_urls:
            return
        if not is_navigable_page_url(absolute):
            return
        if not is_playsight_url(absolute):
            return
        parsed = parse_url_or_none(absolute)
        if parsed is None:
            return
        path = parsed.path.lower()
        if (
            "/live/" not in path
            and "/livestreaming/" not in path
            and "/facility/" not in path
        ):
            return
        if "/live/" in path or "/livestreaming/" in path:
            has_livestream_candidate = True

        normalized_hint = normalize_text(label_hint)
        label = extract_court_label(normalized_hint, normalized_hint)
        if not label:
            label = extract_court_label(absolute, default_label)

        candidates.append((label or default_label, absolute))
        seen_urls.add(absolute)

    def collect_from_html(html: str, allow_full_scan: bool) -> None:
        soup = BeautifulSoup(html, "html.parser")
        default_label = extract_court_label(soup.get_text(" ", strip=True), "Main Court")

        def build_label_hint(tag: Any) -> str:
            hint = normalize_text(
                " ".join(
                    value
                    for value in (
                        tag.get_text(" ", strip=True),
                        tag.get("title"),
                        tag.get("aria-label"),
                        tag.get("data-label"),
                        tag.get("alt"),
                    )
                    if isinstance(value, str) and normalize_text(value)
                )
            )
            if hint:
                return hint

            current = getattr(tag, "parent", None)
            for _ in range(5):
                if current is None:
                    break
                snippet = normalize_text(current.get_text(" ", strip=True))
                match = re.search(r"\bCourt\s*0*(\d+)\b", snippet, re.IGNORECASE)
                if match:
                    return f"Court {match.group(1)}"
                current = getattr(current, "parent", None)
            return ""

        for tag in soup.find_all(True):
            label_hint = build_label_hint(tag)
            for attribute_key, attribute_value in tag.attrs.items():
                if not isinstance(attribute_value, str):
                    continue
                if attribute_key in URL_ATTRIBUTE_KEYS:
                    add_candidate(attribute_value, label_hint, default_label)
                if "playsight" in attribute_value.lower():
                    for discovered_url in extract_urls_from_text(attribute_value):
                        add_candidate(discovered_url, label_hint, default_label)

        for matched_url in PLAYSIGHT_LINK_PATTERN.findall(html):
            add_candidate(matched_url, matched_url, default_label)
        if allow_full_scan:
            for discovered_url in extract_urls_from_text(html):
                if "playsight" in discovered_url.lower():
                    add_candidate(discovered_url, discovered_url, default_label)

    fetched_html = None
    try:
        fetched_html = fetch_html(page_url, config)
    except Exception:
        pass

    if fetched_html:
        collect_from_html(
            fetched_html,
            allow_full_scan=not is_playsight_url(page_url),
        )
        # Some school sites hydrate stream iframes client-side; retry with rendered HTML
        # when static HTML produced no PlaySight candidates.
        if not candidates and not is_playsight_url(page_url):
            try:
                rendered_html = fetch_html(page_url, config, rendered=True)
                collect_from_html(rendered_html, allow_full_scan=True)
            except Exception:
                pass
    else:
        try:
            rendered_html = fetch_html(page_url, config, rendered=True)
        except Exception:
            return []
        collect_from_html(
            rendered_html,
            allow_full_scan=not is_playsight_url(page_url),
        )
        return candidates

    if is_playsight_url(page_url) and not has_livestream_candidate:
        try:
            rendered_html = fetch_html(page_url, config, rendered=True)
            collect_from_html(rendered_html, allow_full_scan=False)
        except Exception:
            pass

    return candidates


def extract_embedded_playsight_facility_pages(
    page_url: str, config: dict[str, Any]
) -> list[str]:
    facility_pages: list[str] = []
    seen_urls: set[str] = set()
    for _, candidate_url in extract_playsight_candidates(page_url, config):
        parsed = parse_url_or_none(candidate_url)
        if parsed is None:
            continue
        if "/facility/" not in parsed.path.lower():
            continue
        if candidate_url in seen_urls:
            continue
        facility_pages.append(candidate_url)
        seen_urls.add(candidate_url)
    return facility_pages


def extract_playsight_watch_pages_with_selenium(page_url: str) -> list[tuple[str, str]]:
    from selenium.webdriver.common.by import By

    options = build_chrome_options(enable_performance_logs=False)
    try:
        driver = create_chrome_driver(options)
    except Exception as exc:
        log_status(f"Failed to start Chrome WebDriver for PlaySight watch scraping: {exc}")
        return []

    candidates: list[tuple[str, str]] = []
    seen_urls: set[str] = set()
    try:
        driver.get(page_url)
        time.sleep(8)

        watch_controls = driver.find_elements(
            By.XPATH,
            (
                '//a[contains(translate(normalize-space(.), "ABCDEFGHIJKLMNOPQRSTUVWXYZ", '
                '"abcdefghijklmnopqrstuvwxyz"), "watch")]'
                ' | '
                '//button[contains(translate(normalize-space(.), "ABCDEFGHIJKLMNOPQRSTUVWXYZ", '
                '"abcdefghijklmnopqrstuvwxyz"), "watch")]'
            ),
        )

        for control in watch_controls:
            href = (
                control.get_attribute("href")
                or control.get_attribute("data-href")
                or ""
            )
            if not href:
                onclick = control.get_attribute("onclick") or ""
                discovered = extract_urls_from_text(onclick)
                if discovered:
                    href = discovered[0]
            if not href:
                continue

            absolute = join_url_or_none(page_url, href)
            if not absolute or absolute in seen_urls:
                continue
            if not is_playsight_url(absolute):
                continue
            parsed = parse_url_or_none(absolute)
            if parsed is None:
                continue
            path = parsed.path.lower()
            if "/live/" not in path and "/livestreaming/" not in path:
                continue

            label = "Watch"
            container = control
            for _ in range(6):
                container_text = normalize_text(container.text)
                if container_text:
                    label = extract_court_label(container_text, label)
                    smartcourt_match = re.search(
                        r"SmartCourt\s*0*(\d+)",
                        container_text,
                        re.IGNORECASE,
                    )
                    if smartcourt_match:
                        label = f"Court {smartcourt_match.group(1)}"
                        break
                try:
                    container = container.find_element(By.XPATH, "..")
                except Exception:
                    break

            candidates.append((label, absolute))
            seen_urls.add(absolute)
    finally:
        driver.quit()

    return candidates


def extract_playsight_watch_pages(page_url: str, config: dict[str, Any]) -> list[tuple[str, str]]:
    candidates: list[tuple[str, str]] = []
    default_label = "Main Court"

    parsed_page = parse_url_or_none(page_url)
    if parsed_page and "/live/" in parsed_page.path.lower():
        candidates.append((default_label, page_url))

    for label, href in extract_playsight_candidates(page_url, config):
        parsed_href = parse_url_or_none(href)
        if parsed_href is None:
            continue
        href_path = parsed_href.path.lower()
        if "/live/" not in href_path and "/livestreaming/" not in href_path:
            continue
        candidates.append((label or default_label, href))

    deduped: list[tuple[str, str]] = []
    seen_urls: set[str] = set()
    for label, href in candidates:
        if href in seen_urls:
            continue
        deduped.append((label, href))
        seen_urls.add(href)

    parsed_page = parse_url_or_none(page_url)
    if (
        not deduped
        and parsed_page is not None
        and is_playsight_url(page_url)
        and "/facility/" in parsed_page.path.lower()
    ):
        selenium_candidates = extract_playsight_watch_pages_with_selenium(page_url)
        for label, href in selenium_candidates:
            if href in seen_urls:
                continue
            deduped.append((label, href))
            seen_urls.add(href)
    return deduped


def extract_nusports_event_pages_with_selenium(
    page_url: str, event_title_filters: list[str] | None = None
) -> list[tuple[str, str]]:
    from selenium.webdriver.common.by import By

    event_title_filters = [token.lower() for token in (event_title_filters or [])]
    options = build_chrome_options(enable_performance_logs=False)
    try:
        driver = create_chrome_driver(options)
    except Exception as exc:
        log_status(f"Failed to start Chrome WebDriver for Northwestern watch scraping: {exc}")
        return []

    candidates: list[tuple[str, str]] = []
    seen_urls: set[str] = set()

    def should_keep_event(text: str) -> bool:
        normalized = normalize_text(text).lower()
        if "mten vs" not in normalized:
            return False
        if not event_title_filters:
            return True
        return any(token in normalized for token in event_title_filters)

    def extract_label(text: str, fallback_index: int) -> str:
        normalized = normalize_text(text)
        court_match = re.search(r"Court\s*0*(\d+)", normalized, re.IGNORECASE)
        if court_match:
            return f"Court {court_match.group(1)}"
        return f"Court {fallback_index}"

    try:
        log_status(f"Opening Northwestern watch page: {page_url}")
        driver.get(page_url)
        time.sleep(8)

        show_all_locators = [
            (By.XPATH, '//a[contains(translate(normalize-space(.), "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"), "show all")]'),
            (By.XPATH, '//button[contains(translate(normalize-space(.), "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"), "show all")]'),
        ]
        for by, selector in show_all_locators:
            try:
                controls = driver.find_elements(by, selector)
            except Exception:
                continue
            if not controls:
                continue
            try:
                driver.execute_script("arguments[0].click();", controls[0])
                time.sleep(2)
                break
            except Exception:
                continue

        anchor_cards = driver.find_elements(
            By.XPATH,
            (
                '//a['
                './/*[contains(translate(normalize-space(.), "ABCDEFGHIJKLMNOPQRSTUVWXYZ", '
                '"abcdefghijklmnopqrstuvwxyz"), "mten vs")]'
                ']'
            ),
        )

        fallback_nodes = []
        if not anchor_cards:
            fallback_nodes = driver.find_elements(
                By.XPATH,
                (
                    '//*[contains(translate(normalize-space(.), "ABCDEFGHIJKLMNOPQRSTUVWXYZ", '
                    '"abcdefghijklmnopqrstuvwxyz"), "mten vs")]'
                ),
            )

        entries = anchor_cards if anchor_cards else fallback_nodes
        for index, entry in enumerate(entries, start=1):
            text = normalize_text(entry.text)
            if not should_keep_event(text):
                continue

            href = (
                entry.get_attribute("href")
                or entry.get_attribute("data-href")
                or ""
            )

            current = entry
            for _ in range(6):
                if href:
                    break
                onclick = current.get_attribute("onclick") or ""
                if onclick:
                    discovered = extract_urls_from_text(onclick)
                    if discovered:
                        href = discovered[0]
                        break
                try:
                    current = current.find_element(By.XPATH, "..")
                except Exception:
                    break
                href = (
                    current.get_attribute("href")
                    or current.get_attribute("data-href")
                    or ""
                )

            if not href:
                continue

            absolute = join_url_or_none(page_url, href)
            if not absolute or absolute in seen_urls:
                continue
            if not is_navigable_page_url(absolute):
                continue

            candidates.append((extract_label(text, index), absolute))
            seen_urls.add(absolute)
    finally:
        driver.quit()

    return candidates


def extract_fightingillini_watch_pages(
    page_url: str, config: dict[str, Any]
) -> list[tuple[str, str]]:
    try:
        html = fetch_html(page_url, config)
    except Exception:
        try:
            html = fetch_html(page_url, config, rendered=True)
        except Exception:
            return []

    soup = BeautifulSoup(html, "html.parser")
    candidates: list[tuple[int, str, str]] = []
    seen_watch_urls: set[str] = set()

    def court_label_from_tag(tag: Any, fallback_index: int) -> tuple[int, str]:
        current = tag
        for _ in range(5):
            if current is None:
                break
            snippet = normalize_text(current.get_text(" ", strip=True))
            match = re.search(r"Court\s*0*(\d+)", snippet, re.IGNORECASE)
            if match:
                court_number = int(match.group(1))
                return court_number, f"Court {court_number}"
            current = getattr(current, "parent", None)
        return fallback_index, f"Court {fallback_index}"

    def maybe_add_candidate(source_tag: Any, raw_url: str, fallback_index: int) -> None:
        absolute = join_url_or_none(page_url, raw_url)
        if not absolute:
            return
        live_id = extract_live_query_id(absolute)
        if not live_id:
            return
        watch_url = build_fightingillini_watch_url(live_id)
        if watch_url in seen_watch_urls:
            return
        court_number, label = court_label_from_tag(source_tag, fallback_index)
        candidates.append((court_number, label, watch_url))
        seen_watch_urls.add(watch_url)

    fallback_index = 1
    for anchor in soup.find_all("a"):
        href = normalize_text(anchor.get("href") or "")
        if not href:
            continue
        text = normalize_text(anchor.get_text(" ", strip=True)).lower()
        href_lower = href.lower()
        if (
            "full screen" not in text
            and "/watch/?live=" not in href_lower
            and "/showcase/embed.aspx?live=" not in href_lower
        ):
            continue
        maybe_add_candidate(anchor, href, fallback_index)
        fallback_index += 1

    for iframe in soup.find_all("iframe"):
        src = normalize_text(iframe.get("src") or iframe.get("data-src") or "")
        if "/showcase/embed.aspx?live=" not in src.lower():
            continue
        maybe_add_candidate(iframe, src, fallback_index)
        fallback_index += 1

    candidates.sort(key=lambda item: (item[0], item[2]))
    if not candidates:
        return []

    max_court_number = max(court_number for court_number, _, _ in candidates)
    next_court_number = max_court_number + 1
    seen_court_numbers: dict[int, int] = {}
    normalized_candidates: list[tuple[str, str]] = []
    for court_number, _, watch_url in candidates:
        seen_court_numbers[court_number] = seen_court_numbers.get(court_number, 0) + 1
        if seen_court_numbers[court_number] == 1:
            final_court_number = court_number
        else:
            final_court_number = next_court_number
            next_court_number += 1
        normalized_candidates.append((f"Court {final_court_number}", watch_url))

    return normalized_candidates


def extract_embedded_stream_targets(
    page_url: str, config: dict[str, Any]
) -> list[tuple[str, str]]:
    candidates: list[tuple[str, str]] = []
    candidate_index_by_key: dict[str, int] = {}

    def playsight_livestream_key(url: str) -> str:
        parsed = parse_url_or_none(url)
        if parsed is None:
            return url
        match = re.search(r"/livestreaming/([a-z0-9]+)/", parsed.path, re.IGNORECASE)
        if match:
            return f"playsight-livestream:{match.group(1).lower()}"
        return url

    def is_court_label(label: str) -> bool:
        return re.search(r"\bCourt\s*0*\d+\b", normalize_text(label), re.IGNORECASE) is not None

    def should_replace_label(current: str, candidate: str) -> bool:
        current_text = normalize_text(current).lower()
        candidate_text = normalize_text(candidate).lower()
        if is_court_label(candidate) and not is_court_label(current):
            return True
        if "full screen" in current_text and "full screen" not in candidate_text:
            return True
        return False

    for label, href in extract_playsight_candidates(page_url, config):
        parsed_href = parse_url_or_none(href)
        if parsed_href is None:
            continue
        path = parsed_href.path.lower()
        if "/live/" not in path and "/livestreaming/" not in path:
            continue
        key = playsight_livestream_key(href)
        existing_index = candidate_index_by_key.get(key)
        if existing_index is not None:
            existing_label, existing_href = candidates[existing_index]
            if should_replace_label(existing_label, label):
                candidates[existing_index] = (label, href)
            continue
        candidate_index_by_key[key] = len(candidates)
        candidates.append((label, href))

    return candidates


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


def fightingillini_watch_page_unavailable(page_url: str, config: dict[str, Any]) -> bool:
    try:
        response = requests.get(
            page_url,
            timeout=min(10, config["request_timeout_seconds"]),
            allow_redirects=True,
        )
    except Exception:
        return False

    body_present = bool(normalize_text(response.text))
    if response.status_code in {404, 410}:
        log_status(
            f"Illinois Full Screen page not live yet ({response.status_code}): {page_url}"
        )
        return True
    if response.status_code >= 400 and not body_present:
        log_status(
            "Illinois Full Screen page returned no content "
            f"({response.status_code}): {page_url}"
        )
        return True
    return False


def sign_into_playsight_if_needed(driver: Any, page_url: str) -> None:
    skip_login = os.getenv("PLAYSIGHT_SKIP_LOGIN", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    if skip_login:
        log_status("PLAYSIGHT_SKIP_LOGIN is set. Skipping Playsight login attempt.")
        return

    email = os.getenv("PLAYSIGHT_EMAIL")
    password = os.getenv("PLAYSIGHT_PASSWORD")
    if not email or not password:
        log_status("No Playsight credentials in environment. Continuing without login.")
        return

    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait

    wait = WebDriverWait(driver, 2)

    def find_login_form_fields() -> tuple[Any, Any, Any] | None:
        email_locators = [
            (By.XPATH, '//input[@type="email" and @autocomplete="email"]'),
            (By.XPATH, '//input[@type="email"]'),
            (By.XPATH, '//input[contains(@name, "email")]'),
        ]
        password_locators = [
            (By.XPATH, '//input[@type="password" and @autocomplete="current-password"]'),
            (By.XPATH, '//input[@type="password"]'),
        ]
        submit_locators = [
            (By.XPATH, '//button[@type="submit"]'),
            (By.XPATH, '//button[contains(translate(normalize-space(.), "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"), "sign in")]'),
            (By.XPATH, '//button[contains(translate(normalize-space(.), "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"), "log in")]'),
        ]

        def first_present(locators: list[tuple[Any, str]]) -> Any | None:
            for locator in locators:
                try:
                    elements = driver.find_elements(*locator)
                    if elements:
                        return elements[0]
                except Exception:
                    continue
            return None

        def first_clickable(locators: list[tuple[Any, str]]) -> Any | None:
            for locator in locators:
                try:
                    return wait.until(EC.element_to_be_clickable(locator))
                except Exception:
                    continue
            return None

        email_field = first_present(email_locators)
        password_field = first_present(password_locators)
        submit_button = first_clickable(submit_locators)

        if email_field and password_field and submit_button:
            return email_field, password_field, submit_button
        return None

    login_fields = find_login_form_fields()
    if login_fields is None:
        sign_in_locators = [
            (By.XPATH, '//a[contains(translate(normalize-space(.), "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"), "sign in")]'),
            (By.XPATH, '//button[contains(translate(normalize-space(.), "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"), "sign in")]'),
            (By.XPATH, '//a[contains(translate(normalize-space(.), "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"), "log in")]'),
            (By.XPATH, '//button[contains(translate(normalize-space(.), "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"), "log in")]'),
            (By.XPATH, '//a[contains(@href, "/auth")]'),
            (By.XPATH, '//button[contains(@aria-label, "sign in")]'),
        ]

        clicked_sign_in = False
        for locator in sign_in_locators:
            try:
                sign_in_button = wait.until(EC.element_to_be_clickable(locator))
                log_status(f"Clicking Playsight sign-in control on {page_url}")
                driver.execute_script("arguments[0].click();", sign_in_button)
                clicked_sign_in = True
                break
            except Exception:
                continue

        if clicked_sign_in:
            time.sleep(2)
            login_fields = find_login_form_fields()

    if login_fields is None:
        log_status("No Playsight login form detected on page.")
        return

    email_field, password_field, submit_button = login_fields

    log_status(f"Signing into Playsight for {page_url}")
    email_field.clear()
    email_field.send_keys(email)
    password_field.clear()
    password_field.send_keys(password)
    submit_button.click()
    time.sleep(5)
    driver.get(page_url)
    time.sleep(5)
    log_status("Playsight sign-in submitted.")


def extract_media_url_from_performance_entries(entries: list[dict[str, Any]]) -> str | None:
    for entry in reversed(entries):
        try:
            message = json.loads(entry["message"])
            inner = message.get("message", {})
            method = inner.get("method", "")
            params = inner.get("params", {})
            url = ""
            if method == "Network.responseReceived":
                url = params.get("response", {}).get("url", "")
            elif method == "Network.requestWillBeSent":
                url = params.get("request", {}).get("url", "")
            if ".m3u8" in url or ".mpd" in url or ".mp4" in url:
                return url
        except Exception:
            continue
    for entry in reversed(entries):
        message = entry.get("message", "")
        media_match = re.search(
            r"https?://[^\"' ]+\.(?:m3u8|mpd|mp4)[^\"' ]*",
            message,
        )
        if media_match:
            return media_match.group(0)
    return None


def extract_media_url_from_driver_state(driver: Any) -> str | None:
    media_candidates = []
    media_match_pattern = r"(https?:[^\"'\\s<>]+\\.(?:m3u8|mpd|mp4)[^\"'\\s<>]*)"

    try:
        resources = driver.execute_script(
            """
            const urls = [];
            try {
                window.performance.getEntriesByType('resource')
                    .forEach((entry) => entry && entry.name && urls.push(entry.name));
            } catch (error) {}
            document.querySelectorAll('video, source').forEach((el) => {
                if (el.src) urls.push(el.src);
                if (el.currentSrc) urls.push(el.currentSrc);
                if (el.dataset && el.dataset.src) urls.push(el.dataset.src);
                if (el.getAttribute) {
                    const srcset = el.getAttribute('srcset');
                    if (srcset) urls.push(srcset);
                }
            });
            return urls;
            """
        )
        if resources:
            media_candidates.extend(resources)
    except Exception:
        pass

    try:
        page_source = driver.page_source or ""
        matches = re.findall(media_match_pattern, page_source, re.IGNORECASE)
        media_candidates.extend(matches)
    except Exception:
        pass

    for raw_url in reversed(media_candidates):
        if not raw_url or not isinstance(raw_url, str):
            continue
        url = raw_url.strip()
        if ".m3u8" in url.lower() or ".mpd" in url.lower() or ".mp4" in url.lower():
            return url
    return None


def click_playsight_play_controls(driver: Any) -> None:
    from selenium.webdriver.common.by import By

    play_locators = [
        (By.XPATH, '//*[contains(@class, "vjs-big-play-button")]'),
        (By.XPATH, '//*[contains(@class, "play-button")]'),
        (By.XPATH, '//button[contains(translate(@aria-label, "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"), "play")]'),
        (By.XPATH, '//button[contains(translate(normalize-space(.), "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"), "play")]'),
        (By.XPATH, '//*[@role="button" and contains(translate(@aria-label, "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"), "play")]'),
    ]

    for by, selector in play_locators:
        try:
            elements = driver.find_elements(by, selector)
        except Exception:
            continue
        for element in elements:
            try:
                driver.execute_script("arguments[0].click();", element)
                return
            except Exception:
                continue

    try:
        video_elements = driver.find_elements(By.TAG_NAME, "video")
        if video_elements:
            driver.execute_script(
                "const v=arguments[0]; if(v && v.paused){v.play().catch(()=>{});} ",
                video_elements[0],
            )
    except Exception:
        pass


def extract_media_url_with_playsight(page_url: str) -> str | None:
    options = build_chrome_options(enable_performance_logs=True)
    try:
        driver = create_chrome_driver(options)
    except Exception as exc:
        log_status(f"Failed to start Chrome WebDriver for Playsight: {exc}")
        return None
    try:
        log_status(f"Opening Playsight page: {page_url}")
        driver.get(page_url)
        deadline = time.time() + 45
        login_check_after = time.time() + 10
        next_interaction_at = time.time() + 3
        attempted_login = False
        while time.time() < deadline:
            entries = driver.get_log("performance")
            resolved = extract_media_url_from_performance_entries(entries)
            if resolved:
                log_status(f"Resolved Playsight media URL for {page_url}")
                return resolved
            resolved = extract_media_url_from_driver_state(driver)
            if resolved:
                log_status(f"Resolved Playsight media URL from page state for {page_url}")
                return resolved

            if time.time() >= next_interaction_at:
                click_playsight_play_controls(driver)
                next_interaction_at = time.time() + 8

            if not attempted_login and time.time() >= login_check_after:
                if os.getenv("PLAYSIGHT_EMAIL") and os.getenv("PLAYSIGHT_PASSWORD"):
                    log_status(
                        "No media URL detected yet. Attempting optional Playsight login fallback."
                    )
                    sign_into_playsight_if_needed(driver, page_url)
                attempted_login = True
            time.sleep(2)
    finally:
        driver.quit()
    log_status(f"No media URL found on Playsight page: {page_url}")
    return None


def click_track_tennis_play_controls(driver: Any) -> None:
    from selenium.webdriver.common.by import By

    play_locators = [
        (By.XPATH, '//*[contains(@class, "vjs-big-play-button")]'),
        (By.XPATH, '//button[contains(translate(@aria-label, "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"), "play")]'),
        (By.XPATH, '//button[contains(translate(normalize-space(.), "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"), "play")]'),
        (By.XPATH, '//*[@role="button" and contains(translate(@aria-label, "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"), "play")]'),
    ]

    for by, selector in play_locators:
        try:
            elements = driver.find_elements(by, selector)
        except Exception:
            continue
        for element in elements:
            try:
                driver.execute_script("arguments[0].click();", element)
                return
            except Exception:
                continue


def click_nusports_play_controls(driver: Any) -> None:
    from selenium.webdriver.common.by import By

    play_locators = [
        (By.XPATH, '//*[contains(@class, "vjs-big-play-button")]'),
        (By.XPATH, '//*[contains(@class, "play-button")]'),
        (By.XPATH, '//button[contains(translate(@aria-label, "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"), "play")]'),
        (By.XPATH, '//button[contains(translate(normalize-space(.), "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"), "play")]'),
        (By.XPATH, '//*[@role="button" and contains(translate(@aria-label, "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"), "play")]'),
    ]

    for by, selector in play_locators:
        try:
            elements = driver.find_elements(by, selector)
        except Exception:
            continue
        for element in elements:
            try:
                driver.execute_script("arguments[0].click();", element)
                return
            except Exception:
                continue

    try:
        video_elements = driver.find_elements(By.TAG_NAME, "video")
        if video_elements:
            driver.execute_script(
                "const v=arguments[0]; if(v && v.paused){v.play().catch(()=>{});} ",
                video_elements[0],
            )
    except Exception:
        pass


def extract_media_url_with_nusports(page_url: str) -> str | None:
    options = build_chrome_options(enable_performance_logs=True)
    try:
        driver = create_chrome_driver(options)
    except Exception as exc:
        log_status(f"Failed to start Chrome WebDriver for Northwestern stream: {exc}")
        return None

    try:
        log_status(f"Opening Northwestern stream page: {page_url}")
        driver.get(page_url)
        deadline = time.time() + 60
        next_interaction_at = time.time() + 3
        while time.time() < deadline:
            entries = driver.get_log("performance")
            resolved = extract_media_url_from_performance_entries(entries)
            if resolved:
                log_status(f"Resolved Northwestern media URL for {page_url}")
                return resolved

            if time.time() >= next_interaction_at:
                click_nusports_play_controls(driver)
                next_interaction_at = time.time() + 8
            time.sleep(2)
    finally:
        driver.quit()

    log_status(f"No media URL found on Northwestern stream page: {page_url}")
    return None


def extract_media_url_with_fightingillini(page_url: str) -> str | None:
    options = build_chrome_options(enable_performance_logs=True)
    try:
        driver = create_chrome_driver(options)
    except Exception as exc:
        log_status(f"Failed to start Chrome WebDriver for Illinois stream: {exc}")
        return None

    try:
        log_status(f"Opening Illinois stream page: {page_url}")
        driver.get(page_url)
        deadline = time.time() + 60
        next_interaction_at = time.time() + 3
        while time.time() < deadline:
            entries = driver.get_log("performance")
            resolved = extract_media_url_from_performance_entries(entries)
            if resolved:
                log_status(f"Resolved Illinois media URL for {page_url}")
                return resolved

            if time.time() >= next_interaction_at:
                click_nusports_play_controls(driver)
                next_interaction_at = time.time() + 8
            time.sleep(2)
    finally:
        driver.quit()

    log_status(f"No media URL found on Illinois stream page: {page_url}")
    return None


def extract_media_url_with_track_tennis(page_url: str) -> str | None:
    options = build_chrome_options(enable_performance_logs=True)
    try:
        driver = create_chrome_driver(options)
    except Exception as exc:
        log_status(f"Failed to start Chrome WebDriver for Track.Tennis: {exc}")
        return None
    try:
        log_status(f"Opening Track.Tennis page: {page_url}")
        driver.get(page_url)
        deadline = time.time() + 60
        next_click_at = time.time() + 3
        while time.time() < deadline:
            entries = driver.get_log("performance")
            resolved = extract_media_url_from_performance_entries(entries)
            if resolved:
                log_status(f"Resolved Track.Tennis media URL for {page_url}")
                return resolved

            if time.time() >= next_click_at:
                click_track_tennis_play_controls(driver)
                next_click_at = time.time() + 8
            time.sleep(2)
    finally:
        driver.quit()
    log_status(f"No media URL found on Track.Tennis page: {page_url}")
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


def extract_stream_target_filter(
    match_id: str, config: dict[str, Any]
) -> tuple[set[int], list[str]]:
    override = config.get("match_overrides", {}).get(match_id, {})

    requested_numbers: set[int] = set()
    for value in override.get("include_stream_numbers", []):
        try:
            requested_numbers.add(int(value))
        except (TypeError, ValueError):
            continue

    requested_label_substrings = [
        normalize_text(str(value)).lower()
        for value in override.get("include_stream_labels", [])
        if normalize_text(str(value))
    ]
    return requested_numbers, requested_label_substrings


def apply_runtime_stream_target_filter(
    config: dict[str, Any],
    match_id: str,
    stream_numbers: list[int] | None = None,
    stream_labels: list[str] | None = None,
) -> dict[str, Any]:
    stream_numbers = stream_numbers or []
    stream_labels = [label for label in (stream_labels or []) if normalize_text(label)]
    if not stream_numbers and not stream_labels:
        return config

    updated_config = copy.deepcopy(config)
    override = dict(updated_config.setdefault("match_overrides", {}).get(match_id, {}))
    override["include_stream_numbers"] = list(dict.fromkeys(stream_numbers))
    override["include_stream_labels"] = list(dict.fromkeys(stream_labels))
    updated_config["match_overrides"][match_id] = override
    return updated_config


def extract_event_title_filters(match_id: str, config: dict[str, Any]) -> list[str]:
    override = config.get("match_overrides", {}).get(match_id, {})
    raw = override.get("event_title_contains")
    if raw is None:
        return []

    values: list[str]
    if isinstance(raw, str):
        values = [raw]
    elif isinstance(raw, list):
        values = [str(item) for item in raw]
    else:
        return []

    return [normalize_text(value) for value in values if normalize_text(value)]


def label_matches_stream_filter(
    label: str, requested_numbers: set[int], requested_label_substrings: list[str]
) -> bool:
    if not requested_numbers and not requested_label_substrings:
        return True

    normalized_label = normalize_text(label).lower()
    label_numbers = {
        int(match.group(1))
        for match in re.finditer(r"\b(\d{1,3})\b", normalized_label)
    }
    number_match = bool(requested_numbers and label_numbers & requested_numbers)
    label_match = bool(
        requested_label_substrings
        and any(token in normalized_label for token in requested_label_substrings)
    )
    return number_match or label_match


def resolve_stream_targets(
    page_url: str,
    config: dict[str, Any],
    requested_numbers: set[int] | None = None,
    requested_label_substrings: list[str] | None = None,
    event_title_filters: list[str] | None = None,
) -> list[StreamTarget]:
    requested_numbers = requested_numbers or set()
    requested_label_substrings = requested_label_substrings or []
    event_title_filters = [
        normalize_text(token).lower()
        for token in (event_title_filters or [])
        if normalize_text(token)
    ]
    if not page_url or page_url == MANUAL_STREAM_URL_REQUIRED:
        return []

    if page_url.lower().endswith(DIRECT_STREAM_SUFFIXES):
        log_status(f"Using direct media URL: {page_url}")
        return [StreamTarget(label="Main Court", page_url=page_url, media_url=page_url)]

    if is_nusports_watch_url(page_url):
        event_pages = extract_nusports_event_pages_with_selenium(
            page_url, event_title_filters
        )
        if requested_numbers or requested_label_substrings:
            event_pages = [
                (label, event_page)
                for label, event_page in event_pages
                if label_matches_stream_filter(
                    label, requested_numbers, requested_label_substrings
                )
            ]
        if event_pages:
            log_status(
                f"Found {len(event_pages)} Northwestern event link(s) on {page_url}"
            )
            resolved_targets: list[StreamTarget] = []
            for label, event_page in event_pages:
                log_status(
                    f"Resolving Northwestern event stream target {label}: {event_page}"
                )
                try:
                    media_url = resolve_stream_url(event_page, config, depth=1)
                except Exception as exc:
                    log_status(
                        f"Failed to resolve Northwestern target {label} ({event_page}): {exc}"
                    )
                    continue
                if media_url:
                    resolved_targets.append(
                        StreamTarget(label=label, page_url=event_page, media_url=media_url)
                    )
            if resolved_targets:
                log_status(
                    f"Resolved {len(resolved_targets)} Northwestern stream target(s) from {page_url}"
                )
                return uniquify_stream_targets(resolved_targets)
        log_status(f"No Northwestern event streams resolved from {page_url}")

    if is_illinois_mten_stream_hub_url(page_url):
        watch_pages = extract_fightingillini_watch_pages(page_url, config)
        if requested_numbers or requested_label_substrings:
            watch_pages = [
                (label, watch_page)
                for label, watch_page in watch_pages
                if label_matches_stream_filter(
                    label, requested_numbers, requested_label_substrings
                )
            ]
        if watch_pages:
            log_status(
                f"Found {len(watch_pages)} Illinois Full Screen court link(s) on {page_url}"
            )
            resolved_targets: list[StreamTarget] = []
            for label, watch_page in watch_pages:
                log_status(
                    f"Resolving Illinois stream target {label}: {watch_page}"
                )
                try:
                    media_url = resolve_stream_url(watch_page, config, depth=1)
                except Exception as exc:
                    log_status(
                        f"Failed to resolve Illinois target {label} ({watch_page}): {exc}"
                    )
                    continue
                if media_url:
                    resolved_targets.append(
                        StreamTarget(label=label, page_url=watch_page, media_url=media_url)
                    )
            if resolved_targets:
                log_status(
                    f"Resolved {len(resolved_targets)} Illinois stream target(s) from {page_url}"
                )
                return uniquify_stream_targets(resolved_targets)
            log_status(
                f"Illinois Full Screen links were found but no live media URLs are available yet on {page_url}"
            )
        else:
            log_status(
                f"No Illinois Full Screen court links were detected on {page_url}"
            )

    parsed_page = parse_url_or_none(page_url)
    parsed_path = parsed_page.path.lower() if parsed_page else ""
    if is_track_tennis_url(page_url):
        log_status(f"Resolving Track.Tennis page: {page_url}")
        resolved = resolve_stream_url(page_url, config)
        if resolved:
            label = "Main Court"
            if not label_matches_stream_filter(
                label, requested_numbers, requested_label_substrings
            ):
                log_status("Track.Tennis target skipped by stream target filters.")
                return []
            log_status(f"Resolved Track.Tennis target {label}: {page_url}")
            return [StreamTarget(label=label, page_url=page_url, media_url=resolved)]
        log_status(f"Track.Tennis page did not yield media URL yet: {page_url}")

    if is_playsight_url(page_url) and ("/live/" in parsed_path or "/livestreaming/" in parsed_path):
        log_status(f"Resolving direct Playsight court page first: {page_url}")
        resolved = resolve_stream_url(page_url, config)
        if resolved:
            label = extract_court_label(page_url, "Main Court")
            if not label_matches_stream_filter(
                label, requested_numbers, requested_label_substrings
            ):
                log_status(
                    f"Direct Playsight target {label} skipped by stream target filters."
                )
                return []
            log_status(f"Resolved direct Playsight target {label}: {page_url}")
            return [StreamTarget(label=label, page_url=page_url, media_url=resolved)]
        log_status(f"Direct Playsight page did not yield media URL yet: {page_url}")

    embedded_facility_pages: list[str] = []
    if not is_playsight_url(page_url):
        embedded_facility_pages = extract_embedded_playsight_facility_pages(
            page_url, config
        )
    if embedded_facility_pages:
        log_status(
            f"Found {len(embedded_facility_pages)} embedded Playsight facility link(s) on {page_url}"
        )
        for facility_page in embedded_facility_pages:
            watch_pages = extract_playsight_watch_pages(facility_page, config)
            if requested_numbers or requested_label_substrings:
                watch_pages = [
                    (label, watch_page)
                    for label, watch_page in watch_pages
                    if label_matches_stream_filter(
                        label, requested_numbers, requested_label_substrings
                    )
                ]
            if not watch_pages:
                continue
            log_status(
                f"Found {len(watch_pages)} Playsight watch page(s) on {facility_page}"
            )
            resolved_targets: list[StreamTarget] = []
            for label, watch_page in watch_pages:
                log_status(f"Resolving Playsight watch page {label}: {watch_page}")
                try:
                    media_url = resolve_stream_url(watch_page, config, depth=1)
                except Exception as exc:
                    log_status(
                        f"Failed to resolve watch page {label} ({watch_page}): {exc}"
                    )
                    continue
                if media_url:
                    resolved_targets.append(
                        StreamTarget(label=label, page_url=watch_page, media_url=media_url)
                    )
            if resolved_targets:
                log_status(
                    f"Resolved {len(resolved_targets)} live stream target(s) from {facility_page}"
                )
                return uniquify_stream_targets(resolved_targets)

    embedded_targets = extract_embedded_stream_targets(page_url, config)
    if requested_numbers or requested_label_substrings:
        embedded_targets = [
            (label, watch_page)
            for label, watch_page in embedded_targets
            if label_matches_stream_filter(
                label, requested_numbers, requested_label_substrings
            )
        ]
    if embedded_targets:
        log_status(
            f"Found {len(embedded_targets)} embedded Playsight court links on {page_url}"
        )
        resolved_targets: list[StreamTarget] = []
        for label, watch_page in embedded_targets:
            log_status(f"Resolving embedded stream target {label}: {watch_page}")
            try:
                media_url = resolve_stream_url(watch_page, config, depth=1)
            except Exception as exc:
                log_status(
                    f"Failed to resolve embedded target {label} ({watch_page}): {exc}"
                )
                continue
            if media_url:
                resolved_targets.append(
                    StreamTarget(label=label, page_url=watch_page, media_url=media_url)
                )
        if resolved_targets:
            log_status(f"Resolved {len(resolved_targets)} live stream target(s) from {page_url}")
            return uniquify_stream_targets(resolved_targets)

    if is_playsight_facility_or_multi_page(page_url):
        watch_pages = extract_playsight_watch_pages(page_url, config)
        if requested_numbers or requested_label_substrings:
            watch_pages = [
                (label, watch_page)
                for label, watch_page in watch_pages
                if label_matches_stream_filter(
                    label, requested_numbers, requested_label_substrings
                )
            ]
        if watch_pages:
            log_status(f"Found {len(watch_pages)} Playsight watch page(s) on {page_url}")
            resolved_targets: list[StreamTarget] = []
            for label, watch_page in watch_pages:
                log_status(f"Resolving Playsight watch page {label}: {watch_page}")
                try:
                    media_url = resolve_stream_url(watch_page, config, depth=1)
                except Exception as exc:
                    log_status(
                        f"Failed to resolve watch page {label} ({watch_page}): {exc}"
                    )
                    continue
                if media_url:
                    resolved_targets.append(
                        StreamTarget(
                            label=label, page_url=watch_page, media_url=media_url)
                    )
            if resolved_targets:
                log_status(f"Resolved {len(resolved_targets)} live stream target(s) from {page_url}")
                return uniquify_stream_targets(resolved_targets)

    resolved = resolve_stream_url(page_url, config)
    if not resolved:
        log_status(f"No live stream target resolved from {page_url}")
        return []

    label = "Main Court"
    if is_playsight_url(page_url):
        label = extract_court_label(page_url, "Main Court")
    log_status(f"Resolved single stream target {label}: {page_url}")
    return [StreamTarget(label=label, page_url=page_url, media_url=resolved)]


def resolve_stream_url(page_url: str, config: dict[str, Any], depth: int = 0) -> str | None:
    if not page_url or page_url == MANUAL_STREAM_URL_REQUIRED or depth > 2:
        return None

    lowered = page_url.lower()
    playsight_intent = is_playsight_intent_url(page_url)
    track_tennis_intent = "track.tennis" in lowered
    nusports_intent = is_nusports_url(page_url)
    fightingillini_intent = is_fightingillini_url(page_url)
    if lowered.endswith(DIRECT_STREAM_SUFFIXES):
        return page_url

    parsed = parse_url_or_none(page_url)
    if parsed is None or parsed.scheme not in {"http", "https"}:
        return None
    path = parsed.path.lower() if parsed else ""
    if is_track_tennis_url(page_url):
        return extract_media_url_with_track_tennis(page_url)
    if is_nusports_watch_url(page_url):
        return None
    if is_illinois_mten_stream_hub_url(page_url):
        return None
    if is_fightingillini_watch_url(page_url):
        if fightingillini_watch_page_unavailable(page_url, config):
            return None
        resolved = extract_media_url_with_fightingillini(page_url)
        if resolved:
            return resolved
        live_id = extract_live_query_id(page_url)
        if live_id:
            embed_url = build_fightingillini_embed_url(live_id)
            return extract_media_url_with_fightingillini(embed_url)
        return None
    if is_fightingillini_embed_url(page_url):
        return extract_media_url_with_fightingillini(page_url)
    if nusports_intent:
        resolved = extract_media_url_with_nusports(page_url)
        if resolved:
            return resolved
    if is_playsight_url(page_url) and ("/live/" in path or "/livestreaming/" in path):
        resolved = extract_media_url_with_playsight(page_url)
        if resolved:
            return resolved
        return extract_media_url_with_yt_dlp(page_url)

    embedded_links = fetch_page_links(page_url, config)
    if playsight_intent:
        playsight_links = []
        for link in embedded_links:
            if not is_playsight_url(link):
                continue
            parsed_link = parse_url_or_none(link)
            if parsed_link is None:
                continue
            link_path = parsed_link.path.lower()
            if (
                "/live/" not in link_path
                and "/livestreaming/" not in link_path
                and "/facility/" not in link_path
            ):
                continue
            playsight_links.append(link)
        if playsight_links:
            embedded_links = playsight_links
        else:
            embedded_links = [
                link
                for link in embedded_links
                if "youtube.com" not in link.lower() and "youtu.be" not in link.lower()
            ]
    if track_tennis_intent:
        track_links = [link for link in embedded_links if "track.tennis" in link.lower()]
        if track_links:
            embedded_links = track_links
    if nusports_intent:
        nusports_links = [link for link in embedded_links if is_nusports_url(link)]
        if nusports_links:
            embedded_links = nusports_links
    if fightingillini_intent:
        fightingillini_links = [
            link for link in embedded_links if is_fightingillini_url(link)
        ]
        if fightingillini_links:
            embedded_links = fightingillini_links
    for embedded_link in embedded_links:
        try:
            resolved = resolve_stream_url(embedded_link, config, depth + 1)
        except Exception:
            continue
        if resolved:
            return resolved

    for resolver in (
        lambda: extract_media_url_with_requests(page_url, config),
        lambda: None
        if (
            is_playsight_url(page_url)
            or playsight_intent
            or track_tennis_intent
            or nusports_intent
            or fightingillini_intent
        )
        else extract_media_url_with_yt_dlp(page_url),
    ):
        resolved = resolver()
        if resolved:
            return resolved
    if is_playsight_url(page_url):
        resolved = extract_media_url_with_playsight(page_url)
        if resolved:
            return resolved
        return extract_media_url_with_yt_dlp(page_url)
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
    requested_numbers, requested_label_substrings = extract_stream_target_filter(
        match.match_id, config
    )
    event_title_filters = extract_event_title_filters(match.match_id, config)
    if requested_numbers or requested_label_substrings:
        filter_summary: list[str] = []
        if requested_numbers:
            filter_summary.append(
                "numbers=" + ",".join(str(number) for number in sorted(requested_numbers))
            )
        if requested_label_substrings:
            filter_summary.append(
                "labels=" + ",".join(requested_label_substrings)
            )
        log_status(
            f"Applying stream target filter for {match.match_id}: {'; '.join(filter_summary)}"
        )
    if event_title_filters:
        log_status(
            "Applying event title filter for "
            f"{match.match_id}: {', '.join(event_title_filters)}"
        )
    log_status(
        f"Preparing recording for {match.match_id} ({match.title}) scheduled at {start_dt.isoformat()}"
    )
    if now < window_start:
        seconds_until_start = (window_start - now).total_seconds()
        if seconds_until_start > 0:
            log_status(
                f"Waiting until lead window opens at {window_start.isoformat()} "
                f"({int(seconds_until_start)} seconds)"
            )
            time.sleep(seconds_until_start)

    deadline = start_dt + timedelta(minutes=config["stream_retry_minutes"])
    now = datetime.now(ZoneInfo(match.timezone))
    if now > deadline:
        log_status(
            "Recorder started after the scheduled retry window "
            f"(current: {now.isoformat()}, deadline: {deadline.isoformat()}). "
            f"Extending polling by {config['stream_retry_minutes']} minutes from now."
        )
        deadline = now + timedelta(minutes=config["stream_retry_minutes"])
    attempt = 1
    while datetime.now(ZoneInfo(match.timezone)) <= deadline:
        log_status(
            f"Polling for live stream targets (attempt {attempt}) from {match.stream_page_url}"
        )
        resolved_targets = resolve_stream_targets(
            match.stream_page_url or "",
            config,
            requested_numbers=requested_numbers,
            requested_label_substrings=requested_label_substrings,
            event_title_filters=event_title_filters,
        )
        if resolved_targets:
            labels = ", ".join(target.label for target in resolved_targets)
            log_status(
                f"Live stream target(s) ready for {match.match_id}: {labels}"
            )
            return resolved_targets
        log_status(
            f"No live stream targets found yet. Sleeping {config['poll_interval_seconds']} seconds."
        )
        time.sleep(config["poll_interval_seconds"])
        attempt += 1

    raise RuntimeError(
        f"Unable to resolve a live stream for {match.match_id} before {deadline.isoformat()}."
    )


def apply_stream_target_filters(
    match_id: str, targets: list[StreamTarget], config: dict[str, Any]
) -> list[StreamTarget]:
    requested_numbers, requested_label_substrings = extract_stream_target_filter(
        match_id, config
    )

    if not requested_numbers and not requested_label_substrings:
        return targets

    filtered: list[StreamTarget] = []
    for target in targets:
        if label_matches_stream_filter(
            target.label, requested_numbers, requested_label_substrings
        ):
            filtered.append(target)

    if not filtered:
        available_labels = ", ".join(target.label for target in targets) or "(none)"
        raise RuntimeError(
            f"Stream target filters for {match_id} matched nothing. "
            f"Available targets: {available_labels}"
        )

    selected_labels = ", ".join(target.label for target in filtered)
    log_status(f"Applied stream target filters for {match_id}. Keeping: {selected_labels}")
    return filtered


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
    log_status(
        f"Starting recording for {len(targets)} stream target(s) for {match.match_id}"
    )

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
        log_status(
            f"Launching ffmpeg for {target.label}: {target.page_url} -> {output_path}"
        )
        process = subprocess.Popen(command)
        recordings.append((target, output_path, process))

    failures: list[str] = []
    completed: list[tuple[StreamTarget, Path]] = []
    for target, output_path, process in recordings:
        if process.wait() != 0:
            log_status(f"ffmpeg failed for {target.label}")
            failures.append(target.label)
        else:
            log_status(f"Recording finished for {target.label}: {output_path}")
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
        log_status(f"YouTube upload disabled. Skipping upload for {output_path}")
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
    log_status(f"Uploading to YouTube: {output_path}")
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
    log_status(f"YouTube upload complete: {output_path}")


def record_match(
    config_path: Path,
    match_id: str,
    stream_numbers: list[int] | None = None,
    stream_labels: list[str] | None = None,
) -> list[Path]:
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

    config = apply_runtime_stream_target_filter(
        config,
        match_id,
        stream_numbers=stream_numbers,
        stream_labels=stream_labels,
    )
    log_status(f"Using stream page URL for {match_id}: {selected.stream_page_url}")
    stream_targets = wait_for_live_streams(selected, config)
    stream_targets = apply_stream_target_filters(match_id, stream_targets, config)
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
    record_parser.add_argument(
        "--stream-number",
        action="append",
        dest="stream_numbers",
        type=int,
        help="Restrict the recording to a specific resolved court/stream number. May be repeated.",
    )
    record_parser.add_argument(
        "--stream-label",
        action="append",
        dest="stream_labels",
        help="Restrict the recording to stream labels containing this text. May be repeated.",
    )

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
            output_paths = record_match(
                config_path,
                args.match_id,
                stream_numbers=args.stream_numbers,
                stream_labels=args.stream_labels,
            )
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
