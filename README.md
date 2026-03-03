# ps-match-recorder

This repo now includes a Big Ten men's tennis workflow that:

1. Pulls the conference schedule from `bigten.org`.
2. Stores a local catalog of matches and stream URLs.
3. Waits for the stream to go live, records it with `ffmpeg`, and saves it under a match-specific folder.
4. Optionally uploads the finished file to YouTube.
5. Generates `launchd` plists so macOS can start each recording automatically.

## Install

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
brew install ffmpeg
```

`site_parser.py` uses `requests` first and falls back to headless Selenium if the schedule page needs rendering. `yt-dlp` is used to resolve many stream pages into direct media URLs. Playsight pages have an extra Selenium fallback that watches Chrome's network log for `.m3u8` traffic.

## Config

Start from the example config:

```bash
cp config/bigten_recorder.example.json config/bigten_recorder.json
```

Then edit `config/bigten_recorder.json`:

- Set `schedule_timezone` to the timezone you want schedule times interpreted in.
- Fill `match_overrides` for any match that could not be resolved automatically.
- Turn on `youtube.enabled` after you have YouTube OAuth credentials.

If a match has no known stream page yet, leave `stream_page_url` as `MANUAL_STREAM_URL_REQUIRED` or add the real page later.

## Schedule Sync

Build or refresh the local match catalog:

```bash
python3 site_parser.py --config config/bigten_recorder.json sync
```

That writes:

- `data/bigten_matches.json`: the generated match catalog
- `data/schedule_debug.html`: the raw HTML snapshot used for parsing

Use the catalog output to find the match ids that need overrides.

## Manual Example

The sample config includes the Arizona-hosted Michigan State match as:

- Match date: `2026-03-03`
- Local Arizona time: `1:00 PM MST`
- Pacific time: `12:00 PM PST`
- Stream landing page: `https://arizonawildcats.com/sports/2020/1/14/tennis-live-streaming`

That Arizona stream page links to Playsight at `https://web.playsight.com/live/university-of-arizona/101`.

## Record A Match

```bash
python3 site_parser.py --config config/bigten_recorder.json record --match-id 2026-03-03_michigan-state-vs-arizona
```

The recorder:

- waits until the lead window opens
- keeps polling until the stream is actually live
- resolves the page URL to a direct stream URL
- records every live court if the page is a Playsight facility or multi-court page
- records for the configured duration
- saves the file under `recordings/YYYY-MM-DD/<match-title>/`
- uploads to YouTube if enabled

If a Playsight page requires login, set:

```bash
export PLAYSIGHT_EMAIL="you@example.com"
export PLAYSIGHT_PASSWORD="your-password"
```

## Generate macOS Jobs

Create `launchd` plists for future matches with known stream URLs:

```bash
python3 site_parser.py --config config/bigten_recorder.json schedule-jobs
```

The generated plists are written to `launchd/`. To install one:

```bash
cp launchd/com.psmatchrecorder.<match-id>.plist ~/Library/LaunchAgents/
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.psmatchrecorder.<match-id>.plist
```

Logs are written under `logs/`.

## YouTube Uploads

Place your OAuth client file at `config/youtube_client_secrets.json`, then run:

```bash
python3 youtube_uploader.py auth \
  --client-secrets config/youtube_client_secrets.json \
  --token config/youtube_token.json
```

After that, enable uploads in `config/bigten_recorder.json`.

## Legacy Script

`video_downloader.py` is still present, but the new Big Ten flow lives in `site_parser.py` and `youtube_uploader.py`.
