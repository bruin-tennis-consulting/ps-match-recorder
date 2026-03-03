from __future__ import annotations

import argparse
from pathlib import Path

SCOPES = ["https://www.googleapis.com/auth/youtube.upload"]


def build_credentials(client_secrets_file: Path, token_file: Path):
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow

    credentials = None
    if token_file.exists():
        credentials = Credentials.from_authorized_user_file(str(token_file), SCOPES)

    if credentials and credentials.expired and credentials.refresh_token:
        credentials.refresh(Request())
        token_file.write_text(credentials.to_json(), encoding="utf-8")
        return credentials

    if credentials and credentials.valid:
        return credentials

    flow = InstalledAppFlow.from_client_secrets_file(str(client_secrets_file), SCOPES)
    credentials = flow.run_local_server(port=0)
    token_file.parent.mkdir(parents=True, exist_ok=True)
    token_file.write_text(credentials.to_json(), encoding="utf-8")
    return credentials


def upload_video(
    file_path: Path,
    title: str,
    description: str,
    client_secrets_file: Path,
    token_file: Path,
    privacy_status: str = "unlisted",
    category_id: str = "17",
    tags: list[str] | None = None,
) -> dict:
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload

    credentials = build_credentials(client_secrets_file, token_file)
    youtube = build("youtube", "v3", credentials=credentials)

    request = youtube.videos().insert(
        part="snippet,status",
        body={
            "snippet": {
                "title": title,
                "description": description,
                "tags": tags or [],
                "categoryId": category_id,
            },
            "status": {"privacyStatus": privacy_status},
        },
        media_body=MediaFileUpload(str(file_path), chunksize=-1, resumable=True),
    )

    response = None
    while response is None:
        _, response = request.next_chunk()
    return response


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Authorize and upload match recordings to YouTube.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    auth_parser = subparsers.add_parser("auth", help="Run the local OAuth flow and cache the token.")
    auth_parser.add_argument("--client-secrets", required=True)
    auth_parser.add_argument("--token", required=True)

    upload_parser = subparsers.add_parser("upload", help="Upload a single file to YouTube.")
    upload_parser.add_argument("--file", required=True)
    upload_parser.add_argument("--title", required=True)
    upload_parser.add_argument("--description", default="")
    upload_parser.add_argument("--client-secrets", required=True)
    upload_parser.add_argument("--token", required=True)
    upload_parser.add_argument("--privacy-status", default="unlisted")
    upload_parser.add_argument("--category-id", default="17")
    upload_parser.add_argument("--tags", nargs="*", default=[])
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    client_secrets_file = Path(args.client_secrets).resolve()
    token_file = Path(args.token).resolve()

    if args.command == "auth":
        build_credentials(client_secrets_file, token_file)
        print(token_file)
        return 0

    if args.command == "upload":
        response = upload_video(
            file_path=Path(args.file).resolve(),
            title=args.title,
            description=args.description,
            client_secrets_file=client_secrets_file,
            token_file=token_file,
            privacy_status=args.privacy_status,
            category_id=args.category_id,
            tags=args.tags,
        )
        print(response.get("id", "uploaded"))
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
