import logging
import os
import subprocess
import sys
import tempfile
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests
import telebot
import yt_dlp
from dotenv import load_dotenv
from requests.exceptions import ConnectionError as RequestsConnectionError, RequestException
from telebot import types
from telebot.apihelper import ApiTelegramException
from yt_dlp.utils import DownloadError


load_dotenv()

TELEGRAM_TOKEN_ENV = "TELEGRAM_BOT_TOKEN"
TELEGRAM_API_ID_ENV = "TELEGRAM_API_ID"
TELEGRAM_API_HASH_ENV = "TELEGRAM_API_HASH"
TELEGRAM_LOCAL_API_URL_ENV = "TELEGRAM_LOCAL_API_URL"
TELEGRAM_LOCAL_SERVER_ENABLED_ENV = "TELEGRAM_LOCAL_SERVER_ENABLED"
TELEGRAM_LOCAL_SERVER_TIMEOUT_ENV = "TELEGRAM_LOCAL_SERVER_TIMEOUT"
TELEGRAM_LOCAL_SERVER_FORCE_REBUILD_ENV = "TELEGRAM_LOCAL_SERVER_FORCE_REBUILD"
SUPPORTED_FORMATS = {"mp3", "mp4"}
LOG_DIR = Path("logs")
LOG_FILE = LOG_DIR / "bot.log"
TELEGRAM_UPLOAD_LIMIT_BYTES = 49 * 1024 * 1024
TELEGRAM_LOCAL_UPLOAD_LIMIT_BYTES = 2000 * 1024 * 1024
MP3_BITRATE_CHOICES = [192, 160, 128, 96, 64]
DEFAULT_LOCAL_API_URL = "http://127.0.0.1:8081"
DEFAULT_LOCAL_SERVER_TIMEOUT_SECONDS = 180
LOCAL_SERVER_COMPOSE_FILE = Path("docker-compose.local-bot-api.yml")
LOCAL_SERVER_SERVICE = "telegram-bot-api"
LOCAL_SERVER_CONTAINER_NAME = "telegram-video-downloader-bot-api"
CLOUD_BOT_API_BASE_URL = "https://api.telegram.org"
DEFAULT_API_URL_TEMPLATE = telebot.apihelper.API_URL
DEFAULT_FILE_URL_TEMPLATE = telebot.apihelper.FILE_URL

LOG_DIR.mkdir(exist_ok=True)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.propagate = False

_stdout_handler = logging.StreamHandler(sys.stdout)
_stdout_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
logger.addHandler(_stdout_handler)

_file_handler = RotatingFileHandler(LOG_FILE, maxBytes=1_000_000, backupCount=3, encoding="utf-8")
_file_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
logger.addHandler(_file_handler)

user_data: dict[int, dict[str, Any]] = {}
# Chats com download em andamento — evita processamento duplo de callbacks
_active_downloads: set[int] = set()


def short_text(value: str, max_length: int = 80) -> str:
    cleaned = " ".join(value.split())
    if len(cleaned) <= max_length:
        return cleaned
    return f"{cleaned[: max_length - 3]}..."


def log_event(event: str, **context: Any) -> None:
    details = " ".join(f"{key}={value}" for key, value in context.items())
    if details:
        logger.info("[%s] %s", event, details)
        return
    logger.info("[%s]", event)


def get_bot_token() -> str:
    token = os.getenv(TELEGRAM_TOKEN_ENV, "").strip()
    if not token:
        raise RuntimeError(f"Defina {TELEGRAM_TOKEN_ENV} no ambiente ou no arquivo .env.")
    return token


def get_required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Defina {name} no ambiente ou no arquivo .env.")
    return value


def env_flag(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"}


def is_local_server_enabled() -> bool:
    return env_flag(TELEGRAM_LOCAL_SERVER_ENABLED_ENV) or bool(os.getenv(TELEGRAM_LOCAL_API_URL_ENV, "").strip())


def get_local_api_url() -> str | None:
    if not is_local_server_enabled():
        return None
    return os.getenv(TELEGRAM_LOCAL_API_URL_ENV, DEFAULT_LOCAL_API_URL).strip().rstrip("/")


def get_local_server_timeout_seconds() -> int:
    raw_value = os.getenv(TELEGRAM_LOCAL_SERVER_TIMEOUT_ENV, str(DEFAULT_LOCAL_SERVER_TIMEOUT_SECONDS)).strip()
    try:
        return max(10, int(raw_value))
    except ValueError as exc:
        raise RuntimeError(f"{TELEGRAM_LOCAL_SERVER_TIMEOUT_ENV} precisa ser um numero inteiro.") from exc


def configure_bot_api_endpoints(base_url: str | None = None) -> None:
    if base_url:
        telebot.apihelper.API_URL = f"{base_url}/bot{{0}}/{{1}}"
        telebot.apihelper.FILE_URL = f"{base_url}/file/bot{{0}}/{{1}}"
        return
    telebot.apihelper.API_URL = DEFAULT_API_URL_TEMPLATE
    telebot.apihelper.FILE_URL = DEFAULT_FILE_URL_TEMPLATE


def get_upload_limit() -> int:
    if get_local_api_url():
        return TELEGRAM_LOCAL_UPLOAD_LIMIT_BYTES
    return TELEGRAM_UPLOAD_LIMIT_BYTES


def create_bot(token: str | None = None) -> telebot.TeleBot:
    return telebot.TeleBot(token or get_bot_token(), parse_mode=None)


def get_bot_api_method_url(base_url: str, token: str, method: str) -> str:
    return f"{base_url.rstrip('/')}/bot{token}/{method}"


def call_bot_api(base_url: str, token: str, method: str, **payload: Any) -> dict[str, Any]:
    response = requests.post(
        get_bot_api_method_url(base_url, token, method),
        data=payload or None,
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def ensure_local_server_credentials() -> None:
    get_required_env(TELEGRAM_API_ID_ENV)
    get_required_env(TELEGRAM_API_HASH_ENV)


def run_local_server_compose(*compose_args: str) -> None:
    if not LOCAL_SERVER_COMPOSE_FILE.exists():
        raise RuntimeError(
            f"Arquivo de compose nao encontrado: {LOCAL_SERVER_COMPOSE_FILE}. "
            "Nao consigo iniciar o servidor Bot API local automaticamente."
        )

    command = ["docker", "compose", "-f", str(LOCAL_SERVER_COMPOSE_FILE), *compose_args]
    try:
        subprocess.run(
            command,
            check=True,
            env=os.environ.copy(),
        )
    except FileNotFoundError as exc:
        raise RuntimeError("Docker ou Docker Compose nao esta disponivel neste ambiente.") from exc
    except subprocess.CalledProcessError as exc:
        message = f"Falha ao executar {' '.join(command)}."
        raise RuntimeError(message) from exc


def wait_for_local_server(base_url: str, timeout_seconds: int) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            response = requests.get(base_url, timeout=3)
            if response.status_code < 500:
                return
        except RequestException as exc:
            last_error = exc
        time.sleep(1)

    if last_error:
        raise RuntimeError(f"O servidor Bot API local nao respondeu a tempo: {last_error}") from last_error
    raise RuntimeError("O servidor Bot API local nao respondeu a tempo.")


def get_local_server_container_state() -> str | None:
    command = [
        "docker",
        "inspect",
        LOCAL_SERVER_CONTAINER_NAME,
        "--format",
        "{{.State.Status}}|{{.State.ExitCode}}|{{.State.Error}}",
    ]
    try:
        completed = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            env=os.environ.copy(),
        )
    except (FileNotFoundError, subprocess.CalledProcessError):
        return None
    return completed.stdout.strip() or None


def get_local_server_logs() -> str | None:
    command = ["docker", "compose", "-f", str(LOCAL_SERVER_COMPOSE_FILE), "logs", "--tail=50", LOCAL_SERVER_SERVICE]
    try:
        completed = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            env=os.environ.copy(),
        )
    except (FileNotFoundError, subprocess.CalledProcessError):
        return None
    logs = completed.stdout.strip()
    return logs or None


def ensure_cloud_logout(token: str) -> None:
    try:
        call_bot_api(CLOUD_BOT_API_BASE_URL, token, "deleteWebhook", drop_pending_updates="false")
        result = call_bot_api(CLOUD_BOT_API_BASE_URL, token, "logOut")
    except RequestException as exc:
        raise RuntimeError("Falha ao fazer logOut do bot na API publica do Telegram.") from exc

    if not result.get("ok"):
        raise RuntimeError(f"Nao consegui fazer logOut do bot na API publica: {result.get('description') or result}")


def start_local_server(token: str) -> str:
    local_url = get_local_api_url()
    if not local_url:
        configure_bot_api_endpoints(None)
        return CLOUD_BOT_API_BASE_URL

    ensure_local_server_credentials()
    log_event("local_server_starting", url=local_url)
    if env_flag(TELEGRAM_LOCAL_SERVER_FORCE_REBUILD_ENV):
        logger.info("Reconstruindo o servidor Bot API local porque %s=1.", TELEGRAM_LOCAL_SERVER_FORCE_REBUILD_ENV)
        run_local_server_compose("build", LOCAL_SERVER_SERVICE)
    logger.info("Subindo o servidor Bot API local. Na primeira vez isso pode demorar alguns minutos.")
    run_local_server_compose("up", "-d", LOCAL_SERVER_SERVICE)
    logger.info("Servidor local iniciado no Docker. Aguardando responder em %s...", local_url)
    try:
        wait_for_local_server(local_url, get_local_server_timeout_seconds())
    except RuntimeError as exc:
        state = get_local_server_container_state()
        logs = get_local_server_logs()
        if state:
            logger.error("Estado do container do servidor local: %s", state)
        if logs:
            logger.error("Ultimas linhas do servidor Bot API local:\n%s", logs)
        raise RuntimeError(
            "O servidor Bot API local nao ficou disponivel. "
            "Verifique os logs acima do container 'telegram-bot-api'."
        ) from exc
    ensure_cloud_logout(token)
    configure_bot_api_endpoints(local_url)
    log_event("local_server_ready", url=local_url)
    return local_url


def stop_local_server() -> None:
    local_url = get_local_api_url()
    try:
        if local_url:
            run_local_server_compose("down", "--remove-orphans")
            log_event("local_server_stopped", url=local_url)
    finally:
        configure_bot_api_endpoints(None)


def is_valid_url(text: str) -> bool:
    parsed = urlparse(text)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def format_size_label(size_bytes: int | None) -> str:
    if not size_bytes or size_bytes <= 0:
        return "tamanho incerto"

    size = float(size_bytes)
    units = ["B", "KB", "MB", "GB"]
    unit_index = 0
    while size >= 1024 and unit_index < len(units) - 1:
        size /= 1024
        unit_index += 1
    return f"{size:.1f} {units[unit_index]}"


def build_playlist_download_keyboard(size_estimates: dict[str, int | None] | None = None) -> types.InlineKeyboardMarkup:
    size_estimates = size_estimates or {}
    markup = types.InlineKeyboardMarkup()
    markup.add(
        types.InlineKeyboardButton(
            f"Baixar playlist em MP3 ({format_size_label(size_estimates.get('mp3'))})",
            callback_data="dl:mp3",
        ),
        types.InlineKeyboardButton(
            f"Baixar playlist em MP4 ({format_size_label(size_estimates.get('mp4'))})",
            callback_data="dl:mp4",
        ),
    )
    return markup


def sanitize_filename_stem(value: str) -> str:
    normalized = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in Path(value).stem)
    return normalized.strip("_") or "download"


def build_download_options(target_format: str, download_dir: str, *, is_playlist: bool = False) -> dict[str, Any]:
    if target_format not in SUPPORTED_FORMATS:
        raise ValueError(f"Formato nao suportado: {target_format}")

    output_template = "%(title)s.%(ext)s"
    if is_playlist:
        output_template = "%(playlist_index)03d-%(title)s.%(ext)s"

    options: dict[str, Any] = {
        "noplaylist": not is_playlist,
        "restrictfilenames": True,
        "quiet": True,
        "no_warnings": True,
        "outtmpl": str(Path(download_dir) / output_template),
    }

    if target_format == "mp3":
        options.update(
            {
                "format": "bestaudio/best",
                "writethumbnail": True,
                "embedthumbnail": True,
                "addmetadata": True,
                "parse_metadata": [
                    "%(channel|uploader|creator|artist|)s:%(meta_artist)s",
                ],
                "postprocessors": [
                    {
                        "key": "FFmpegExtractAudio",
                        "preferredcodec": "mp3",
                        "preferredquality": "192",
                    }
                ],
            }
        )
    else:
        options.update(
            {
                "format": "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/bestvideo+bestaudio/best",
                "merge_output_format": "mp4",
            }
        )

    return options


def estimate_audio_size_for_bitrate(duration: int | float | None, bitrate_kbps: int) -> int | None:
    if not duration:
        return None
    return int((bitrate_kbps * 1000 / 8) * float(duration))


def estimate_size_from_format(format_info: dict[str, Any], duration: int | float | None = None) -> int | None:
    filesize = format_info.get("filesize") or format_info.get("filesize_approx")
    if filesize:
        return int(filesize)

    bitrate = format_info.get("tbr") or format_info.get("abr")
    if bitrate and duration:
        return int((float(bitrate) * 1000 / 8) * float(duration))
    return None


def pick_best_audio_format(formats: list[dict[str, Any]]) -> dict[str, Any] | None:
    audio_only = [fmt for fmt in formats if fmt.get("vcodec") == "none" and fmt.get("acodec") not in {None, "none"}]
    if not audio_only:
        return None
    return max(
        audio_only,
        key=lambda fmt: (
            fmt.get("abr") or 0,
            fmt.get("tbr") or 0,
            fmt.get("asr") or 0,
        ),
    )


def pick_best_mp4_video_format(formats: list[dict[str, Any]]) -> dict[str, Any] | None:
    video_only = [fmt for fmt in formats if fmt.get("vcodec") not in {None, "none"} and fmt.get("acodec") == "none"]
    preferred = [fmt for fmt in video_only if fmt.get("ext") == "mp4"]
    candidates = preferred or video_only
    if not candidates:
        return None
    return max(
        candidates,
        key=lambda fmt: (
            fmt.get("height") or 0,
            fmt.get("fps") or 0,
            fmt.get("tbr") or 0,
        ),
    )


def pick_best_m4a_audio_format(formats: list[dict[str, Any]]) -> dict[str, Any] | None:
    audio_only = [fmt for fmt in formats if fmt.get("vcodec") == "none" and fmt.get("acodec") not in {None, "none"}]
    preferred = [fmt for fmt in audio_only if fmt.get("ext") in {"m4a", "mp4"}]
    candidates = preferred or audio_only
    if not candidates:
        return None
    return max(
        candidates,
        key=lambda fmt: (
            fmt.get("abr") or 0,
            fmt.get("tbr") or 0,
        ),
    )


def estimate_single_download_sizes(info: dict[str, Any]) -> dict[str, int | None]:
    formats = info.get("formats") or []
    duration = info.get("duration")
    estimates = {"mp3": None, "mp4": None}

    best_audio = pick_best_audio_format(formats)
    if best_audio:
        estimates["mp3"] = estimate_audio_size_for_bitrate(duration, 192) or estimate_size_from_format(best_audio, duration)

    best_video = pick_best_mp4_video_format(formats)
    best_m4a_audio = pick_best_m4a_audio_format(formats)
    if best_video and best_m4a_audio:
        video_size = estimate_size_from_format(best_video, duration) or 0
        audio_size = estimate_size_from_format(best_m4a_audio, duration) or 0
        estimates["mp4"] = video_size + audio_size
    else:
        muxed_mp4 = [fmt for fmt in formats if fmt.get("ext") == "mp4" and fmt.get("vcodec") not in {None, "none"}]
        if muxed_mp4:
            best_muxed = max(
                muxed_mp4,
                key=lambda fmt: (
                    fmt.get("height") or 0,
                    fmt.get("fps") or 0,
                    fmt.get("tbr") or 0,
                ),
            )
            estimates["mp4"] = estimate_size_from_format(best_muxed, duration)

    return estimates


def estimate_download_sizes(info: dict[str, Any]) -> dict[str, int | None]:
    if is_playlist_info(info):
        total_mp3 = 0
        total_mp4 = 0
        found_mp3 = False
        found_mp4 = False

        for entry in info.get("entries", []):
            if not entry:
                continue
            entry_estimates = estimate_single_download_sizes(entry)
            if entry_estimates["mp3"]:
                total_mp3 += entry_estimates["mp3"] or 0
                found_mp3 = True
            if entry_estimates["mp4"]:
                total_mp4 += entry_estimates["mp4"] or 0
                found_mp4 = True

        return {
            "mp3": total_mp3 if found_mp3 else None,
            "mp4": total_mp4 if found_mp4 else None,
        }

    return estimate_single_download_sizes(info)


def extract_artist(info: dict[str, Any]) -> str | None:
    for field in ("artist", "channel", "uploader", "creator"):
        value = info.get(field)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def extract_resolution_label(info: dict[str, Any]) -> str | None:
    requested_downloads = info.get("requested_downloads") or []
    for item in requested_downloads:
        height = item.get("height")
        width = item.get("width")
        if height:
            return f"{width}x{height}" if width else f"{height}p"

    height = info.get("height")
    width = info.get("width")
    if height:
        return f"{width}x{height}" if width else f"{height}p"
    return None


def extract_audio_quality_label(info: dict[str, Any]) -> str | None:
    abr = info.get("abr")
    if abr:
        return f"{int(abr)}kbps"
    return "192kbps"


def get_mp4_download_choices(info: dict[str, Any], upload_limit_bytes: int | None = None) -> list[dict[str, Any]]:
    limit = upload_limit_bytes if upload_limit_bytes is not None else get_upload_limit()
    formats = info.get("formats") or []
    duration = info.get("duration")
    video_candidates = [
        fmt for fmt in formats
        if fmt.get("vcodec") not in {None, "none"} and fmt.get("acodec") == "none"
    ]
    audio_candidates = [
        fmt for fmt in formats
        if fmt.get("vcodec") == "none" and fmt.get("acodec") not in {None, "none"}
    ]

    preferred_audio = sorted(
        audio_candidates,
        key=lambda fmt: (fmt.get("ext") in {"m4a", "mp4"}, fmt.get("abr") or 0, fmt.get("tbr") or 0),
        reverse=True,
    )
    preferred_video = sorted(
        video_candidates,
        key=lambda fmt: (fmt.get("ext") == "mp4", fmt.get("height") or 0, fmt.get("fps") or 0, fmt.get("tbr") or 0),
        reverse=True,
    )

    by_height: dict[int, dict[str, Any]] = {}

    for video_fmt in preferred_video:
        video_size = estimate_size_from_format(video_fmt, duration)
        if not video_size:
            continue
        for audio_fmt in preferred_audio:
            audio_size = estimate_size_from_format(audio_fmt, duration)
            if not audio_size:
                continue
            total_size = video_size + audio_size
            height = video_fmt.get("height") or 0
            current = by_height.get(height)
            candidate = {
                "format_selector": f"{video_fmt['format_id']}+{audio_fmt['format_id']}",
                "height": height,
                "estimated_size": total_size,
                "within_limit": total_size <= limit,
            }
            if current is None or candidate["estimated_size"] < current["estimated_size"]:
                by_height[height] = candidate
            break

    muxed_candidates = [
        fmt for fmt in formats
        if fmt.get("ext") == "mp4" and fmt.get("vcodec") not in {None, "none"}
    ]
    muxed_candidates = sorted(
        muxed_candidates,
        key=lambda fmt: (fmt.get("height") or 0, fmt.get("fps") or 0, fmt.get("tbr") or 0),
        reverse=True,
    )
    for muxed_fmt in muxed_candidates:
        total_size = estimate_size_from_format(muxed_fmt, duration)
        if total_size:
            height = muxed_fmt.get("height") or 0
            by_height.setdefault(height, {
                "format_selector": muxed_fmt["format_id"],
                "height": height,
                "estimated_size": total_size,
                "within_limit": total_size <= limit,
            })

    return sorted(
        by_height.values(),
        key=lambda choice: (choice.get("height") or 0, choice.get("estimated_size") or 0),
        reverse=True,
    )


def get_mp3_download_choices(info: dict[str, Any], upload_limit_bytes: int | None = None) -> list[dict[str, Any]]:
    limit = upload_limit_bytes if upload_limit_bytes is not None else get_upload_limit()
    duration = info.get("duration")
    choices = []
    for bitrate in MP3_BITRATE_CHOICES:
        estimated_size = estimate_audio_size_for_bitrate(duration, bitrate)
        if estimated_size:
            choices.append(
                {
                    "bitrate": bitrate,
                    "estimated_size": estimated_size,
                    "within_limit": estimated_size <= limit,
                }
            )
    return choices


def plan_download(info: dict[str, Any], target_format: str) -> dict[str, Any]:
    if target_format == "mp3":
        choices = get_mp3_download_choices(info)
        choices = [choice for choice in choices if choice["within_limit"]]
        if not choices:
            raise ValueError("Mesmo reduzindo o bitrate, o MP3 estimado ainda ultrapassa o limite do Telegram.")
        return {"target_format": "mp3", **choices[0]}

    choices = get_mp4_download_choices(info)
    choices = [choice for choice in choices if choice["within_limit"]]
    if not choices:
        raise ValueError("Nao encontrei uma versao MP4 pequena o suficiente para o limite do Telegram.")
    return {
        "target_format": "mp4",
        **choices[0],
    }


def resolve_output_path(download_dir: str, info: dict[str, Any], target_format: str) -> Path:
    expected_suffix = ".mp3" if target_format == "mp3" else ".mp4"
    normalized_stem = sanitize_filename_stem(info.get("title") or "download")

    expected_path = Path(download_dir) / f"{normalized_stem}{expected_suffix}"
    if expected_path.exists():
        return expected_path

    candidates = [
        path
        for path in Path(download_dir).iterdir()
        if path.is_file() and not path.name.endswith((".part", ".ytdl"))
    ]

    preferred = [path for path in candidates if path.suffix.lower() == expected_suffix]
    if preferred:
        return max(preferred, key=lambda path: path.stat().st_size)
    if candidates:
        return max(candidates, key=lambda path: path.stat().st_size)

    raise FileNotFoundError("Nao foi possivel localizar o arquivo baixado.")


def is_playlist_info(info: dict[str, Any]) -> bool:
    return info.get("_type") == "playlist" and bool(info.get("entries"))


def inspect_link(link: str) -> dict[str, Any]:
    options = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
    }
    with yt_dlp.YoutubeDL(options) as ydl:
        return ydl.extract_info(link, download=False)


def summarize_link_info(info: dict[str, Any]) -> dict[str, Any]:
    size_estimates = estimate_download_sizes(info)
    if is_playlist_info(info):
        entries = [entry for entry in info.get("entries", []) if entry]
        return {
            "kind": "playlist",
            "title": info.get("title") or "Playlist",
            "entry_count": len(entries),
            "url": info.get("webpage_url") or info.get("original_url"),
            "size_estimates": size_estimates,
        }

    return {
        "kind": "single",
        "title": info.get("title") or "Video",
        "entry_count": 1,
        "url": info.get("webpage_url") or info.get("original_url"),
        "size_estimates": size_estimates,
    }


def has_download_metadata(info: dict[str, Any] | None) -> bool:
    if not info:
        return False
    if info.get("formats"):
        return True
    return bool(info.get("duration")) and not is_playlist_info(info)


def apply_download_plan(
    options: dict[str, Any],
    info: dict[str, Any],
    target_format: str,
    selected_value: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    if target_format == "mp3" and selected_value is not None:
        matching = next((choice for choice in get_mp3_download_choices(info) if choice["bitrate"] == int(selected_value)), None)
        if not matching:
            raise ValueError("A opcao de MP3 escolhida nao esta mais disponivel.")
        if not matching["within_limit"]:
            raise ValueError("Essa opcao de MP3 excede o limite atual de upload do Telegram. Escolha uma qualidade menor.")
        plan = {
            "target_format": "mp3",
            **matching,
        }
    elif target_format == "mp4" and selected_value is not None:
        # O usuario escolheu explicitamente uma qualidade (mesmo com aviso [limite]).
        # Removemos o bloqueio preventivo: se a estimativa estiver errada pra baixo,
        # o download pode funcionar. Caso o arquivo final realmente passe do limite,
        # send_download() vai recusar o upload com uma mensagem clara.
        matching = next((choice for choice in get_mp4_download_choices(info) if choice["format_selector"] == selected_value), None)
        if not matching:
            raise ValueError("A opcao de MP4 escolhida nao esta mais disponivel.")
        plan = {"target_format": "mp4", **matching}
    else:
        plan = plan_download(info, target_format)

    planned_options = dict(options)
    if target_format == "mp3":
        planned_options["postprocessors"] = [
            {
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": str(plan["bitrate"]),
            }
        ]
    else:
        planned_options["format"] = plan["format_selector"]
    return plan, planned_options


def describe_download_plan(plan: dict[str, Any], target_format: str) -> str:
    estimated_size = format_size_label(plan.get("estimated_size"))
    within_limit = plan.get("within_limit", True)
    if target_format == "mp3":
        return f"MP3 {plan['bitrate']} kbps. Tamanho estimado: {estimated_size}."
    base = (
        f"MP4 {plan.get('height') or '?'}p. "
        f"Tamanho estimado: {estimated_size}."
    )
    if not within_limit:
        base += (
            "\n\nAtencao: a estimativa ultrapassa o limite de 49 MB do Telegram. "
            "O download vai acontecer mesmo assim — se o arquivo final passar do limite, "
            "o envio sera cancelado."
        )
    return base


def build_mp3_quality_keyboard(info: dict[str, Any]) -> types.InlineKeyboardMarkup:
    markup = types.InlineKeyboardMarkup()
    choices = get_mp3_download_choices(info)
    for choice in choices:
        warning = "" if choice["within_limit"] else " [limite]"
        markup.add(
            types.InlineKeyboardButton(
                f"{choice['bitrate']} kbps ({format_size_label(choice['estimated_size'])}){warning}",
                callback_data=f"dl:mp3:{choice['bitrate']}",
            )
        )
    markup.add(types.InlineKeyboardButton("Voltar", callback_data="back:formats"))
    return markup


def build_mp4_quality_keyboard(info: dict[str, Any]) -> types.InlineKeyboardMarkup:
    markup = types.InlineKeyboardMarkup()
    choices = get_mp4_download_choices(info)
    for choice in choices:
        warning = "" if choice["within_limit"] else " [limite]"
        markup.add(
            types.InlineKeyboardButton(
                f"{choice['height']}p ({format_size_label(choice['estimated_size'])}){warning}",
                callback_data=f"dl:mp4:{choice['format_selector']}",
            )
        )
    markup.add(types.InlineKeyboardButton("Voltar", callback_data="back:formats"))
    return markup


def build_primary_download_keyboard(info: dict[str, Any]) -> types.InlineKeyboardMarkup:
    markup = types.InlineKeyboardMarkup()

    mp3_choices = get_mp3_download_choices(info)
    mp4_choices = get_mp4_download_choices(info)
    best_mp3 = next((choice for choice in mp3_choices if choice["within_limit"]), mp3_choices[0] if mp3_choices else None)
    best_mp4 = next((choice for choice in mp4_choices if choice["within_limit"]), mp4_choices[0] if mp4_choices else None)

    if best_mp3:
        warning = "" if best_mp3["within_limit"] else " [limite]"
        markup.add(
            types.InlineKeyboardButton(
                f"Baixar MP3 {best_mp3['bitrate']} kbps ({format_size_label(best_mp3['estimated_size'])}){warning}",
                callback_data=f"dl:mp3:{best_mp3['bitrate']}",
            )
        )
    if best_mp4:
        warning = "" if best_mp4["within_limit"] else " [limite]"
        markup.add(
            types.InlineKeyboardButton(
                f"Baixar MP4 {best_mp4['height']}p ({format_size_label(best_mp4['estimated_size'])}){warning}",
                callback_data=f"dl:mp4:{best_mp4['format_selector']}",
            )
        )

    markup.add(
        types.InlineKeyboardButton("Outras qualidades MP3", callback_data="fmt:mp3"),
        types.InlineKeyboardButton("Outras qualidades MP4", callback_data="fmt:mp4"),
    )
    return markup


def has_within_limit_choice(choices: list[dict[str, Any]]) -> bool:
    return any(choice.get("within_limit") for choice in choices)


def send_download(
    bot: telebot.TeleBot,
    chat_id: int,
    link: str,
    target_format: str,
    info: dict[str, Any] | None = None,
    selected_value: str | None = None,
    notify_plan: bool = True,
) -> None:
    with tempfile.TemporaryDirectory(prefix="telegram_bot_") as download_dir:
        if not has_download_metadata(info):
            info = inspect_link(link)
        ydl_opts = build_download_options(target_format, download_dir)
        plan, ydl_opts = apply_download_plan(ydl_opts, info, target_format, selected_value=selected_value)
        log_event("download_started", chat_id=chat_id, format=target_format, url=short_text(link))
        log_event(
            "download_plan",
            chat_id=chat_id,
            format=target_format,
            estimated_size=format_size_label(plan.get("estimated_size")),
            quality=(f"{plan['bitrate']}kbps" if target_format == "mp3" else f"{plan.get('height') or '?'}p"),
        )
        if notify_plan:
            bot.send_message(chat_id, describe_download_plan(plan, target_format))

        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(link, download=True)

        output_path = resolve_output_path(download_dir, info, target_format)
        artist = extract_artist(info)
        quality = extract_audio_quality_label(info) if target_format == "mp3" else extract_resolution_label(info)
        file_size_mb = output_path.stat().st_size / (1024 * 1024)
        log_event(
            "download_finished",
            chat_id=chat_id,
            format=target_format,
            title=short_text(info.get("title") or "sem_titulo"),
            artist=short_text(artist or "desconhecido"),
            quality=quality or "desconhecida",
            file=output_path.name,
            size_mb=f"{file_size_mb:.2f}",
        )

        if output_path.stat().st_size > get_upload_limit():
            raise ValueError(
                "O arquivo final ainda ficou maior que o limite do Telegram "
                f"({format_size_label(output_path.stat().st_size)}). "
                "Tente novamente escolhendo uma qualidade menor."
            )

        with output_path.open("rb") as media_file:
            if target_format == "mp3":
                log_event("upload_started", chat_id=chat_id, media_type="audio", file=output_path.name)
                bot.send_audio(
                    chat_id,
                    media_file,
                    title=info.get("title"),
                    performer=artist,
                )
                log_event("upload_finished", chat_id=chat_id, media_type="audio", file=output_path.name)
                return
            log_event("upload_started", chat_id=chat_id, media_type="video", file=output_path.name)
            bot.send_video(chat_id, media_file, supports_streaming=True)
            log_event("upload_finished", chat_id=chat_id, media_type="video", file=output_path.name)


def send_playlist_download(
    bot: telebot.TeleBot,
    chat_id: int,
    link: str,
    target_format: str,
    selected_value: str | None = None,
) -> None:
    info = inspect_link(link)
    entries = [entry for entry in info.get("entries", []) if entry]
    if not entries:
        raise FileNotFoundError("Nao foi possivel localizar itens na playlist.")

    log_event(
        "playlist_download_started",
        chat_id=chat_id,
        format=target_format,
        url=short_text(link),
        items=len(entries),
    )

    for index, entry_info in enumerate(entries, start=1):
        entry_url = entry_info.get("webpage_url") or entry_info.get("url")
        title = entry_info.get("title") or f"Item {index}"
        if not entry_url:
            log_event("playlist_item_skipped", chat_id=chat_id, index=index, reason="missing_url")
            bot.send_message(chat_id, f"Pulando item {index}: nao encontrei a URL do video.")
            continue

        bot.send_message(chat_id, f"Processando item {index}/{len(entries)}: {title}")
        try:
            send_download(
                bot,
                chat_id,
                entry_url,
                target_format,
                info=entry_info,
                selected_value=selected_value,
                notify_plan=True,
            )
        except ValueError as exc:
            log_event("playlist_item_too_large", chat_id=chat_id, index=index, title=short_text(title), reason=short_text(str(exc)))
            bot.send_message(chat_id, f"Nao consegui enviar o item {index}/{len(entries)} porque ele ficou grande demais. {exc}")
        except Exception:
            logger.exception("Falha ao processar item %s da playlist no chat %s", index, chat_id)
            bot.send_message(chat_id, f"O item {index}/{len(entries)} falhou durante o processamento.")


def register_handlers(bot: telebot.TeleBot) -> telebot.TeleBot:
    @bot.message_handler(commands=["start", "help"])
    def show_help(message):
        log_event("command_received", chat_id=message.chat.id, command=message.text.lower())
        bot.send_message(
            message.chat.id,
            (
                "Envie um link de video e eu vou oferecer as opcoes MP3 e MP4.\n"
                "O bot usa yt-dlp, entao costuma funcionar com YouTube e outras plataformas suportadas."
            ),
        )

    @bot.message_handler(func=lambda message: bool(message.text) and message.text.lower() == "ping")
    def ping(message):
        log_event("command_received", chat_id=message.chat.id, command="ping")
        bot.reply_to(message, "pong")

    @bot.message_handler(func=lambda message: bool(message.text))
    def handle_message(message):
        try:
            if message.text.lower() == "ping":
                return

            link = message.text.strip()
            log_event("message_received", chat_id=message.chat.id, text=short_text(link))
            if not is_valid_url(link):
                log_event("message_rejected", chat_id=message.chat.id, reason="invalid_url")
                bot.send_message(message.chat.id, "Por favor, envie um link valido.")
                return

            info = inspect_link(link)
            link_data = summarize_link_info(info)
            link_data["url"] = link
            link_data["info"] = info
            user_data[message.chat.id] = link_data
            log_event(
                "link_stored",
                chat_id=message.chat.id,
                url=short_text(link),
                kind=link_data["kind"],
                items=link_data["entry_count"],
                mp3_size=format_size_label(link_data["size_estimates"].get("mp3")),
                mp4_size=format_size_label(link_data["size_estimates"].get("mp4")),
            )

            if link_data["kind"] == "playlist":
                bot.send_message(
                    message.chat.id,
                    (
                        f"Playlist detectada: {link_data['title']}\n"
                        f"Itens encontrados: {link_data['entry_count']}\n"
                        "Escolha o formato abaixo para baixar e enviar item por item."
                    ),
                    reply_markup=build_playlist_download_keyboard(link_data["size_estimates"]),
                )
            else:
                bot.send_message(
                    message.chat.id,
                    f"Video detectado: {link_data['title']}\nEscolha o formato abaixo:",
                    reply_markup=build_primary_download_keyboard(info),
                )
        except DownloadError:
            logger.exception("Nao foi possivel inspecionar o link do chat %s", message.chat.id)
            bot.send_message(
                message.chat.id,
                "Nao consegui ler esse link. Verifique se ele esta publico e se a plataforma e suportada.",
            )
        except Exception:
            logger.exception("Erro ao processar a mensagem do chat %s", message.chat.id)
            bot.send_message(message.chat.id, "Ocorreu um erro ao processar o link.")

    @bot.callback_query_handler(func=lambda call: call.data.startswith(("fmt:", "dl:", "back:")))
    def handle_callback(call):
        chat_id = call.message.chat.id
        link_data = user_data.get(chat_id)
        log_event("callback_received", chat_id=chat_id, action=call.data)
        should_clear_state = False

        if not link_data:
            log_event("callback_rejected", chat_id=chat_id, reason="missing_link")
            bot.answer_callback_query(call.id, "Envie o link novamente.")
            bot.send_message(chat_id, "Link nao encontrado. Por favor, envie o link novamente.")
            return

        # Evita processar o mesmo chat duas vezes em paralelo (clique duplo / retry do Telegram)
        if call.data.startswith("dl:") and chat_id in _active_downloads:
            log_event("callback_ignored", chat_id=chat_id, reason="download_already_active")
            bot.answer_callback_query(call.id, "Ja estou processando sua solicitacao, aguarde.")
            return

        try:
            if call.data == "back:formats":
                bot.answer_callback_query(call.id, "Voltando para formatos.")
                text = (
                    f"Playlist detectada: {link_data['title']}\n"
                    f"Itens encontrados: {link_data['entry_count']}\n"
                    "Escolha o formato abaixo para baixar e enviar item por item."
                ) if link_data["kind"] == "playlist" else f"Video detectado: {link_data['title']}\nEscolha o formato abaixo:"
                reply_markup = (
                    build_playlist_download_keyboard(link_data["size_estimates"])
                    if link_data["kind"] == "playlist"
                    else build_primary_download_keyboard(link_data["info"])
                )
                bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=call.message.message_id,
                    text=text,
                    reply_markup=reply_markup,
                )
                return

            callback_type, selected_format, *rest = call.data.split(":", 2)
            if callback_type == "fmt":
                if link_data["kind"] == "playlist":
                    raise ValueError("Selecao de qualidade detalhada esta disponivel apenas para links de video unico.")
                bot.answer_callback_query(call.id, f"Escolhendo opcoes de {selected_format.upper()}...")
                info = link_data["info"]
                if selected_format == "mp3":
                    choices = get_mp3_download_choices(info)
                    if not choices:
                        raise ValueError("Nao encontrei uma opcao MP3 que caiba no limite do Telegram.")
                    reply_markup = build_mp3_quality_keyboard(info)
                    prompt = "Escolha a qualidade do MP3:"
                    if not has_within_limit_choice(choices):
                        prompt += "\n\nNenhuma opcao de MP3 cabe no limite atual da Bot API publica."
                else:
                    choices = get_mp4_download_choices(info)
                    if not choices:
                        raise ValueError("Nao encontrei uma opcao MP4 que caiba no limite do Telegram.")
                    reply_markup = build_mp4_quality_keyboard(info)
                    prompt = "Escolha a qualidade do MP4:"
                    if not has_within_limit_choice(choices):
                        prompt += (
                            "\n\nNenhuma opcao de MP4 cabe no limite de 49 MB da Bot API publica. "
                            "Voce pode tentar mesmo assim — se a estimativa estiver errada, pode funcionar."
                        )

                bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=call.message.message_id,
                    text=prompt,
                    reply_markup=reply_markup,
                )
                return

            selected_value = rest[0] if rest else None
            bot.answer_callback_query(call.id, f"Preparando {selected_format.upper()}...")
            _active_downloads.add(chat_id)
            if link_data["kind"] == "playlist":
                bot.send_message(
                    chat_id,
                    (
                        f"Baixando playlist com {link_data['entry_count']} itens em {selected_format.upper()}.\n"
                        "Vou enviar os arquivos um por um."
                    ),
                )
                send_playlist_download(
                    bot,
                    chat_id,
                    link_data["url"],
                    selected_format,
                    selected_value=selected_value,
                )
                should_clear_state = True
            else:
                bot.send_message(chat_id, f"Baixando seu arquivo em {selected_format.upper()}. Aguarde um instante.")
                send_download(
                    bot,
                    chat_id,
                    link_data["url"],
                    selected_format,
                    info=link_data["info"],
                    selected_value=selected_value,
                )
                should_clear_state = True
            log_event(
                "request_completed",
                chat_id=chat_id,
                format=selected_format,
                kind=link_data["kind"],
            )
        except DownloadError:
            logger.exception("Falha do yt-dlp no chat %s", chat_id)
            bot.send_message(
                chat_id,
                (
                    "Nao consegui baixar esse link. Verifique se a URL e suportada "
                    "ou se o FFmpeg esta instalado para conversao em MP3."
                ),
            )
        except FileNotFoundError:
            logger.exception("Arquivo baixado nao encontrado no chat %s", chat_id)
            bot.send_message(chat_id, "O download terminou, mas eu nao encontrei o arquivo gerado.")
        except ValueError as exc:
            log_event("request_rejected", chat_id=chat_id, action=call.data, reason=short_text(str(exc)))
            bot.send_message(chat_id, str(exc))
        except ApiTelegramException:
            logger.exception("Falha ao enviar arquivo para o Telegram no chat %s", chat_id)
            bot.send_message(
                chat_id,
                "O arquivo foi baixado, mas o Telegram recusou o envio. Ele pode ser grande demais."
            )
        except RequestsConnectionError:
            logger.exception("Falha de conexao durante upload para o Telegram no chat %s", chat_id)
            bot.send_message(
                chat_id,
                (
                    "O upload para o Telegram falhou por conexao ou timeout. "
                    "Se o arquivo for grande, reinicie o bot atualizado e tente novamente; "
                    "agora ele deve escolher uma versao menor automaticamente."
                ),
            )
        except Exception:
            logger.exception("Erro ao processar a escolha do chat %s", chat_id)
            bot.send_message(chat_id, "Ocorreu um erro ao processar sua escolha.")
        finally:
            _active_downloads.discard(chat_id)
            if should_clear_state:
                user_data.pop(chat_id, None)
                log_event("chat_state_cleared", chat_id=chat_id)

    return bot


def main() -> None:
    token = get_bot_token()
    using_local_server = bool(get_local_api_url())
    start_local_server(token)
    bot = register_handlers(create_bot(token))
    me = bot.get_me()
    logger.info("")
    logger.info("==============================================")
    logger.info("Bot iniciado com sucesso")
    logger.info("Nome: %s", me.first_name)
    logger.info("Username: @%s", me.username)
    logger.info("ID: %s", me.id)
    logger.info("Status: aguardando mensagens no Telegram")
    if using_local_server:
        logger.info("Modo: servidor Bot API local gerenciado (%s)", get_local_api_url())
    else:
        logger.info("Modo: API publica do Telegram")
    logger.info("Upload limit configurado: %s", format_size_label(get_upload_limit()))
    logger.info("Arquivo de log: %s", LOG_FILE)
    logger.info("Pressione Ctrl+C para encerrar")
    logger.info("==============================================")
    logger.info("")
    try:
        bot.infinity_polling(skip_pending=True, timeout=30, long_polling_timeout=30)
    except ApiTelegramException as exc:
        if exc.error_code == 409:
            logger.error("Outra instancia do bot ja esta rodando com este token.")
            logger.error("Encerre a outra execucao antes de iniciar uma nova.")
            return
        raise
    except KeyboardInterrupt:
        logger.info("Bot encerrado pelo usuario.")
    finally:
        if using_local_server:
            stop_local_server()


if __name__ == "__main__":
    main()
