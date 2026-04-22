import logging
import os
import sys
import tempfile
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import telebot
import yt_dlp
from dotenv import load_dotenv
from requests.exceptions import ConnectionError as RequestsConnectionError
from telebot import types
from telebot.apihelper import ApiTelegramException
from yt_dlp.utils import DownloadError


load_dotenv()

TELEGRAM_TOKEN_ENV = "TELEGRAM_BOT_TOKEN"
TELEGRAM_LOCAL_API_URL_ENV = "TELEGRAM_LOCAL_API_URL"
SUPPORTED_FORMATS = {"mp3", "mp4"}
LOG_DIR = Path("logs")
LOG_FILE = LOG_DIR / "bot.log"
TELEGRAM_UPLOAD_LIMIT_BYTES = 49 * 1024 * 1024        # limite da API publica
TELEGRAM_LOCAL_UPLOAD_LIMIT_BYTES = 2 * 1024 ** 3     # 2 GB — limite do servidor local
MP3_BITRATE_CHOICES = [192, 160, 128, 96, 64]

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


def get_local_api_url() -> str | None:
    url = os.getenv(TELEGRAM_LOCAL_API_URL_ENV, "").strip()
    return url or None


def get_upload_limit() -> int:
    return TELEGRAM_LOCAL_UPLOAD_LIMIT_BYTES if get_local_api_url() else TELEGRAM_UPLOAD_LIMIT_BYTES


def create_bot(token: str | None = None) -> telebot.TeleBot:
    local_url = get_local_api_url()
    bot = telebot.TeleBot(token or get_bot_token(), parse_mode=None)
    if local_url:
        telebot.apihelper.API_URL = f"{local_url.rstrip('/')}/bot{{0}}/{{1}}"
        telebot.apihelper.FILE_URL = f"{local_url.rstrip('/')}/file/bot{{0}}/{{1}}"
    return bot


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


def build_format_keyboard(size_estimates: dict[str, int | None] | None = None) -> types.InlineKeyboardMarkup:
    size_estimates = size_estimates or {}
    markup = types.InlineKeyboardMarkup()
    markup.add(
        types.InlineKeyboardButton(
            f"Baixar MP3 ({format_size_label(size_estimates.get('mp3'))})",
            callback_data="fmt:mp3",
        ),
        types.InlineKeyboardButton(
            f"Baixar MP4 ({format_size_label(size_estimates.get('mp4'))})",
            callback_data="fmt:mp4",
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


def choose_mp3_bitrate(info: dict[str, Any], upload_limit_bytes: int | None = None) -> int | None:
    limit = upload_limit_bytes if upload_limit_bytes is not None else get_upload_limit()
    duration = info.get("duration")
    for bitrate in MP3_BITRATE_CHOICES:
        estimated_size = estimate_audio_size_for_bitrate(duration, bitrate)
        if estimated_size and estimated_size <= limit:
            return bitrate
    return None


def choose_mp4_format(info: dict[str, Any], upload_limit_bytes: int | None = None) -> dict[str, Any] | None:
    choices = get_mp4_download_choices(info, upload_limit_bytes)
    choices = [choice for choice in choices if choice["within_limit"]]
    return choices[0] if choices else None


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


def resolve_output_paths(download_dir: str, target_format: str) -> list[Path]:
    expected_suffix = ".mp3" if target_format == "mp3" else ".mp4"
    candidates = [
        path
        for path in Path(download_dir).iterdir()
        if path.is_file()
        and path.suffix.lower() == expected_suffix
        and not path.name.endswith((".part", ".ytdl"))
    ]
    return sorted(candidates, key=lambda path: path.name)


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
            raise ValueError(
                "Essa opcao de MP3 excede o limite da Bot API publica do Telegram. "
                "Para arquivos grandes, so funciona bem usando um servidor Bot API local."
            )
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
        return f"Vou tentar em MP3 a {plan['bitrate']} kbps. Tamanho estimado: {estimated_size}."
    base = (
        f"Vou tentar em MP4 com resolucao aproximada de {plan.get('height') or '?'}p. "
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
        info = info or inspect_link(link)
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
                "Para videos grandes voce precisa de um servidor Bot API local."
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
                    reply_markup=build_primary_download_keyboard(info),
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
                bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=call.message.message_id,
                    text=text,
                    reply_markup=build_primary_download_keyboard(link_data["info"]),
                )
                return

            callback_type, selected_format, *rest = call.data.split(":", 2)
            if callback_type == "fmt":
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
    bot = register_handlers(create_bot())
    me = bot.get_me()
    local_url = get_local_api_url()
    logger.info("")
    logger.info("==============================================")
    logger.info("Bot iniciado com sucesso")
    logger.info("Nome: %s", me.first_name)
    logger.info("Username: @%s", me.username)
    logger.info("ID: %s", me.id)
    logger.info("Status: aguardando mensagens no Telegram")
    if local_url:
        logger.info("Modo: servidor local (%s)", local_url)
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


if __name__ == "__main__":
    main()