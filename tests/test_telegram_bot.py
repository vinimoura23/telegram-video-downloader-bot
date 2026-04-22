import os
import tempfile
import unittest
from pathlib import Path

from telegram_bot import (
    TELEGRAM_LOCAL_API_URL_ENV,
    TELEGRAM_LOCAL_SERVER_ENABLED_ENV,
    TELEGRAM_LOCAL_UPLOAD_LIMIT_BYTES,
    TELEGRAM_TOKEN_ENV,
    TELEGRAM_UPLOAD_LIMIT_BYTES,
    build_download_options,
    build_mp4_quality_keyboard,
    build_playlist_download_keyboard,
    build_primary_download_keyboard,
    describe_download_plan,
    estimate_download_sizes,
    estimate_single_download_sizes,
    format_size_label,
    extract_audio_quality_label,
    extract_artist,
    extract_resolution_label,
    get_bot_token,
    get_upload_limit,
    is_valid_url,
    is_playlist_info,
    resolve_output_path,
    sanitize_filename_stem,
    summarize_link_info,
)


class TelegramBotHelpersTest(unittest.TestCase):
    def setUp(self):
        self._saved_env = {
            TELEGRAM_LOCAL_SERVER_ENABLED_ENV: os.environ.get(TELEGRAM_LOCAL_SERVER_ENABLED_ENV),
            TELEGRAM_LOCAL_API_URL_ENV: os.environ.get(TELEGRAM_LOCAL_API_URL_ENV),
        }

    def tearDown(self):
        for name, value in self._saved_env.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value

    def disable_local_server(self):
        os.environ.pop(TELEGRAM_LOCAL_SERVER_ENABLED_ENV, None)
        os.environ.pop(TELEGRAM_LOCAL_API_URL_ENV, None)

    def test_is_valid_url_accepts_http_and_https(self):
        self.assertTrue(is_valid_url("https://example.com/video"))
        self.assertTrue(is_valid_url("http://example.com/video"))
        self.assertFalse(is_valid_url("ftp://example.com/video"))
        self.assertFalse(is_valid_url("example.com/video"))

    def test_get_bot_token_reads_environment(self):
        previous = os.environ.get(TELEGRAM_TOKEN_ENV)
        os.environ[TELEGRAM_TOKEN_ENV] = "token-de-teste"
        try:
            self.assertEqual(get_bot_token(), "token-de-teste")
        finally:
            if previous is None:
                os.environ.pop(TELEGRAM_TOKEN_ENV, None)
            else:
                os.environ[TELEGRAM_TOKEN_ENV] = previous

    def test_get_bot_token_raises_when_missing(self):
        previous = os.environ.pop(TELEGRAM_TOKEN_ENV, None)
        try:
            with self.assertRaises(RuntimeError):
                get_bot_token()
        finally:
            if previous is not None:
                os.environ[TELEGRAM_TOKEN_ENV] = previous

    def test_build_download_options_for_mp3(self):
        options = build_download_options("mp3", "/tmp/downloads")
        self.assertEqual(options["format"], "bestaudio/best")
        self.assertTrue(options["writethumbnail"])
        self.assertTrue(options["embedthumbnail"])
        self.assertTrue(options["addmetadata"])
        self.assertEqual(options["parse_metadata"][0], "%(channel|uploader|creator|artist|)s:%(meta_artist)s")
        self.assertEqual(options["postprocessors"][0]["preferredcodec"], "mp3")

    def test_build_download_options_for_mp4(self):
        options = build_download_options("mp4", "/tmp/downloads")
        self.assertEqual(
            options["format"],
            "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/bestvideo+bestaudio/best",
        )
        self.assertEqual(options["merge_output_format"], "mp4")

    def test_build_download_options_for_playlist(self):
        options = build_download_options("mp3", "/tmp/downloads", is_playlist=True)
        self.assertFalse(options["noplaylist"])
        self.assertIn("%(playlist_index)03d", options["outtmpl"])

    def test_format_size_label(self):
        self.assertEqual(format_size_label(None), "tamanho incerto")
        self.assertEqual(format_size_label(1024), "1.0 KB")
        self.assertEqual(format_size_label(5 * 1024 * 1024), "5.0 MB")

    def test_build_playlist_download_keyboard_includes_estimates(self):
        keyboard = build_playlist_download_keyboard({"mp3": 5 * 1024 * 1024, "mp4": 20 * 1024 * 1024})
        buttons = keyboard.keyboard[0]
        self.assertIn("playlist em MP3", buttons[0].text)
        self.assertIn("5.0 MB", buttons[0].text)
        self.assertIn("20.0 MB", buttons[1].text)

    def test_build_primary_download_keyboard_shows_best_first_and_other_options(self):
        self.disable_local_server()
        info = {
            "duration": 100,
            "formats": [
                {"format_id": "137", "vcodec": "avc1", "acodec": "none", "ext": "mp4", "height": 1080, "tbr": 6000},
                {"format_id": "136", "vcodec": "avc1", "acodec": "none", "ext": "mp4", "height": 720, "tbr": 2000},
                {"format_id": "135", "vcodec": "avc1", "acodec": "none", "ext": "mp4", "height": 480, "tbr": 900},
                {"format_id": "140", "vcodec": "none", "acodec": "mp4a.40.2", "ext": "m4a", "abr": 128},
            ],
        }
        keyboard = build_primary_download_keyboard(info)
        texts = [button.text for row in keyboard.keyboard for button in row]
        self.assertIn("Baixar MP4 720p", texts[1])
        self.assertTrue(any("Outras qualidades MP3" in text for text in texts))
        self.assertTrue(any("Outras qualidades MP4" in text for text in texts))

    def test_resolve_output_path_prefers_expected_extension(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            info = {"title": "Meu Video Legal"}
            mp4_file = Path(tmpdir) / "Meu_Video_Legal.mp4"
            txt_file = Path(tmpdir) / "Meu_Video_Legal.txt"
            txt_file.write_text("nao")
            mp4_file.write_text("video")

            resolved = resolve_output_path(tmpdir, info, "mp4")
            self.assertEqual(resolved, mp4_file)

    def test_resolve_output_path_raises_without_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertRaises(FileNotFoundError):
                resolve_output_path(tmpdir, {"title": "Nada"}, "mp3")

    def test_extract_artist_uses_best_available_field(self):
        self.assertEqual(extract_artist({"channel": "Canal X"}), "Canal X")
        self.assertEqual(extract_artist({"uploader": "Uploader Y"}), "Uploader Y")
        self.assertEqual(extract_artist({"artist": "Artista Z"}), "Artista Z")
        self.assertIsNone(extract_artist({}))

    def test_extract_resolution_label(self):
        self.assertEqual(extract_resolution_label({"width": 1920, "height": 1080}), "1920x1080")
        self.assertEqual(
            extract_resolution_label({"requested_downloads": [{"width": 1280, "height": 720}]}),
            "1280x720",
        )
        self.assertIsNone(extract_resolution_label({}))

    def test_extract_audio_quality_label(self):
        self.assertEqual(extract_audio_quality_label({"abr": 160}), "160kbps")
        self.assertEqual(extract_audio_quality_label({}), "192kbps")

    def test_sanitize_filename_stem(self):
        self.assertEqual(sanitize_filename_stem("Meu Video Legal"), "Meu_Video_Legal")
        self.assertEqual(sanitize_filename_stem("!!!"), "download")

    def test_is_playlist_info(self):
        self.assertTrue(is_playlist_info({"_type": "playlist", "entries": [{"title": "x"}]}))
        self.assertFalse(is_playlist_info({"_type": "video"}))

    def test_summarize_link_info_for_playlist(self):
        summary = summarize_link_info(
            {
                "_type": "playlist",
                "title": "Minha Playlist",
                "entries": [{"title": "1"}, {"title": "2"}],
                "webpage_url": "https://example.com/p",
            }
        )
        self.assertEqual(summary["kind"], "playlist")
        self.assertEqual(summary["entry_count"], 2)
        self.assertEqual(summary["title"], "Minha Playlist")

    def test_summarize_link_info_for_single_video(self):
        summary = summarize_link_info(
            {
                "title": "Meu Video",
                "webpage_url": "https://example.com/v",
            }
        )
        self.assertEqual(summary["kind"], "single")
        self.assertEqual(summary["entry_count"], 1)
        self.assertEqual(summary["title"], "Meu Video")

    def test_estimate_single_download_sizes(self):
        info = {
            "duration": 100,
            "formats": [
                {"vcodec": "none", "acodec": "mp4a.40.2", "ext": "m4a", "abr": 128},
                {"vcodec": "avc1", "acodec": "none", "ext": "mp4", "height": 720, "tbr": 2000},
            ],
        }
        estimates = estimate_single_download_sizes(info)
        self.assertIsNotNone(estimates["mp3"])
        self.assertIsNotNone(estimates["mp4"])
        self.assertGreater(estimates["mp4"], estimates["mp3"])

    def test_estimate_download_sizes_for_playlist(self):
        info = {
            "_type": "playlist",
            "entries": [
                {
                    "duration": 100,
                    "formats": [
                        {"vcodec": "none", "acodec": "mp4a.40.2", "ext": "m4a", "abr": 128},
                        {"vcodec": "avc1", "acodec": "none", "ext": "mp4", "height": 720, "tbr": 2000},
                    ],
                },
                {
                    "duration": 50,
                    "formats": [
                        {"vcodec": "none", "acodec": "mp4a.40.2", "ext": "m4a", "abr": 96},
                        {"vcodec": "avc1", "acodec": "none", "ext": "mp4", "height": 480, "tbr": 1000},
                    ],
                },
            ],
        }
        estimates = estimate_download_sizes(info)
        self.assertIsNotNone(estimates["mp3"])
        self.assertIsNotNone(estimates["mp4"])

    def test_build_mp4_quality_keyboard_shows_limited_and_unlimited_options(self):
        self.disable_local_server()
        info = {
            "duration": 100,
            "formats": [
                {"format_id": "137", "vcodec": "avc1", "acodec": "none", "ext": "mp4", "height": 1080, "tbr": 6000},
                {"format_id": "136", "vcodec": "avc1", "acodec": "none", "ext": "mp4", "height": 720, "tbr": 2000},
                {"format_id": "135", "vcodec": "avc1", "acodec": "none", "ext": "mp4", "height": 480, "tbr": 900},
                {"format_id": "140", "vcodec": "none", "acodec": "mp4a.40.2", "ext": "m4a", "abr": 128},
            ],
        }
        keyboard = build_mp4_quality_keyboard(info)
        texts = [button.text for row in keyboard.keyboard for button in row]
        self.assertTrue(any("1080p" in text and "[limite]" in text for text in texts))
        self.assertTrue(any("480p" in text and "[limite]" not in text for text in texts))

    def test_public_upload_limit_constant(self):
        self.assertEqual(TELEGRAM_UPLOAD_LIMIT_BYTES, 49 * 1024 * 1024)

    def test_get_upload_limit_uses_public_limit_by_default(self):
        self.disable_local_server()
        self.assertEqual(get_upload_limit(), TELEGRAM_UPLOAD_LIMIT_BYTES)

    def test_get_upload_limit_uses_local_limit_when_enabled(self):
        previous = os.environ.get(TELEGRAM_LOCAL_SERVER_ENABLED_ENV)
        os.environ[TELEGRAM_LOCAL_SERVER_ENABLED_ENV] = "1"
        try:
            self.assertEqual(get_upload_limit(), TELEGRAM_LOCAL_UPLOAD_LIMIT_BYTES)
        finally:
            if previous is None:
                os.environ.pop(TELEGRAM_LOCAL_SERVER_ENABLED_ENV, None)
            else:
                os.environ[TELEGRAM_LOCAL_SERVER_ENABLED_ENV] = previous

    def test_describe_download_plan_is_direct_for_mp3(self):
        text = describe_download_plan({"bitrate": 160, "estimated_size": 5 * 1024 * 1024}, "mp3")
        self.assertEqual(text, "MP3 160 kbps. Tamanho estimado: 5.0 MB.")

    def test_describe_download_plan_is_direct_for_mp4(self):
        text = describe_download_plan({"height": 720, "estimated_size": 20 * 1024 * 1024, "within_limit": True}, "mp4")
        self.assertEqual(text, "MP4 720p. Tamanho estimado: 20.0 MB.")


if __name__ == "__main__":
    unittest.main()
