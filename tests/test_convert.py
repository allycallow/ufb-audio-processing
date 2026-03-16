import os
import subprocess
import tempfile
from unittest.mock import MagicMock, patch, call

import pytest

from task import (
    process_universal_cmaf,
    download_from_s3,
    upload_directory_to_s3,
    main,
    HLS_VARIANTS,
)


# --- process_universal_cmaf ---


class TestProcessUniversalCmaf:
    def test_calls_ffmpeg_and_packager(self, tmp_path, monkeypatch):
        """FFmpeg and Shaka Packager are called with correct arguments."""
        mp3_path = "/fake/input.mp3"
        output_dir = str(tmp_path / "cmaf")
        os.makedirs(output_dir)

        monkeypatch.setenv("DRM_KEY_SERVER_URL", "https://drm.example.com")
        packager_calls = []

        def fake_run(cmd, check):
            if cmd[0] == "ffmpeg":
                # Simulate creating the intermediate AAC file
                out_idx = cmd.index("-y") + 1
                aac_path = cmd[out_idx]
                with open(aac_path, "wb") as f:
                    f.write(b"aac")
            elif cmd[0] == "packager":
                packager_calls.append(cmd)
                # Simulate creating output files
                with open(os.path.join(output_dir, "master.m3u8"), "w") as f:
                    f.write("#EXTM3U\n")
                with open(os.path.join(output_dir, "manifest.mpd"), "w") as f:
                    f.write("<MPD>")
            return MagicMock(returncode=0)

        with patch("task.subprocess.run", side_effect=fake_run) as mock_run:
            process_universal_cmaf(mp3_path, output_dir, "contentid123")

        # Check that ffmpeg was called for each variant
        ffmpeg_calls = [c for c in mock_run.call_args_list if c[0][0][0] == "ffmpeg"]
        assert len(ffmpeg_calls) == len(HLS_VARIANTS)
        # Check that packager was called
        assert packager_calls
        # Check that output files exist
        assert (tmp_path / "cmaf" / "master.m3u8").exists()
        assert (tmp_path / "cmaf" / "manifest.mpd").exists()


# --- download_from_s3 ---


class TestDownloadFromS3:
    @patch("task.boto3.client")
    def test_calls_s3_download(self, mock_client):
        mock_s3 = MagicMock()
        mock_client.return_value = mock_s3

        download_from_s3("my-bucket", "audio/song.mp3", "/tmp/song.mp3")

        mock_client.assert_called_once_with("s3")
        mock_s3.download_file.assert_called_once_with(
            "my-bucket", "audio/song.mp3", "/tmp/song.mp3"
        )


# --- upload_directory_to_s3 ---


class TestUploadDirectoryToS3:
    @patch("task.boto3.client")
    def test_uploads_all_files(self, mock_client, tmp_path):
        mock_s3 = MagicMock()
        mock_client.return_value = mock_s3

        # Create some test files
        (tmp_path / "master.m3u8").write_text("#EXTM3U\n")
        sub = tmp_path / "low"
        sub.mkdir()
        (sub / "playlist.m3u8").write_text("#EXTM3U\n")
        (sub / "segment_000.ts").write_bytes(b"\x00")

        upload_directory_to_s3(str(tmp_path), "my-bucket", "audio/song/hls")

        assert mock_s3.upload_file.call_count == 3

    @patch("task.boto3.client")
    def test_sets_correct_content_types(self, mock_client, tmp_path):
        mock_s3 = MagicMock()
        mock_client.return_value = mock_s3

        (tmp_path / "main.m3u8").write_text("#EXTM3U\n")
        (tmp_path / "manifest.mpd").write_text("<MPD>")
        (tmp_path / "seg_000.m4s").write_bytes(b"\x00")

        upload_directory_to_s3(str(tmp_path), "my-bucket", "prefix")

        upload_calls = mock_s3.upload_file.call_args_list
        content_types = {c[1]["ExtraArgs"]["ContentType"] for c in upload_calls}
        assert "application/x-mpegURL" in content_types
        assert "application/dash+xml" in content_types
        assert "application/mp4" in content_types


# --- main ---


class TestMain:
    @patch("task.upload_directory_to_s3")
    @patch("task.process_universal_cmaf")
    @patch("task.download_from_s3")
    def test_main_flow(self, mock_download, mock_process, mock_upload, monkeypatch):
        monkeypatch.setenv("S3_BUCKET", "test-bucket")
        monkeypatch.setenv("S3_KEY", "audio/song.mp3")
        monkeypatch.setenv("CONTENT_ID", "cid123")

        main()

        mock_download.assert_called_once()
        assert mock_download.call_args[0][0] == "test-bucket"
        assert mock_download.call_args[0][1] == "audio/song.mp3"

        mock_process.assert_called_once()
        mock_upload.assert_called_once()
        assert mock_upload.call_args[0][1] == "test-bucket"
        assert mock_upload.call_args[0][2].endswith("/cmaf")

    @patch("task.upload_directory_to_s3")
    @patch("task.process_universal_cmaf")
    @patch("task.download_from_s3")
    def test_main_output_prefix_no_dir(
        self, mock_download, mock_process, mock_upload, monkeypatch
    ):
        monkeypatch.setenv("S3_BUCKET", "bucket")
        monkeypatch.setenv("S3_KEY", "song.mp3")
        monkeypatch.setenv("CONTENT_ID", "cid456")

        main()

        # Should be just '/cmaf' if no dir (matches code logic)
        assert mock_upload.call_args[0][2] == "/cmaf"
