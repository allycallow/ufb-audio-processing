import os
import subprocess
import tempfile
from unittest.mock import MagicMock, patch, call

import pytest

from convert import (
    convert_mp3_to_hls,
    download_from_s3,
    upload_directory_to_s3,
    main,
    HLS_VARIANTS,
)


# --- convert_mp3_to_hls ---


class TestConvertMp3ToHls:
    def test_creates_master_playlist(self, tmp_path):
        """Master playlist is created with correct variant references."""
        mp3_path = "/fake/input.mp3"
        output_dir = str(tmp_path / "hls")
        os.makedirs(output_dir)

        with patch("convert.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stderr="")
            convert_mp3_to_hls(mp3_path, output_dir)

        master = tmp_path / "hls" / "master.m3u8"
        assert master.exists()
        content = master.read_text()
        assert content.startswith("#EXTM3U\n")
        for label, _ in HLS_VARIANTS:
            assert f"{label}/playlist.m3u8" in content

    def test_master_playlist_bandwidths(self, tmp_path):
        """Each variant has the correct BANDWIDTH in the master playlist."""
        output_dir = str(tmp_path / "hls")
        os.makedirs(output_dir)

        with patch("convert.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            convert_mp3_to_hls("/fake/input.mp3", output_dir)

        content = (tmp_path / "hls" / "master.m3u8").read_text()
        for label, bitrate in HLS_VARIANTS:
            expected_bps = int(bitrate.rstrip("k")) * 1000
            assert f"BANDWIDTH={expected_bps}" in content

    def test_calls_ffmpeg_per_variant(self, tmp_path):
        """ffmpeg is called once per variant with correct bitrate."""
        output_dir = str(tmp_path / "hls")
        os.makedirs(output_dir)

        with patch("convert.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            convert_mp3_to_hls("/fake/input.mp3", output_dir)

        assert mock_run.call_count == len(HLS_VARIANTS)
        for i, (label, bitrate) in enumerate(HLS_VARIANTS):
            cmd = mock_run.call_args_list[i][0][0]
            assert cmd[0] == "ffmpeg"
            assert "-b:a" in cmd
            bitrate_idx = cmd.index("-b:a") + 1
            assert cmd[bitrate_idx] == bitrate

    def test_creates_variant_directories(self, tmp_path):
        """A subdirectory is created for each variant."""
        output_dir = str(tmp_path / "hls")
        os.makedirs(output_dir)

        with patch("convert.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            convert_mp3_to_hls("/fake/input.mp3", output_dir)

        for label, _ in HLS_VARIANTS:
            assert (tmp_path / "hls" / label).is_dir()

    def test_raises_on_ffmpeg_failure(self, tmp_path):
        """RuntimeError is raised when ffmpeg exits non-zero."""
        output_dir = str(tmp_path / "hls")
        os.makedirs(output_dir)

        with patch("convert.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stderr="encode error")
            with pytest.raises(RuntimeError, match="ffmpeg exited with code 1"):
                convert_mp3_to_hls("/fake/input.mp3", output_dir)


# --- download_from_s3 ---


class TestDownloadFromS3:
    @patch("convert.boto3.client")
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
    @patch("convert.boto3.client")
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

    @patch("convert.boto3.client")
    def test_sets_correct_content_types(self, mock_client, tmp_path):
        mock_s3 = MagicMock()
        mock_client.return_value = mock_s3

        (tmp_path / "playlist.m3u8").write_text("#EXTM3U\n")
        (tmp_path / "segment_000.ts").write_bytes(b"\x00")

        upload_directory_to_s3(str(tmp_path), "my-bucket", "prefix")

        upload_calls = mock_s3.upload_file.call_args_list
        content_types = {c[1]["ExtraArgs"]["ContentType"] for c in upload_calls}
        assert "application/vnd.apple.mpegurl" in content_types
        assert "audio/aac" in content_types


# --- main ---


class TestMain:
    @patch("convert.upload_directory_to_s3")
    @patch("convert.convert_mp3_to_hls")
    @patch("convert.download_from_s3")
    def test_main_flow(self, mock_download, mock_convert, mock_upload, monkeypatch):
        monkeypatch.setenv("S3_BUCKET", "test-bucket")
        monkeypatch.setenv("S3_KEY", "audio/song.mp3")

        main()

        mock_download.assert_called_once()
        assert mock_download.call_args[0][0] == "test-bucket"
        assert mock_download.call_args[0][1] == "audio/song.mp3"

        mock_convert.assert_called_once()
        mock_upload.assert_called_once()
        assert mock_upload.call_args[0][1] == "test-bucket"
        assert mock_upload.call_args[0][2] == "audio/hls"

    @patch("convert.upload_directory_to_s3")
    @patch("convert.convert_mp3_to_hls")
    @patch("convert.download_from_s3")
    def test_main_output_prefix_no_dir(
        self, mock_download, mock_convert, mock_upload, monkeypatch
    ):
        monkeypatch.setenv("S3_BUCKET", "bucket")
        monkeypatch.setenv("S3_KEY", "song.mp3")

        main()

        assert mock_upload.call_args[0][2] == "hls"
