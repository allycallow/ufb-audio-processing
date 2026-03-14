import os
import sys
import subprocess
import tempfile
import logging

import boto3

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# HLS settings
HLS_SEGMENT_DURATION = int(os.environ.get("HLS_SEGMENT_DURATION", "10"))

# Adaptive bitrate variants: (label, bitrate)
HLS_VARIANTS = [
    ("low", "96k"),
    ("mid", "160k"),
    ("high", "320k"),
]


def download_from_s3(bucket: str, key: str, dest: str) -> None:
    logger.info("Downloading s3://%s/%s", bucket, key)
    s3 = boto3.client("s3")
    s3.download_file(bucket, key, dest)
    logger.info("Downloaded to %s", dest)


def upload_directory_to_s3(local_dir: str, bucket: str, prefix: str) -> None:
    s3 = boto3.client("s3")
    for root, _dirs, files in os.walk(local_dir):
        for filename in files:
            local_path = os.path.join(root, filename)
            relative = os.path.relpath(local_path, local_dir)
            s3_key = f"{prefix}/{relative}"

            content_type = (
                "application/vnd.apple.mpegurl"
                if filename.endswith(".m3u8")
                else "audio/aac"
            )
            logger.info("Uploading %s -> s3://%s/%s", relative, bucket, s3_key)
            s3.upload_file(
                local_path,
                bucket,
                s3_key,
                ExtraArgs={"ContentType": content_type},
            )


def convert_mp3_to_hls(mp3_path: str, output_dir: str) -> None:
    """Convert an MP3 into multi-bitrate HLS with a master playlist."""
    for label, bitrate in HLS_VARIANTS:
        variant_dir = os.path.join(output_dir, label)
        os.makedirs(variant_dir, exist_ok=True)

        playlist_path = os.path.join(variant_dir, "playlist.m3u8")
        segment_pattern = os.path.join(variant_dir, "segment_%03d.ts")

        cmd = [
            "ffmpeg",
            "-i",
            mp3_path,
            "-vn",
            "-acodec",
            "aac",
            "-b:a",
            bitrate,
            "-hls_time",
            str(HLS_SEGMENT_DURATION),
            "-hls_segment_type",
            "mpegts",
            "-hls_playlist_type",
            "vod",
            "-hls_segment_filename",
            segment_pattern,
            playlist_path,
        ]

        logger.info("Encoding %s variant (%s): %s", label, bitrate, " ".join(cmd))
        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            logger.error("ffmpeg stderr:\n%s", result.stderr)
            raise RuntimeError(
                f"ffmpeg exited with code {result.returncode} for {label} variant"
            )

        logger.info("Variant %s complete: %s", label, playlist_path)

    # Write master playlist referencing each variant
    master_path = os.path.join(output_dir, "master.m3u8")
    with open(master_path, "w") as f:
        f.write("#EXTM3U\n")
        for label, bitrate in HLS_VARIANTS:
            # Convert e.g. "128k" -> 128000
            bps = int(bitrate.rstrip("k")) * 1000
            f.write(f'#EXT-X-STREAM-INF:BANDWIDTH={bps},CODECS="mp4a.40.2"\n')
            f.write(f"{label}/playlist.m3u8\n")

    logger.info("Master playlist written: %s", master_path)


def main() -> None:
    s3_bucket = os.environ["S3_BUCKET"]
    s3_key = os.environ["S3_KEY"]

    basename = os.path.splitext(os.path.basename(s3_key))[0]
    # Place HLS output as sibling to the MP3, e.g. audio/uuid/uuid.mp3 -> audio/uuid/hls/
    key_dir = os.path.dirname(s3_key)
    output_prefix = f"{key_dir}/hls" if key_dir else "hls"

    with tempfile.TemporaryDirectory() as tmpdir:
        mp3_path = os.path.join(tmpdir, "input.mp3")
        hls_output_dir = os.path.join(tmpdir, "hls")
        os.makedirs(hls_output_dir)

        download_from_s3(s3_bucket, s3_key, mp3_path)
        convert_mp3_to_hls(mp3_path, hls_output_dir)
        upload_directory_to_s3(hls_output_dir, s3_bucket, output_prefix)

    logger.info("Done. HLS output at s3://%s/%s/", s3_bucket, output_prefix)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("Fatal error")
        sys.exit(1)
