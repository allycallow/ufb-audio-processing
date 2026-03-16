import os
import sys
import subprocess
import tempfile
import logging
import boto3

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

HLS_VARIANTS = [
    ("low", "128k"),
    ("mid", "192k"),
    ("high", "320k"),
]


def download_from_s3(bucket: str, key: str, dest: str) -> None:
    s3 = boto3.client("s3")
    s3.download_file(bucket, key, dest)


def upload_directory_to_s3(local_dir: str, bucket: str, prefix: str) -> None:
    s3 = boto3.client("s3")
    for root, _, files in os.walk(local_dir):
        for filename in files:
            local_path = os.path.join(root, filename)
            s3_key = f"{prefix}/{os.path.relpath(local_path, local_dir)}"
            # Shaka uses fMP4 (.m4s) which is generally 'application/mp4'
            content_type = (
                "application/x-mpegURL"
                if filename.endswith(".m3u8")
                else (
                    "application/dash+xml"
                    if filename.endswith(".mpd")
                    else "application/mp4"
                )
            )
            s3.upload_file(
                local_path, bucket, s3_key, ExtraArgs={"ContentType": content_type}
            )


def process_universal_cmaf(mp3_path: str, output_dir: str, content_id: str):
    """
    Step 1: Encode variants using FFmpeg.
    Step 2: Package & Encrypt using Shaka Packager (cbcs protection).
    """
    key_server_url = os.environ["DRM_KEY_SERVER_URL"]
    packager_inputs = []

    # --- STEP 1: ENCODE ---
    for label, bitrate in HLS_VARIANTS:
        intermediate_aac = os.path.join(output_dir, f"{label}_temp.m4a")

        logger.info(f"Encoding {label} variant...")
        subprocess.run(
            [
                "ffmpeg",
                "-i",
                mp3_path,
                "-vn",
                "-acodec",
                "aac",
                "-b:a",
                bitrate,
                "-y",
                intermediate_aac,
            ],
            check=True,
        )

        # --- STEP 2: BUILD SHAKA SPECS ---
        os.makedirs(os.path.join(output_dir, label), exist_ok=True)
        input_spec = (
            f"input={intermediate_aac},stream=audio,"
            f"init_segment={label}/init.mp4,"
            f"segment_template={label}/seg_$Number$.m4s,"
            f"playlist_name={label}/main.m3u8,"
            f"dash_label={label}"
        )
        packager_inputs.append(input_spec)

    # --- STEP 3: ENCRYPT & PACKAGE ---
    master_hls = os.path.join(output_dir, "master.m3u8")
    master_dash = os.path.join(output_dir, "manifest.mpd")

    packager_cmd = [
        "packager",
        *packager_inputs,
        "--enable_widevine_encryption",
        "--enable_fairplay_encryption",
        "--protection_scheme",
        "cbcs",  # Essential for Universal CMAF
        "--key_server_url",
        key_server_url,
        "--content_id",
        content_id,
        "--hls_master_playlist_output",
        master_hls,
        "--mpd_output",
        master_dash,
        "--clear_lead",
        "0",
    ]

    logger.info("Starting Universal Packaging...")
    subprocess.run(packager_cmd, check=True)

    # Cleanup temp AAC files
    for label, _ in HLS_VARIANTS:
        os.remove(os.path.join(output_dir, f"{label}_temp.m4a"))


def main():
    s3_bucket = os.environ["S3_BUCKET"]
    s3_key = os.environ["S3_KEY"]
    content_id = os.environ["CONTENT_ID"]  # From Django/Airflow

    output_prefix = f"{os.path.dirname(s3_key)}/cmaf"

    with tempfile.TemporaryDirectory() as tmpdir:
        input_path = os.path.join(tmpdir, "input.mp3")
        output_path = os.path.join(tmpdir, "out")
        os.makedirs(output_path)

        download_from_s3(s3_bucket, s3_key, input_path)
        process_universal_cmaf(input_path, output_path, content_id)
        upload_directory_to_s3(output_path, s3_bucket, output_prefix)

    logger.info(f"CMAF Uploaded to: s3://{s3_bucket}/{output_prefix}")


if __name__ == "__main__":
    main()
