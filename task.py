import os
import sys
import subprocess
import tempfile
import logging
import boto3
import base64

# Setup logging for AWS ECS
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def download_from_s3(bucket: str, key: str, dest: str) -> None:
    """Downloads the source WAV file from S3."""
    s3 = boto3.client("s3")
    logger.info(f"Downloading s3://{bucket}/{key} to {dest}")
    s3.download_file(bucket, key, dest)


def upload_directory_to_s3(local_dir: str, bucket: str, prefix: str) -> None:
    """Uploads the encrypted CMAF segments and manifests back to S3."""
    s3 = boto3.client("s3")
    for root, _, files in os.walk(local_dir):
        for filename in files:
            local_path = os.path.join(root, filename)
            s3_key = f"{prefix}/{os.path.relpath(local_path, local_dir)}"

            if filename.endswith(".m3u8"):
                content_type = "application/x-mpegURL"
            elif filename.endswith(".mpd"):
                content_type = "application/dash+xml"
            else:
                content_type = "application/mp4"

            s3.upload_file(
                local_path, bucket, s3_key, ExtraArgs={"ContentType": content_type}
            )
    logger.info(f"Upload complete to s3://{bucket}/{prefix}")


def process_universal_cmaf(wav_path: str, output_dir: str, content_id: str):
    """
    Step 1: Transcode WAV to Lossless FLAC in an MP4 container.
    Step 2: Package & Encrypt using Shaka Packager (Raw Keys).
    """

    # --- DATA FROM YOUR LATEST EZDRM XML RESPONSE ---
    ezdrm_kid_guid = "a5814b40-3ac0-4573-98e8-aa98f178dc8c"
    ezdrm_key_b64 = "Z9Ok3nOKom4GmyMmhyDTZQ=="
    ezdrm_pssh_b64 = "AAAAP3Bzc2gAAAAA7e+LqXnWSs6jyCfc1R0h7QAAAB8SEKWBS0A6wEVzmOiqmPF43IwaBWV6ZHJtSOPclZsG"

    # --- FORMAT CONVERSIONS ---
    key_id_hex = ezdrm_kid_guid.replace("-", "")
    key_hex = base64.b64decode(ezdrm_key_b64).hex()
    pssh_hex = base64.b64decode(ezdrm_pssh_b64).hex()

    label = "lossless"
    intermediate_container = os.path.join(output_dir, f"{label}_temp.mp4")

    # --- STEP 1: ENCODE WAV TO LOSSLESS FLAC ---
    logger.info(f"Transcoding WAV to {label} variant...")
    # Using 16-bit/44.1kHz to match your source audio logs exactly
    subprocess.run(
        [
            "ffmpeg",
            "-i",
            wav_path,
            "-vn",
            "-c:a",
            "flac",
            "-sample_fmt",
            "s16",
            "-ar",
            "44100",
            "-y",
            intermediate_container,
        ],
        check=True,
    )

    # --- STEP 2: DEFINE SHAKA INPUT ---
    os.makedirs(os.path.join(output_dir, label), exist_ok=True)
    # Note: dash_label remains 'lossless' for the manifest name,
    # but the DRM system will now look for the 'AUDIO' key.
    input_spec = (
        f"input={intermediate_container},stream=audio,"
        f"init_segment={label}/init.mp4,"
        f"segment_template={label}/seg_$Number$.m4s,"
        f"playlist_name={label}/main.m3u8,"
        f"dash_label={label}"
    )

    # --- STEP 3: ENCRYPT & PACKAGE ---
    master_hls = os.path.join(output_dir, "master.m3u8")
    master_dash = os.path.join(output_dir, "manifest.mpd")

    # FIXED: Changed label=lossless to label=AUDIO to match Shaka's internal stream detection
    packager_cmd = [
        "packager",
        input_spec,
        "--enable_raw_key_encryption",
        "--keys",
        f"label=AUDIO:key_id={key_id_hex}:key={key_hex}",
        "--pssh",
        pssh_hex,
        "--protection_scheme",
        "cbcs",
        "--protection_systems",
        "Widevine",
        "--clear_lead",
        "0",
        "--hls_master_playlist_output",
        master_hls,
        "--mpd_output",
        master_dash,
    ]

    logger.info(f"Starting Shaka Packaging (KID: {key_id_hex})")
    subprocess.run(packager_cmd, check=True)

    # Cleanup intermediate container
    os.remove(intermediate_container)


def main():
    s3_bucket = os.environ.get("S3_BUCKET")
    s3_key = os.environ.get("S3_KEY")
    content_id = os.environ.get("CONTENT_ID", "default_id")

    if not s3_bucket or not s3_key:
        logger.error("S3_BUCKET or S3_KEY environment variables are missing.")
        sys.exit(1)

    output_prefix = f"{os.path.dirname(s3_key)}/cmaf"

    with tempfile.TemporaryDirectory() as tmpdir:
        input_path = os.path.join(tmpdir, "input.wav")
        output_path = os.path.join(tmpdir, "out")
        os.makedirs(output_path)

        download_from_s3(s3_bucket, s3_key, input_path)
        process_universal_cmaf(input_path, output_path, content_id)
        upload_directory_to_s3(output_path, s3_bucket, output_prefix)

    logger.info("Task completed successfully.")


if __name__ == "__main__":
    main()
