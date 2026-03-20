import os
import sys
import subprocess
import tempfile
import logging
import contextlib
import xml.etree.ElementTree as ET
import base64
import binascii
import requests
import boto3
import uuid

# --- LOGGING CONFIGURATION ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("CMAF_Packager")

SEGMENT_DURATION = 5


# --- Bug fix #1: context manager guarantees cwd restoration ---
@contextlib.contextmanager
def working_directory(path: str):
    original = os.getcwd()
    try:
        os.chdir(path)
        yield
    finally:
        os.chdir(original)


def fetch_ezdrm_keys(content_id, kid_guid, username, password):
    """
    Dynamically fetches KID_HEX, KEY_HEX, and PSSH_HEX from EZDRM CPIX API.
    """
    url = f"https://cpix.ezdrm.com/keygenerator/cpix.aspx?k={kid_guid}&u={username}&p={password}&c={content_id}"
    logger.info("EZDRM: Fetching keys from CPIX API...")

    response = requests.get(url)
    response.raise_for_status()

    # Namespaces for CPIX XML parsing
    ns = {"cpix": "urn:dashif:org:cpix", "pskc": "urn:ietf:params:xml:ns:keyprov:pskc"}

    root = ET.fromstring(response.content)

    # 1. Extract KID_HEX (Strip dashes from GUID)
    raw_kid = root.find(".//cpix:ContentKey", ns).get("kid")
    kid_hex = raw_kid.replace("-", "").lower()

    # 2. Extract KEY_HEX (Decode Base64 PlainValue)
    base64_key = root.find(".//pskc:PlainValue", ns).text
    key_hex = binascii.hexlify(base64.b64decode(base64_key)).decode("utf-8")

    # 3. Extract PSSH_HEX (Widevine System ID: edef8ba9-79d6-4ace-a3c8-27dcd51d21ed)
    widevine_id = "edef8ba9-79d6-4ace-a3c8-27dcd51d21ed"
    base64_pssh = root.find(
        f".//cpix:DRMSystem[@systemId='{widevine_id}']/cpix:PSSH", ns
    ).text
    pssh_hex = binascii.hexlify(base64.b64decode(base64_pssh)).decode("utf-8")

    logger.info("EZDRM: Keys successfully rotated/fetched.")
    return kid_hex, key_hex, pssh_hex


def download_from_s3(bucket: str, key: str, dest: str) -> None:
    s3 = boto3.client("s3")
    logger.info("ACTION: Downloading s3://%s/%s", bucket, key)
    s3.download_file(bucket, key, dest)


def upload_directory_to_s3(local_dir: str, bucket: str, prefix: str) -> None:
    s3 = boto3.client("s3")
    content_types = {
        ".m3u8": "application/x-mpegURL",
        ".mpd": "application/dash+xml",
        ".m4s": "video/iso.segment",
        ".mp4": "video/mp4",
    }
    for root, _, files in os.walk(local_dir):
        for filename in files:
            local_path = os.path.join(root, filename)
            rel_path = os.path.relpath(local_path, local_dir)
            s3_key = f"{prefix}/{rel_path}"
            ext = os.path.splitext(filename)[1].lower()
            ct = content_types.get(ext, "application/octet-stream")

            s3.upload_file(local_path, bucket, s3_key, ExtraArgs={"ContentType": ct})


def process_universal_cmaf(wav_path: str, output_dir: str, drm_keys: tuple) -> None:
    kid_hex, key_hex, pssh_hex = drm_keys
    label = "lossless"
    variant_dir = os.path.join(output_dir, label)
    os.makedirs(variant_dir, exist_ok=True)

    intermediate = os.path.join(output_dir, "temp_flac.mp4")

    # 1. Transcode WAV → FLAC
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
            intermediate,
        ],
        check=True,
    )

    # 2. Package with shaka-packager
    input_spec = (
        f"input={intermediate},stream=audio,"
        f"init_segment=init.mp4,"
        f"segment_template=seg_$Number$.m4s,"
        f"playlist_name=main.m3u8,"
        f"hls_group_id=audio,hls_name={label}"
    )

    packager_cmd = [
        "packager",
        input_spec,
        "--enable_raw_key_encryption",
        "--keys",
        f"label=AUDIO:key_id={kid_hex}:key={key_hex}",
        "--pssh",
        pssh_hex,
        "--protection_scheme",
        "cbcs",
        "--protection_systems",
        "Widevine",
        "--segment_duration",
        str(SEGMENT_DURATION),
        "--clear_lead",
        "0",
        "--hls_master_playlist_output",
        "../master.m3u8",
        "--mpd_output",
        "../manifest.mpd",
    ]

    with working_directory(variant_dir):
        subprocess.run(packager_cmd, check=True)

    os.remove(intermediate)


def main() -> None:
    # Environment Variables
    s3_bucket = os.environ.get("S3_BUCKET")
    s3_key = os.environ.get("S3_KEY")
    ez_user = os.environ.get("EZDRM_USER")
    ez_pass = os.environ.get("EZDRM_PASS")
    content_id = os.environ.get("CONTENT_ID")
    kid_guid = str(uuid.uuid4())

    logger.info(
        f"TASK STARTED: Processing {s3_key} with KID {kid_guid} and Content ID {content_id}"
    )

    # Derive IDs from S3 Key: audio/{id}/original.wav
    key_parts = s3_key.split("/")
    output_prefix = "/".join(key_parts[:-1]) + "/cmaf"

    # Fetch dynamic keys (Using your provided GUID for the KID)
    # Note: In production, track_id and kid_guid might be the same or linked
    drm_keys = fetch_ezdrm_keys(
        content_id=content_id,
        kid_guid=kid_guid,
        username=ez_user,
        password=ez_pass,
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        input_path = os.path.join(tmpdir, "in.wav")
        output_path = os.path.join(tmpdir, "out")
        os.makedirs(output_path)

        download_from_s3(s3_bucket, s3_key, input_path)
        process_universal_cmaf(input_path, output_path, drm_keys)
        upload_directory_to_s3(output_path, s3_bucket, output_prefix)

    logger.info("TASK COMPLETED SUCCESSFULLY")


if __name__ == "__main__":
    main()
