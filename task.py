import os
import subprocess
import logging
import requests
import xml.etree.ElementTree as ET
import base64
import binascii
import contextlib
import tempfile
import boto3
import uuid

# --- LOGGING ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("ABR_Packager")

SEGMENT_DURATION = 5

# ABR ladder
VARIANTS = [
    {"label": "high", "bitrate": "320k", "bandwidth": 320000},
    {"label": "med", "bitrate": "192k", "bandwidth": 192000},
    {"label": "low", "bitrate": "64k", "bandwidth": 64000},
]


@contextlib.contextmanager
def working_directory(path: str):
    original = os.getcwd()
    try:
        os.chdir(path)
        yield
    finally:
        os.chdir(original)


# ---------------- DRM ---------------- #


def fetch_ezdrm_keys(content_id: str, kid_guid: str, username: str, password: str):
    """Fetch Widevine keys from EZDRM CPIX API."""

    url = (
        f"https://cpix.ezdrm.com/keygenerator/cpix.aspx"
        f"?k={kid_guid}&u={username}&p={password}&c={content_id}"
    )

    logger.info("Fetching DRM keys from EZDRM")

    response = requests.get(url, timeout=15)
    response.raise_for_status()

    ns = {"cpix": "urn:dashif:org:cpix", "pskc": "urn:ietf:params:xml:ns:keyprov:pskc"}

    root = ET.fromstring(response.content)

    raw_kid = root.find(".//cpix:ContentKey", ns).get("kid")
    kid_hex = raw_kid.replace("-", "").lower()

    base64_key = root.find(".//pskc:PlainValue", ns).text
    key_hex = binascii.hexlify(base64.b64decode(base64_key)).decode()

    widevine_id = "edef8ba9-79d6-4ace-a3c8-27dcd51d21ed"

    base64_pssh = root.find(
        f".//cpix:DRMSystem[@systemId='{widevine_id}']/cpix:PSSH", ns
    ).text

    pssh_hex = binascii.hexlify(base64.b64decode(base64_pssh)).decode()

    logger.info("DRM keys retrieved successfully")

    return kid_hex, key_hex, pssh_hex


# ---------------- S3 Upload ---------------- #


def upload_directory_to_s3(local_dir: str, bucket: str, prefix: str):

    s3 = boto3.client("s3")

    content_types = {
        ".m3u8": "application/vnd.apple.mpegurl",
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

            logger.info("Uploading %s → s3://%s/%s", filename, bucket, s3_key)

            s3.upload_file(local_path, bucket, s3_key, ExtraArgs={"ContentType": ct})


# ---------------- Packaging ---------------- #


def process_abr_cmaf(wav_path: str, output_dir: str, drm_keys: tuple):

    kid_hex, key_hex, pssh_hex = drm_keys

    packager_inputs = []

    for v in VARIANTS:

        logger.info("Encoding %s variant (%s)", v["label"], v["bitrate"])

        variant_dir = os.path.join(output_dir, v["label"])
        os.makedirs(variant_dir, exist_ok=True)

        temp_file = os.path.join(output_dir, f"temp_{v['label']}.mp4")

        # --- Encode audio ---
        subprocess.run(
            [
                "ffmpeg",
                "-y",
                "-i",
                wav_path,
                "-vn",
                "-map",
                "0:a:0",
                "-c:a",
                "aac",
                "-b:a",
                v["bitrate"],
                "-minrate",
                v["bitrate"],
                "-maxrate",
                v["bitrate"],
                "-bufsize",
                str(int(v["bandwidth"]) * 2),
                "-ar",
                "44100",
                "-ac",
                "2",
                temp_file,
            ],
            check=True,
        )

        # --- Packager input spec ---
        packager_inputs.append(
            f"input={temp_file},stream=audio,"
            f"bandwidth={v['bandwidth']},"
            f"init_segment={v['label']}/init.mp4,"
            f"segment_template={v['label']}/seg_$Number$.m4s,"
            f"playlist_name={v['label']}/main.m3u8"
        )

    packager_cmd = (
        ["packager"]
        + packager_inputs
        + [
            "--enable_raw_key_encryption",
            "--keys",
            f"label=AUDIO:key_id={kid_hex}:key={key_hex}",
            "--pssh",
            pssh_hex,
            "--protection_scheme",
            "cbcs",
            "--segment_duration",
            str(SEGMENT_DURATION),
            "--clear_lead",
            "0",
            "--hls_master_playlist_output",
            "master.m3u8",
            "--mpd_output",
            "manifest.mpd",
        ]
    )

    logger.info("Running Shaka Packager")

    with working_directory(output_dir):
        subprocess.run(packager_cmd, check=True)

    logger.info("Packaging completed")


# ---------------- Main ---------------- #


def main():

    s3_bucket = os.environ["S3_BUCKET"]
    s3_key = os.environ["S3_KEY"]

    kid_guid = str(uuid.uuid4())
    ez_user = os.environ["EZDRM_USER"]
    ez_pass = os.environ["EZDRM_PASS"]

    key_parts = s3_key.split("/")
    track_id = key_parts[1]

    output_prefix = "/".join(key_parts[:-1]) + "/cmaf"

    logger.info("Processing track %s", track_id)

    drm_keys = fetch_ezdrm_keys(track_id, kid_guid, ez_user, ez_pass)

    with tempfile.TemporaryDirectory() as tmpdir:

        input_path = os.path.join(tmpdir, "input.wav")
        output_path = os.path.join(tmpdir, "output")

        os.makedirs(output_path)

        logger.info("Downloading source from S3")

        boto3.client("s3").download_file(s3_bucket, s3_key, input_path)

        process_abr_cmaf(input_path, output_path, drm_keys)

        upload_directory_to_s3(output_path, s3_bucket, output_prefix)

    logger.info("ABR packaging task completed successfully")


if __name__ == "__main__":
    main()
