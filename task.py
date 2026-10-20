import base64
import binascii
import contextlib
import json
import logging
import os
import subprocess
import tempfile
import uuid
import xml.etree.ElementTree as ET

import boto3
import requests
import sentry_sdk
from confluent_kafka import Producer
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

# --- LOGGING ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("ABR_Packager")

# --- SENTRY ---
sentry_dsn = os.environ.get("SENTRY_DSN")
if sentry_dsn:
    sentry_sdk.init(
        dsn=sentry_dsn, environment=os.environ.get("SENTRY_ENVIRONMENT", "production")
    )

SEGMENT_DURATION = 5

# --- EVENTBRIDGE ---
EVENT_BUS_NAME = os.environ.get("EVENTBRIDGE_BUS_NAME", "production-ufb")
EVENT_SOURCE = "ufb.audio-processing"

# --- KAFKA ---
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TRACK_PROCESSING_EVENTS_TOPIC = os.environ.get(
    "KAFKA_TRACK_PROCESSING_EVENTS_TOPIC", "ufb.track_processing_events"
)

# ABR ladder
VARIANTS = [
    {"label": "high", "bitrate": "320k", "bandwidth": 320000},
    {"label": "med", "bitrate": "192k", "bandwidth": 192000},
    {"label": "low", "bitrate": "64k", "bandwidth": 64000},
]

# Matches VARIANTS[0] ("high") - downloads get the same quality subscribers
# already stream, just as a single flat file instead of chunked HLS.
DOWNLOAD_BITRATE = VARIANTS[0]["bitrate"]


@contextlib.contextmanager
def working_directory(path: str):
    original = os.getcwd()
    try:
        os.chdir(path)
        yield
    finally:
        os.chdir(original)


# ---------------- EventBridge ---------------- #


def emit_progress_event(track_id: str, status: str, **detail):
    """Publish a progress event for this track to EventBridge.

    Best-effort: a failure to publish is logged but never fails the task,
    since progress tracking shouldn't block audio processing.
    """

    payload = {"track_id": track_id, "status": status, **detail}
    detail_type = f"AudioTrack.{status.capitalize()}"

    logger.info("Publishing progress event to EventBridge: %s", payload)

    try:
        response = boto3.client("events").put_events(
            Entries=[
                {
                    "Source": EVENT_SOURCE,
                    "DetailType": detail_type,
                    "Detail": json.dumps(payload),
                    "EventBusName": EVENT_BUS_NAME,
                }
            ]
        )
        if response.get("FailedEntryCount"):
            logger.warning(
                "EventBridge rejected progress event (status=%s): %s",
                status,
                response["Entries"],
            )
    except Exception:
        logger.exception(
            "Failed to publish progress event to EventBridge (status=%s)", status
        )


# ---------------- Kafka ---------------- #

_kafka_producer = None


def _get_kafka_producer() -> Producer:
    global _kafka_producer
    if _kafka_producer is None:
        _kafka_producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    return _kafka_producer


def emit_saga_result(
    track_id: str, status: str, data: dict | None = None, error: str | None = None
):
    """Publish this track's final audio-encoding result to Kafka for
    TrackIngestionSagaWorkflow to react to (see ufb-temporal-worker's
    reply_listener.py, which signals the saga and is what actually writes
    the result to Django). Best-effort: a failure to publish is logged but
    never fails the task — the saga's own result-timeout is the backstop.
    """
    payload = {"track_id": track_id, "step": "audio", "status": status}
    if data is not None:
        payload["data"] = data
    if error is not None:
        payload["error"] = error

    logger.info("Publishing saga result to Kafka: %s", payload)

    try:
        producer = _get_kafka_producer()
        producer.produce(
            topic=KAFKA_TRACK_PROCESSING_EVENTS_TOPIC,
            key=track_id.encode("utf-8"),
            value=json.dumps(payload).encode("utf-8"),
        )
        producer.flush(timeout=10)
    except Exception:
        logger.exception("Failed to publish saga result to Kafka (status=%s)", status)


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


def _validate_clearkey_hex(name: str, value: str) -> str:
    value = value.strip().lower()
    try:
        raw = bytes.fromhex(value)
    except ValueError:
        raise ValueError(f"{name} must be a hex string, got: {value!r}") from None
    if len(raw) != 16:
        raise ValueError(
            f"{name} must be 16 bytes (32 hex chars) for AES-128, got {len(raw)} bytes"
        )
    return value


def generate_clearkey_keys(content_id: str):
    """Return a key_id/key pair for local ClearKey testing.

    If CLEARKEY_KEY_ID and CLEARKEY_KEY are both set, those fixed values
    are used — handy for pinning one key across every dev run so the
    frontend's ClearKey config doesn't need updating per track. Otherwise
    a random pair is generated for this run.

    No PSSH is returned — Shaka Packager falls back to emitting a
    default "common system" PSSH box containing the key IDs, which is
    enough for a ClearKey player to raise an `encrypted` event and
    request the key locally, without ever calling EZDRM.
    """

    override_kid = os.environ.get("CLEARKEY_KEY_ID")
    override_key = os.environ.get("CLEARKEY_KEY")

    if override_kid and override_key:
        kid_hex = _validate_clearkey_hex("CLEARKEY_KEY_ID", override_kid)
        key_hex = _validate_clearkey_hex("CLEARKEY_KEY", override_key)

        logger.info(
            "Using fixed ClearKey pair from CLEARKEY_KEY_ID/CLEARKEY_KEY for "
            "content_id=%s — key_id=%s key=%s",
            content_id,
            kid_hex,
            key_hex,
        )
    else:
        kid_hex = uuid.uuid4().hex
        key_hex = uuid.uuid4().hex

        logger.info(
            "Generated local ClearKey pair for content_id=%s — key_id=%s key=%s "
            "(add this to your player's ClearKey config, or set "
            "CLEARKEY_KEY_ID/CLEARKEY_KEY to pin a fixed key across runs)",
            content_id,
            kid_hex,
            key_hex,
        )

    return kid_hex, key_hex, None


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


def tag_wav_for_archival(bucket: str, key: str):
    """Tag the source WAV archive-tier=source so the media bucket's lifecycle
    rule (modules/storage in ufb-iac) can transition it to Glacier Deep
    Archive. Only called once the CMAF/DRM output has been uploaded
    successfully, so a WAV never goes cold before we're done needing it.

    Best-effort: a failure here shouldn't fail the track, it just means this
    one WAV stays in Standard until a manual retag.
    """

    try:
        boto3.client("s3").put_object_tagging(
            Bucket=bucket,
            Key=key,
            Tagging={"TagSet": [{"Key": "archive-tier", "Value": "source"}]},
        )
    except Exception:
        logger.exception("Failed to tag source WAV for archival: s3://%s/%s", bucket, key)


# ---------------- Preview ---------------- #


def generate_preview(wav_path: str, preview_path: str, track_id: str):
    """Encode a 30s unencrypted MP3 preview centered on the track's midpoint."""

    probe = subprocess.run(
        [
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            wav_path,
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    duration = float(probe.stdout.strip())
    start = max(0.0, duration / 2 - 15)

    logger.info("Generating 30s preview (start=%.1fs)", start)
    emit_progress_event(track_id, "preview")

    subprocess.run(
        [
            "ffmpeg",
            "-y",
            "-ss",
            str(start),
            "-i",
            wav_path,
            "-t",
            "30",
            "-vn",
            "-map",
            "0:a:0",
            "-c:a",
            "libmp3lame",
            "-b:a",
            "96k",
            "-ar",
            "44100",
            "-ac",
            "2",
            preview_path,
        ],
        check=True,
    )


# ---------------- Download (offline, app-encrypted) ---------------- #


def generate_download(wav_path: str, output_path: str, track_id: str):
    """Encode a full-length AAC download asset.

    Structurally identical to one iteration of process_abr_cmaf's per-variant
    encode, but un-trimmed (unlike generate_preview) and never touches Shaka
    Packager - this is played back as a single whole file on-device, not
    streamed in segments, and it's protected by app-level AES-GCM encryption
    (see encrypt_download_asset) rather than Shaka's DRM packaging.
    """

    logger.info("Encoding download asset (%s)", DOWNLOAD_BITRATE)
    emit_progress_event(track_id, "download_encoding")

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
            DOWNLOAD_BITRATE,
            "-ar",
            "44100",
            "-ac",
            "2",
            output_path,
        ],
        check=True,
    )


def encrypt_download_asset(plaintext_path: str, ciphertext_path: str) -> bytes:
    """AES-256-GCM encrypt a download asset in place.

    Writes nonce(12) || ciphertext || tag(16) to ciphertext_path -
    AESGCM.encrypt() already appends the tag, so prepending the nonce
    ourselves produces exactly the "combined" layout expo-crypto's
    aesDecryptAsync expects on the client. Returns the raw key; callers must
    wrap it with wrap_content_key() and discard it immediately - it is never
    logged or persisted unwrapped.
    """

    key = AESGCM.generate_key(bit_length=256)
    nonce = os.urandom(12)

    with open(plaintext_path, "rb") as f:
        plaintext = f.read()

    ciphertext = AESGCM(key).encrypt(nonce, plaintext, None)

    with open(ciphertext_path, "wb") as f:
        f.write(nonce + ciphertext)

    return key


def wrap_content_key(key: bytes) -> str:
    """KMS-wrap a raw content key for storage.

    A key-wrapping step only, not full envelope encryption - only the small
    raw key, never the audio file itself, touches KMS.
    """

    response = boto3.client("kms").encrypt(
        KeyId=os.environ["DOWNLOAD_CONTENT_KEYS_KMS_KEY_ID"], Plaintext=key
    )
    return base64.b64encode(response["CiphertextBlob"]).decode("ascii")


# ---------------- Packaging ---------------- #


def process_abr_cmaf(wav_path: str, output_dir: str, drm_keys: tuple, track_id: str):

    kid_hex, key_hex, pssh_hex = drm_keys

    packager_inputs = []

    for v in VARIANTS:

        logger.info("Encoding %s variant (%s)", v["label"], v["bitrate"])
        emit_progress_event(track_id, "encoding", variant=v["label"])

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
        ]
        + (["--pssh", pssh_hex] if pssh_hex else [])
        + [
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
    emit_progress_event(track_id, "packaging")

    with working_directory(output_dir):
        subprocess.run(packager_cmd, check=True)

    logger.info("Packaging completed")


# ---------------- Main ---------------- #


def main():

    s3_bucket = os.environ["S3_BUCKET"]
    s3_key = os.environ["S3_KEY"]

    key_parts = s3_key.split("/")
    # TRACK_ID is passed explicitly by every known caller now (see
    # ufb-temporal-worker's activities/track_ingestion.py); key_parts[2]
    # remains only as a fallback for anything invoking this task without it.
    track_id = os.environ.get("TRACK_ID") or key_parts[2]

    # Output (CMAF/preview/download) always goes to this bucket - explicit,
    # rather than defaulting to S3_BUCKET, since for a DDEX-triggered run
    # S3_BUCKET is the DDEX delivery bucket (KMS/VPC-restricted, not
    # CloudFront's origin), not the media bucket output actually belongs in.
    # Falls back to S3_BUCKET so a caller that never sets this (none do
    # today) still gets the pre-existing same-bucket behavior.
    output_bucket = os.environ.get("OUTPUT_BUCKET", s3_bucket)
    output_prefix = "/".join(key_parts[:-1]) + "/cmaf"

    logger.info("Processing track %s", track_id)
    emit_progress_event(track_id, "started", s3_bucket=s3_bucket, s3_key=s3_key)

    try:
        drm_env = os.environ.get("DRM_ENV", "production").lower()

        if drm_env == "development":
            logger.info(
                "DRM_ENV=development — using local ClearKey keys, skipping EZDRM"
            )
            drm_keys = generate_clearkey_keys(track_id)
        else:
            kid_guid = str(uuid.uuid4())
            ez_user = os.environ["EZDRM_USER"]
            ez_pass = os.environ["EZDRM_PASS"]
            drm_keys = fetch_ezdrm_keys(track_id, kid_guid, ez_user, ez_pass)

        with tempfile.TemporaryDirectory() as tmpdir:

            input_path = os.path.join(tmpdir, "input.wav")
            output_path = os.path.join(tmpdir, "output")
            preview_path = os.path.join(tmpdir, "preview.mp3")
            download_plain_path = os.path.join(tmpdir, "download.m4a")
            download_cipher_path = os.path.join(tmpdir, "download.enc")

            os.makedirs(output_path)

            logger.info("Downloading source from S3")
            emit_progress_event(track_id, "downloading")

            boto3.client("s3").download_file(s3_bucket, s3_key, input_path)

            process_abr_cmaf(input_path, output_path, drm_keys, track_id)
            generate_preview(input_path, preview_path, track_id)

            generate_download(input_path, download_plain_path, track_id)
            raw_download_key = encrypt_download_asset(
                download_plain_path, download_cipher_path
            )
            download_encryption_key_ciphertext = wrap_content_key(raw_download_key)
            del raw_download_key

            logger.info("Uploading output to S3")
            emit_progress_event(track_id, "uploading")

            upload_directory_to_s3(output_path, output_bucket, output_prefix)

            preview_key = "/".join(key_parts[:-1]) + "/preview.mp3"
            logger.info("Uploading preview to s3://%s/%s", output_bucket, preview_key)
            boto3.client("s3").upload_file(
                preview_path,
                output_bucket,
                preview_key,
                ExtraArgs={"ContentType": "audio/mpeg"},
            )

            download_output_key = "/".join(key_parts[:-1]) + "/download.enc"
            logger.info(
                "Uploading download asset to s3://%s/%s",
                output_bucket,
                download_output_key,
            )
            boto3.client("s3").upload_file(
                download_cipher_path,
                output_bucket,
                download_output_key,
                ExtraArgs={"ContentType": "application/octet-stream"},
            )

            # The media bucket's own lifecycle rule this tags for doesn't
            # apply to a source bucket other than the media bucket itself
            # (e.g. the DDEX delivery bucket, which has its own, separate
            # archival rule - see modules/ddex-delivery in ufb-iac) - and
            # audio-processing's task role has no PutObjectTagging grant
            # there anyway, only GetObject.
            if s3_bucket == output_bucket:
                tag_wav_for_archival(s3_bucket, s3_key)
    except Exception as exc:
        sentry_sdk.capture_exception(exc)
        emit_saga_result(track_id, "failed", error=str(exc))
        raise

    logger.info("ABR packaging task completed successfully")
    emit_saga_result(
        track_id,
        "success",
        data={
            "output_prefix": output_prefix,
            "drm_key_id": drm_keys[0],
            "preview_key": preview_key,
            "download_output_key": download_output_key,
            "download_encryption_key_ciphertext": download_encryption_key_ciphertext,
        },
    )


if __name__ == "__main__":
    main()
