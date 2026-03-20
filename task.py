import contextlib
import logging
import os
import subprocess
import tempfile

import boto3

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("CMAF_Packager")

# --- EZDRM key material ---
KID_HEX = "a5814b403ac0457398e8aa98f178dc8c"
KEY_HEX = "67d3a4de738aa26e069b23268720d365"
PSSH_HEX = (
    "0000003f7073736800000000edef8ba979d64ace83c827dcd51d21ed"
    "0000001f1210a5814b403ac0457398e8aa98f178dc8c1a05657a64726d48e3dc959b06"
)

SEGMENT_DURATION = 5  # seconds — explicit, not relying on packager default


# --- Bug fix #1: context manager guarantees cwd restoration on any exit path ---
@contextlib.contextmanager
def working_directory(path: str):
    original = os.getcwd()
    try:
        os.chdir(path)
        yield
    finally:
        os.chdir(original)


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

            logger.info("UPLOAD: %s → s3://%s/%s [%s]", rel_path, bucket, s3_key, ct)
            s3.upload_file(
                local_path,
                bucket,
                s3_key,
                ExtraArgs={"ContentType": ct},
            )


def process_universal_cmaf(wav_path: str, output_dir: str) -> None:
    label = "lossless"
    variant_dir = os.path.join(output_dir, label)
    os.makedirs(variant_dir, exist_ok=True)

    intermediate = os.path.join(output_dir, "temp_flac.mp4")

    # 1. Transcode WAV → FLAC inside a CMAF-compatible MP4 container
    logger.info("FFMPEG: Encoding WAV → FLAC CMAF...")
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
    #
    # We chdir into variant_dir so the packager writes init.mp4 / seg_N.m4s
    # there with flat (no-subdirectory) paths.  main.m3u8 then references
    # segments as bare filenames, avoiding 404s when CloudFront serves
    # from the cmaf/lossless/ prefix.
    #
    # Output layout after packaging:
    #   output_dir/
    #     master.m3u8        ← HLS master playlist  (--hls_master_playlist_output)
    #     manifest.mpd       ← DASH manifest         (--mpd_output)
    #     lossless/
    #       main.m3u8        ← HLS variant playlist
    #       init.mp4         ← CMAF init segment
    #       seg_1.m4s ...    ← CMAF media segments

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
        f"label=AUDIO:key_id={KID_HEX}:key={KEY_HEX}",
        "--pssh",
        PSSH_HEX,
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

    # Bug fix #1: guaranteed cwd restoration even if packager raises
    with working_directory(variant_dir):
        logger.info("PACKAGER: Running from %s", variant_dir)
        subprocess.run(packager_cmd, check=True)

    os.remove(intermediate)
    logger.info("PACKAGER: Done. Removed intermediate %s", intermediate)


def main() -> None:
    # Bug fix #5: fail fast with a clear message if env vars are absent
    s3_bucket = os.environ.get("S3_BUCKET")
    s3_key = os.environ.get("S3_KEY")
    if not s3_bucket or not s3_key:
        raise EnvironmentError(
            "S3_BUCKET and S3_KEY environment variables must both be set. "
            f"Got: S3_BUCKET={s3_bucket!r}, S3_KEY={s3_key!r}"
        )

    # Bug fix #2: derive the output prefix from the key structure explicitly.
    # S3_KEY is expected to be   audio/{track_id}/original.wav
    # Output prefix becomes      audio/{track_id}/cmaf
    # This is explicit rather than relying on dirname not stripping too much.
    key_parts = s3_key.split("/")
    if len(key_parts) < 3:
        raise ValueError(
            f"S3_KEY must be at least 3 path segments deep (e.g. audio/{{id}}/file.wav). "
            f"Got: {s3_key!r}"
        )
    output_prefix = "/".join(key_parts[:-1]) + "/cmaf"
    logger.info("Output prefix: s3://%s/%s", s3_bucket, output_prefix)

    with tempfile.TemporaryDirectory() as tmpdir:
        input_path = os.path.join(tmpdir, "in.wav")
        output_path = os.path.join(tmpdir, "out")
        os.makedirs(output_path)

        download_from_s3(s3_bucket, s3_key, input_path)
        process_universal_cmaf(input_path, output_path)
        upload_directory_to_s3(output_path, s3_bucket, output_prefix)

    logger.info("TASK COMPLETED SUCCESSFULLY")


if __name__ == "__main__":
    main()
