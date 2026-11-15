import base64

import boto3
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from moto import mock_aws

import task


def test_encrypt_download_asset_roundtrips_and_matches_combined_layout(tmp_path):
    plaintext = b"not really an AAC file, just test bytes" * 1000
    plaintext_path = tmp_path / "download.m4a"
    ciphertext_path = tmp_path / "download.enc"
    plaintext_path.write_bytes(plaintext)

    key = task.encrypt_download_asset(str(plaintext_path), str(ciphertext_path))

    assert len(key) == 32  # AES-256

    combined = ciphertext_path.read_bytes()
    # nonce(12) || ciphertext(len(plaintext)) || tag(16) - the "combined"
    # layout expo-crypto's AESSealedData.fromCombined() expects on the client.
    assert len(combined) == 12 + len(plaintext) + 16

    nonce, sealed = combined[:12], combined[12:]
    decrypted = AESGCM(key).decrypt(nonce, sealed, None)
    assert decrypted == plaintext


@mock_aws
def test_wrap_content_key_round_trips_through_kms(monkeypatch):
    kms = boto3.client("kms", region_name="eu-west-2")
    key_id = kms.create_key()["KeyMetadata"]["KeyId"]
    monkeypatch.setenv("DOWNLOAD_CONTENT_KEYS_KMS_KEY_ID", key_id)

    raw_key = AESGCM.generate_key(bit_length=256)

    wrapped_b64 = task.wrap_content_key(raw_key)

    ciphertext_blob = base64.b64decode(wrapped_b64)
    decrypted = kms.decrypt(CiphertextBlob=ciphertext_blob)["Plaintext"]
    assert decrypted == raw_key


def test_output_dir_is_unchanged_for_a_manual_upload():
    track_id = "0100c3ca-b789-4da1-b460-f3e9d3458162"
    s3_key = f"production-ufb-media/audio/{track_id}/{track_id}.wav"

    assert task.output_dir_for(track_id) == "/".join(s3_key.split("/")[:-1])


def test_output_dir_is_per_track_under_audio_for_a_ddex_delivery():
    # Every track in a release shares one DDEX resources/ folder, but its
    # output has to land where the CDN and backend look for it -
    # audio/<track_id>/ - not alongside the source.
    assert task.output_dir_for("track-id-1") == "production-ufb-media/audio/track-id-1"
    assert task.output_dir_for("track-id-2") == "production-ufb-media/audio/track-id-2"
