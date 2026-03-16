# UFB Audio Processing

Converts MP3 files stored in S3 into Universal CMAF (HLS + DASH) adaptive-bitrate streams using FFmpeg and Shaka Packager, then uploads the output back to S3.

## How It Works

1. Downloads an MP3 from S3 (`S3_BUCKET` / `S3_KEY`)
2. Encodes three AAC variants via FFmpeg:
  - **low** — 128 kbps
  - **mid** — 192 kbps
  - **high** — 320 kbps
3. Packages and encrypts the variants into Universal CMAF (HLS + DASH) using Shaka Packager, with FairPlay and Widevine DRM.
4. Uploads the CMAF output directory back to S3 alongside the original file.

For example, `audio/uuid/track.mp3` produces output at `audio/uuid/cmaf/`.

## Environment Variables

| Variable               | Description                          | Default |
| ---------------------- | ------------------------------------ | ------- |
| `S3_BUCKET`            | S3 bucket name (required)            | —       |
| `S3_KEY`               | S3 object key of the MP3 (required)  | —       |
| `CONTENT_ID`           | Unique content identifier (required). Use the Django track ID or another unique string. | — |
| `DRM_KEY_SERVER_URL`   | DRM key server URL for encryption    | —       |

## Local Development

### Prerequisites

- Python 3.12
- [Pipenv](https://pipenv.pypa.io/)
- FFmpeg

### Setup

```sh
pipenv install --dev
```

### Running Tests

```sh
pipenv run pytest
```

## Docker

```sh
docker build -t ufb-audio-processing .
docker run --rm \
  -e S3_BUCKET=my-bucket \
  -e S3_KEY=audio/track.mp3 \
  -e CONTENT_ID=12345 \
  -e DRM_KEY_SERVER_URL=https://drm.example.com \
  -e AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY \
  ufb-audio-processing
```

## CI/CD

The project uses CircleCI to run tests and deploy a Docker image to AWS ECR on every push. See `.circleci/config.yml` for details.