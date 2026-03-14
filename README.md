# UFB Audio Processing

Converts MP3 files stored in S3 into adaptive-bitrate HLS streams using FFmpeg, then uploads the output back to S3.

## How It Works

1. Downloads an MP3 from S3 (`S3_BUCKET` / `S3_KEY`)
2. Encodes three AAC HLS variants via FFmpeg:
   - **low** — 96 kbps
   - **mid** — 160 kbps
   - **high** — 320 kbps
3. Generates a master playlist (`master.m3u8`) referencing each variant
4. Uploads the HLS directory back to S3 alongside the original file

For example, `audio/uuid/track.mp3` produces output at `audio/uuid/hls/`.

## Environment Variables

| Variable               | Description                          | Default |
| ---------------------- | ------------------------------------ | ------- |
| `S3_BUCKET`            | S3 bucket name (required)            | —       |
| `S3_KEY`               | S3 object key of the MP3 (required)  | —       |
| `HLS_SEGMENT_DURATION` | Segment length in seconds            | `10`    |

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
  -e AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY \
  ufb-audio-processing
```

## CI/CD

The project uses CircleCI to run tests and deploy a Docker image to AWS ECR on every push. See `.circleci/config.yml` for details.