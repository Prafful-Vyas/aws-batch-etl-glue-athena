"""Download NYC TLC monthly trip files and land them in the raw tier.

Examples:
    python scripts/load_trip_data.py --bucket my-bucket --service yellow --months 2024-01 2024-02
    python scripts/load_trip_data.py --bucket my-bucket --zones-only

Each file lands at raw/nyc-taxi/service=<svc>/year=YYYY/month=MM/<file>.parquet,
and the taxi-zone lookup CSV at reference/taxi_zones/taxi_zone_lookup.csv.
"""

import argparse
import re
import sys
import tempfile
import urllib.request
from pathlib import Path

import boto3

TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net"
ZONES_URL = f"{TLC_BASE_URL}/misc/taxi_zone_lookup.csv"
ZONES_KEY = "reference/taxi_zones/taxi_zone_lookup.csv"
SERVICES = ("yellow", "green")


def trip_file_name(service: str, year: int, month: int) -> str:
    return f"{service}_tripdata_{year}-{month:02d}.parquet"


def raw_key(service: str, year: int, month: int) -> str:
    return (
        f"raw/nyc-taxi/service={service}/year={year}/month={month:02d}/"
        f"{trip_file_name(service, year, month)}"
    )


def parse_month(value: str) -> tuple[int, int]:
    match = re.fullmatch(r"(\d{4})-(\d{2})", value)
    if not match or not 1 <= int(match.group(2)) <= 12:
        raise argparse.ArgumentTypeError(f"expected YYYY-MM, got {value!r}")
    return int(match.group(1)), int(match.group(2))


def download(url: str, dest: Path) -> None:
    print(f"  downloading {url}")
    # The TLC CDN rejects urllib's default user agent.
    request = urllib.request.Request(url, headers={"User-Agent": "nyc-taxi-batch-etl/1.0"})
    with urllib.request.urlopen(request) as response, open(dest, "wb") as out:
        while chunk := response.read(1 << 20):
            out.write(chunk)


def upload(s3, path: Path, bucket: str, key: str) -> None:
    print(f"  uploading to s3://{bucket}/{key}")
    # Bucket default encryption (SSE-KMS with the project CMK) applies automatically.
    s3.upload_file(str(path), bucket, key)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--bucket", required=True, help="Data lake bucket (terraform output bucket_name)")
    parser.add_argument("--service", choices=SERVICES, default="yellow")
    parser.add_argument("--months", nargs="*", type=parse_month, default=[], metavar="YYYY-MM")
    parser.add_argument("--zones-only", action="store_true", help="Only upload the taxi zone lookup")
    parser.add_argument("--region", default=None)
    args = parser.parse_args(argv)

    if not args.zones_only and not args.months:
        parser.error("pass --months YYYY-MM [YYYY-MM ...] or --zones-only")

    s3 = boto3.client("s3", region_name=args.region)
    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)

        print("Taxi zone lookup:")
        zones_path = tmp_dir / "taxi_zone_lookup.csv"
        download(ZONES_URL, zones_path)
        upload(s3, zones_path, args.bucket, ZONES_KEY)

        for year, month in args.months:
            name = trip_file_name(args.service, year, month)
            print(f"{name}:")
            path = tmp_dir / name
            download(f"{TLC_BASE_URL}/trip-data/{name}", path)
            upload(s3, path, args.bucket, raw_key(args.service, year, month))
            path.unlink()  # a month of yellow data is ~50 MB; don't accumulate

    print("Done. Next: python scripts/run_pipeline.py")
    return 0


if __name__ == "__main__":
    sys.exit(main())
