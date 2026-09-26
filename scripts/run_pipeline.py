"""Run the Glue workflow (raw crawler -> ETL job) and wait for it to finish.

Resource names are read from `terraform output -json` in infra/, so run
`terraform apply` first. Pass --no-wait to return right after starting.

    python scripts/run_pipeline.py
"""

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

import boto3

INFRA_DIR = Path(__file__).resolve().parent.parent / "infra"
TERMINAL_STATES = {"COMPLETED", "STOPPED", "ERROR"}


def terraform_outputs() -> dict[str, str]:
    raw = subprocess.run(
        ["terraform", "output", "-json"], cwd=INFRA_DIR, check=True, capture_output=True, text=True
    ).stdout
    return {name: value["value"] for name, value in json.loads(raw).items()}


def summarize(run: dict) -> str:
    stats = run.get("Statistics", {})
    return (
        f"{run['Status']}: {stats.get('SucceededActions', 0)} succeeded, "
        f"{stats.get('FailedActions', 0)} failed, {stats.get('RunningActions', 0)} running"
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--workflow", help="Glue workflow name (default: terraform output)")
    parser.add_argument("--region", help="AWS region (default: terraform output)")
    parser.add_argument("--no-wait", action="store_true")
    parser.add_argument("--poll-seconds", type=int, default=30)
    args = parser.parse_args(argv)

    if not (args.workflow and args.region):
        outputs = terraform_outputs()
        args.workflow = args.workflow or outputs["glue_workflow_name"]
        args.region = args.region or outputs["aws_region"]

    glue = boto3.client("glue", region_name=args.region)
    run_id = glue.start_workflow_run(Name=args.workflow)["RunId"]
    print(f"Started workflow {args.workflow} run {run_id}")
    if args.no_wait:
        return 0

    while True:
        run = glue.get_workflow_run(Name=args.workflow, RunId=run_id)["Run"]
        print(f"  {summarize(run)}")
        if run["Status"] in TERMINAL_STATES:
            break
        time.sleep(args.poll_seconds)

    # A workflow run is COMPLETED even when an action inside it failed.
    failed = run["Status"] != "COMPLETED" or run.get("Statistics", {}).get("FailedActions", 0) > 0
    if failed:
        print("Pipeline failed; check the Glue console / CloudWatch logs (/aws-glue/jobs/error).")
        return 1
    print("Pipeline succeeded. Query nyc_taxi_processed.trips_processed in Athena.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
