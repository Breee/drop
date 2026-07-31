#!/usr/bin/env python3
"""Fetch benchmark input data from a real cluster's Prometheus and Loki.

You only supply a Prometheus endpoint and a Loki endpoint (plus optional query
overrides). This tool produces the CSVs that ``evaluate_replay.py`` and
``evaluate_discovery_strategies.py`` consume:

  - images.csv                      per-image p50 pull time + size (from Loki)
  - gitlab_runner_jobs.csv          per-pod jobs (from kube-state-metrics)
  - prometheus_image_samples_5m.csv running-container usage over time
  - kubernetes_events.csv           raw image-pull events (from Loki)

Assumptions
-----------
* Kubernetes events are shipped to Loki by Grafana Alloy
  (``loki.source.kubernetes_events``), so each log line is a JSON object with
  ``reason``, ``name`` and ``msg`` fields. Pull durations and image sizes are
  parsed from kubelet "Successfully pulled image ..." messages.
* Per-pod placement/lifecycle comes from kube-state-metrics
  (``kube_pod_info``, ``kube_pod_container_info``, ``kube_pod_created``,
  ``kube_pod_start_time``, ``kube_pod_completion_time``). ``last_over_time`` is
  used so short-lived Job pods that existed anywhere in the window are captured.

All queries have sensible defaults for GitLab Kubernetes-executor pods
(``pod=~"runner-.*"``) and can be overridden on the command line.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

# --- Parsing helpers (mirrors internal/discovery/loki.go) --------------------

_RE_IMAGE_REF = re.compile(r'(?:image|Image)\s+"([^"]+)"')
_RE_PULL_DURATION = re.compile(r"\bin\s+(\d+(?:\.\d+)?)(ms|s|m|h)\b")
_RE_IMAGE_SIZE_BYTES = re.compile(r"(?i)\bimage\s+size:\s*(\d+)\s+bytes\b")
_RE_LOGFMT_MESSAGE = re.compile(r'\b(?:message|msg)=("(?:[^"\\]|\\.)*"|\S+)')
_DURATION_UNIT_SECONDS = {"ms": 1e-3, "s": 1.0, "m": 60.0, "h": 3600.0}


def parse_image_ref(msg: str) -> str:
    m = _RE_IMAGE_REF.search(msg or "")
    return m.group(1) if m else ""


def parse_pull_duration_seconds(msg: str) -> float:
    m = _RE_PULL_DURATION.search(msg or "")
    if not m:
        return 0.0
    return float(m.group(1)) * _DURATION_UNIT_SECONDS[m.group(2)]


def parse_image_size_bytes(msg: str) -> int:
    m = _RE_IMAGE_SIZE_BYTES.search(msg or "")
    return int(m.group(1)) if m else 0


def infer_reason(msg: str) -> str:
    low = (msg or "").lower()
    if "successfully pulled" in low:
        return "Pulled"
    if "pulling image" in low:
        return "Pulling"
    if "already present" in low:
        return "AlreadyPresent"
    if "failed" in low:
        return "Failed"
    return ""


def parse_event_body(line: str) -> dict[str, str]:
    """Extract event fields from JSON or logfmt Loki lines."""
    try:
        body = json.loads(line)
    except (json.JSONDecodeError, TypeError):
        body = {}
        match = _RE_LOGFMT_MESSAGE.search(line or "")
        if match:
            value = match.group(1)
            if value.startswith('"'):
                body["msg"] = json.loads(value)
            else:
                body["msg"] = value
    return body


# --- HTTP clients ------------------------------------------------------------


def _http_get_json(url: str, params: dict, token: str | None, timeout: int) -> dict:
    query = urllib.parse.urlencode(params)
    full = f"{url}?{query}"
    req = urllib.request.Request(full)
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310 (trusted operator URL)
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "replace")
        raise SystemExit(f"HTTP {e.code} from {url}: {body}") from e
    except urllib.error.URLError as e:
        raise SystemExit(f"Cannot reach {url}: {e.reason}") from e


def prom_instant(base: str, query: str, at: datetime, token: str | None, timeout: int) -> list[dict]:
    """Run an instant PromQL query; returns the result vector."""
    data = _http_get_json(
        base.rstrip("/") + "/api/v1/query",
        {"query": query, "time": at.timestamp()},
        token,
        timeout,
    )
    if data.get("status") != "success":
        raise SystemExit(f"Prometheus error: {data.get('error', data)}")
    return data["data"]["result"]


def prom_range(base: str, query: str, start: datetime, end: datetime, step: int, token: str | None, timeout: int) -> list[dict]:
    """Run a range PromQL query; returns the matrix result."""
    data = _http_get_json(
        base.rstrip("/") + "/api/v1/query_range",
        {"query": query, "start": start.timestamp(), "end": end.timestamp(), "step": f"{step}s"},
        token,
        timeout,
    )
    if data.get("status") != "success":
        raise SystemExit(f"Prometheus error: {data.get('error', data)}")
    return data["data"]["result"]


def loki_range(base: str, query: str, start: datetime, end: datetime, limit: int, token: str | None, timeout: int) -> list[dict]:
    """Run a LogQL range query; returns the stream result."""
    data = _http_get_json(
        base.rstrip("/") + "/loki/api/v1/query_range",
        {
            "query": query,
            "start": str(int(start.timestamp() * 1e9)),
            "end": str(int(end.timestamp() * 1e9)),
            "limit": limit,
            "direction": "forward",
        },
        token,
        timeout,
    )
    if data.get("status") != "success":
        raise SystemExit(f"Loki error: {data.get('error', data)}")
    return data["data"]["result"]


# --- Loki: image-pull events -> images.csv + kubernetes_events.csv -----------


def fetch_events(
    base: str,
    query: str,
    start: datetime,
    end: datetime,
    limit: int,
    bucket: timedelta,
    token: str | None,
    timeout: int,
) -> pd.DataFrame:
    """Parse Alloy-shipped Kubernetes events into a normalized event frame."""
    rows: list[dict] = []
    bucket_start = start
    while bucket_start < end:
        bucket_end = min(bucket_start + bucket, end)
        streams = loki_range(base, query, bucket_start, bucket_end, limit, token, timeout)
        for stream in streams:
            labels = stream.get("stream", {})
            for ts_nano, line in stream.get("values", []):
                reason = labels.get("reason", "")
                pod = labels.get("involvedObject_name") or labels.get("name", "")
                msg = labels.get("message", "")
                body = parse_event_body(line)
                reason = reason or body.get("reason", "")
                pod = pod or body.get("involvedObject_name") or body.get("name", "")
                msg = msg or body.get("message") or body.get("msg", "") or line
                image = parse_image_ref(msg)
                if not reason and msg:
                    reason = infer_reason(msg)
                if not image:
                    continue
                rows.append(
                    {
                        "pod": pod,
                        "image": image,
                        "reason": reason,
                        "timestamp": datetime.fromtimestamp(int(ts_nano) / 1e9, tz=timezone.utc),
                        "pull_seconds": parse_pull_duration_seconds(msg) if reason.lower() == "pulled" else np.nan,
                        "size_bytes": parse_image_size_bytes(msg) if reason.lower() == "pulled" else 0,
                    }
                )
        bucket_start = bucket_end
    return pd.DataFrame(rows)


def build_images(events: pd.DataFrame) -> pd.DataFrame:
    """Derive per-image p50 pull time and size from Pulled events."""
    if events.empty:
        return pd.DataFrame(columns=["image_id", "image", "digest", "size_mb", "p50_pull_seconds"])
    pulled = events[(events["reason"].str.lower() == "pulled") & events["pull_seconds"].notna() & (events["pull_seconds"] > 0)]
    records = []
    for image, g in events.groupby("image"):
        durations = pulled.loc[pulled["image"] == image, "pull_seconds"]
        sizes = pulled.loc[pulled["image"] == image, "size_bytes"]
        sizes = sizes[sizes > 0]
        records.append(
            {
                "image_id": image,
                "image": image,
                "digest": "",
                "size_mb": round(float(sizes.median()) / (1024 * 1024), 2) if len(sizes) else np.nan,
                "p50_pull_seconds": round(float(durations.median()), 3) if len(durations) else np.nan,
            }
        )
    images = pd.DataFrame(records)
    # Fill images that were never observed cold-pulled with the global median.
    global_p50 = images["p50_pull_seconds"].median()
    if pd.isna(global_p50):
        global_p50 = 0.0
    images["p50_pull_seconds"] = images["p50_pull_seconds"].fillna(round(float(global_p50), 3))
    return images


# --- Prometheus: kube-state-metrics -> gitlab_runner_jobs.csv ----------------


def _instant_map(result: list[dict], label: str | None = None) -> dict:
    """Map a series (by pod/namespace) to its scalar value or a chosen label."""
    out: dict[tuple[str, str], object] = {}
    for series in result:
        m = series.get("metric", {})
        key = (m.get("namespace", ""), m.get("pod", ""))
        if label is not None:
            out[key] = m.get(label, "")
        else:
            out[key] = float(series["value"][1])
    return out


def fetch_jobs(base: str, selector: str, lookback: str, at: datetime, default_runtime: float, token: str | None, timeout: int) -> pd.DataFrame:
    """Reconstruct per-pod jobs from kube-state-metrics using last_over_time."""

    def lot(metric: str) -> str:
        return f"last_over_time({metric}{{{selector}}}[{lookback}])"

    info = prom_instant(base, lot("kube_pod_info"), at, token, timeout)
    container = prom_instant(base, lot("kube_pod_container_info"), at, token, timeout)
    created = prom_instant(base, lot("kube_pod_created"), at, token, timeout)
    started = prom_instant(base, lot("kube_pod_start_time"), at, token, timeout)
    completed = prom_instant(base, lot("kube_pod_completion_time"), at, token, timeout)

    node_of = _instant_map(info, "node")
    image_of = _instant_map(container, "image")
    image_id_of = _instant_map(container, "image_id")
    created_of = _instant_map(created)
    started_of = _instant_map(started)
    completed_of = _instant_map(completed)

    rows = []
    for key in sorted(node_of):
        namespace, pod = key
        image = image_of.get(key, "")
        if not image:
            continue
        c = created_of.get(key)
        s = started_of.get(key, c)
        f = completed_of.get(key)
        pod_created = datetime.fromtimestamp(c, tz=timezone.utc) if c else pd.NaT
        pod_scheduled = datetime.fromtimestamp(s, tz=timezone.utc) if s else pod_created
        if f and s:
            useful = max(float(f) - float(s), 0.0)
            job_finished = datetime.fromtimestamp(f, tz=timezone.utc)
        else:
            useful = default_runtime
            job_finished = (pod_scheduled + timedelta(seconds=default_runtime)) if pod_scheduled is not pd.NaT else pd.NaT
        rows.append(
            {
                "job_id": pod,
                "pipeline_id": "",
                "stage": "",
                "pod": pod,
                "namespace": namespace,
                "node": node_of.get(key, ""),
                "image_id": image,
                "image": image,
                "digest": image_id_of.get(key, ""),
                "pod_created": pod_created,
                "pod_scheduled": pod_scheduled,
                "container_started": pod_scheduled,
                "job_script_started": pod_scheduled,
                "job_finished": job_finished,
                "useful_runtime_seconds": round(useful, 1),
            }
        )
    jobs = pd.DataFrame(rows)
    if jobs.empty:
        raise SystemExit(
            "No pods matched the selector. Check --pod-selector and that "
            "kube-state-metrics is scraped by Prometheus."
        )
    return jobs


def attach_observed(jobs: pd.DataFrame, events: pd.DataFrame) -> pd.DataFrame:
    """Join real pull events to each pod for the observed_* signals.

    A job is a "cold hit" if kubelet had to pull its image (a Pulling/Pulled
    event for that pod+image); the observed wait is the pull duration it
    experienced. Jobs whose image was already present wait zero.
    """
    jobs = jobs.copy()
    jobs["observed_image_wait_seconds"] = 0.0
    jobs["observed_startup_delay_seconds"] = 0.0
    jobs["observed_cold_hit"] = False
    if events.empty:
        return jobs

    pulled = events[(events["reason"].str.lower() == "pulled") & events["pull_seconds"].notna()]
    wait_by_pod_image = pulled.groupby(["pod", "image"])["pull_seconds"].max().to_dict()
    cold_pods = set(zip(events["pod"], events["image"]))  # any Pulling/Pulled/Failed event => a pull was attempted

    for idx, row in jobs.iterrows():
        key = (row["pod"], row["image"])
        wait = wait_by_pod_image.get(key)
        if wait is not None:
            jobs.at[idx, "observed_image_wait_seconds"] = float(wait)
            jobs.at[idx, "observed_startup_delay_seconds"] = float(wait)
            jobs.at[idx, "observed_cold_hit"] = True
        elif key in cold_pods:
            jobs.at[idx, "observed_cold_hit"] = True
    return jobs


def attach_pull_times(jobs: pd.DataFrame, images: pd.DataFrame) -> pd.DataFrame:
    """Ensure every job image has a p50 pull time; fill gaps with the median."""
    known = set(images["image_id"])
    global_p50 = images["p50_pull_seconds"].median()
    if pd.isna(global_p50):
        global_p50 = 0.0
    missing = sorted(set(jobs["image_id"]) - known)
    if missing:
        extra = pd.DataFrame(
            {
                "image_id": missing,
                "image": missing,
                "digest": "",
                "size_mb": np.nan,
                "p50_pull_seconds": round(float(global_p50), 3),
            }
        )
        images = pd.concat([images, extra], ignore_index=True)
    jobs = jobs.merge(images[["image_id", "p50_pull_seconds"]], on="image_id", how="left")
    return jobs, images


# --- Prometheus: image usage over time -> prometheus_image_samples_5m.csv -----


def fetch_usage_samples(base: str, query: str, start: datetime, end: datetime, step: int, token: str | None, timeout: int) -> pd.DataFrame:
    result = prom_range(base, query, start, end, step, token, timeout)
    rows = []
    for series in result:
        image = series.get("metric", {}).get("image", "")
        if not image:
            continue
        for ts, val in series.get("values", []):
            rows.append(
                {
                    "timestamp": datetime.fromtimestamp(float(ts), tz=timezone.utc),
                    "image_id": image,
                    "image": image,
                    "running_containers": float(val),
                }
            )
    return pd.DataFrame(rows)


# --- CLI ---------------------------------------------------------------------


def parse_time(value: str | None, default: datetime) -> datetime:
    if not value:
        return default
    return pd.Timestamp(value).to_pydatetime().astimezone(timezone.utc)


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Fetch benchmark input CSVs from a cluster's Prometheus and Loki.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    ap.add_argument("--prometheus-url", required=True, help="Prometheus base URL, e.g. http://localhost:9090")
    ap.add_argument("--loki-url", required=True, help="Loki base URL, e.g. http://localhost:3100")
    ap.add_argument("--start", help="Window start (RFC3339). Default: end - lookback.")
    ap.add_argument("--end", help="Window end (RFC3339). Default: now.")
    ap.add_argument("--lookback", default="24h", help="PromQL range vector window for job reconstruction.")
    ap.add_argument("--pod-selector", default='pod=~"runner-.*"', help="kube-state-metrics label selector for runner pods.")
    ap.add_argument(
        "--loki-query",
        default='{job="kubernetes-events",reason=~"Pulling|Pulled|AlreadyPresent"}',
        help="LogQL selector for Alloy-shipped Kubernetes events.",
    )
    ap.add_argument(
        "--usage-query",
        default='count(kube_pod_container_info{pod=~"runner-.*"}) by (image)',
        help="PromQL for running-container usage over time (grouped by image).",
    )
    ap.add_argument("--step", type=int, default=300, help="Usage-sample resolution in seconds.")
    ap.add_argument("--default-runtime", type=float, default=180.0, help="Fallback useful runtime (s) when completion time is missing.")
    ap.add_argument("--loki-limit", type=int, default=5000, help="Max Loki log entries to fetch.")
    ap.add_argument("--loki-bucket", default="1h", help="Loki query window per request.")
    ap.add_argument("--token", help="Bearer token for both APIs (or set FETCH_TOKEN).")
    ap.add_argument("--timeout", type=int, default=60, help="Per-request timeout in seconds.")
    ap.add_argument("--out", default="data", help="Output directory for the CSVs.")
    args = ap.parse_args()

    token = args.token or os.environ.get("FETCH_TOKEN")

    end = parse_time(args.end, datetime.now(timezone.utc))
    lookback_td = pd.Timedelta(args.lookback).to_pytimedelta()
    loki_bucket = pd.Timedelta(args.loki_bucket).to_pytimedelta()
    if loki_bucket <= timedelta(0):
        raise SystemExit("--loki-bucket must be greater than zero")
    start = parse_time(args.start, end - lookback_td)

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    print(f"Window: {start.isoformat()} -> {end.isoformat()}", file=sys.stderr)

    print("Fetching image-pull events from Loki ...", file=sys.stderr)
    events = fetch_events(args.loki_url, args.loki_query, start, end, args.loki_limit, loki_bucket, token, args.timeout)
    print(f"  parsed {len(events)} events", file=sys.stderr)
    events.to_csv(out / "kubernetes_events.csv", index=False)

    images = build_images(events)
    print(f"  derived {len(images)} images with pull times", file=sys.stderr)

    print("Reconstructing jobs from kube-state-metrics ...", file=sys.stderr)
    jobs = fetch_jobs(args.prometheus_url, args.pod_selector, args.lookback, end, args.default_runtime, token, args.timeout)
    jobs = attach_observed(jobs, events)
    jobs, images = attach_pull_times(jobs, images)
    print(f"  reconstructed {len(jobs)} jobs across {jobs['node'].nunique()} nodes", file=sys.stderr)

    print("Fetching image-usage samples from Prometheus ...", file=sys.stderr)
    usage = fetch_usage_samples(args.prometheus_url, args.usage_query, start, end, args.step, token, args.timeout)
    print(f"  {len(usage)} usage samples", file=sys.stderr)

    images.to_csv(out / "images.csv", index=False)
    jobs.to_csv(out / "gitlab_runner_jobs.csv", index=False)
    usage.to_csv(out / "prometheus_image_samples_5m.csv", index=False)

    print(f"\nWrote CSVs to {out}/:", file=sys.stderr)
    for name in ("images.csv", "gitlab_runner_jobs.csv", "prometheus_image_samples_5m.csv", "kubernetes_events.csv"):
        print(f"  {name}", file=sys.stderr)
    print(
        "\nNext: python evaluate_replay.py --data "
        f"{out} --out outputs && python evaluate_discovery_strategies.py --data {out} --out outputs/strategy_eval",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
