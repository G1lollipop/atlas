#!/usr/bin/env python3
"""Plot measured Atlas experiment trials and summarize complete batches only."""

import argparse
import collections
import gzip
import hashlib
import json
import math
import statistics
import textwrap
from pathlib import Path

import matplotlib.pyplot as plt


METRICS = {
    "throughput_jobs_per_second": "whole_batch_execution_throughput_jobs_per_second",
    "p95_ready_queue_wait_ms": "p95_ready_queue_wait_ms",
    "max_ready_queue_wait_ms": "max_ready_queue_wait_ms",
    "db_cpu_core_equivalents": "mean_db_cpu_core_equivalents",
    "sampled_lock_wait_fraction": "mean_sampled_lock_wait_fraction",
    "observed_submission_rate_jobs_per_second": "observed_submission_rate_jobs_per_second",
    "gpu_vram_reservation_ratio": "mean_gpu_vram_reservation_ratio",
    "gpu_device_reservation_ratio": "_mean_gpu_device_reservation_ratio",
    "whole_device_stranded_vram_mb": "mean_whole_device_stranded_vram_mb",
    "small_gpu_placement_fraction": None,
}

CPU_AXES = [
    "run_id",
    "source_report",
    "offered_submission_rate_jobs_per_second",
    "policy",
    "worker_count",
]
LOAD_AXES = [
    "run_id",
    "source_report",
    "policy",
    "worker_count",
    "offered_submission_rate_jobs_per_second",
]
MIXED_AXES = [
    "run_id",
    "source_report",
    "offered_submission_rate_jobs_per_second",
    "policy",
    "worker_count",
]


def number(value):
    if value is None or isinstance(value, bool):
        return math.nan
    try:
        result = float(value)
    except (TypeError, ValueError):
        return math.nan
    return result if math.isfinite(result) else math.nan


def file_sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def trial_completion(row):
    explicit_error = str(row.get("error") or "").strip()
    if explicit_error:
        return False, explicit_error

    counts = {
        field: number(row.get(field))
        for field in (
            "jobs_planned",
            "jobs_accepted",
            "jobs_succeeded",
            "jobs_dead_or_failed",
            "unfinished_at_deadline",
        )
    }
    missing = [field for field, value in counts.items() if not math.isfinite(value)]
    if missing:
        return False, "missing completion counters: " + ", ".join(missing)

    planned = counts["jobs_planned"]
    if (
        counts["jobs_accepted"] != planned
        or counts["jobs_succeeded"] != planned
        or counts["jobs_dead_or_failed"] != 0
        or counts["unfinished_at_deadline"] != 0
    ):
        return (
            False,
            "incomplete batch "
            f"(planned={planned:g}, accepted={counts['jobs_accepted']:g}, "
            f"succeeded={counts['jobs_succeeded']:g}, "
            f"failed_or_dead={counts['jobs_dead_or_failed']:g}, "
            f"unfinished={counts['unfinished_at_deadline']:g})",
        )
    return True, ""


def load_reports(paths):
    rows = []
    report_summaries = []
    excluded = []

    for path in paths:
        opener = gzip.open if path.suffix.lower() == ".gz" else open
        with opener(path, "rt", encoding="utf-8") as report_file:
            report = json.load(report_file)
        if not isinstance(report, dict):
            raise SystemExit(f"report root must be a JSON object: {path}")

        run_id = str(report.get("run_id") or path.stem)
        source = str(path.resolve())
        configuration = report.get("configuration")
        if not isinstance(configuration, dict):
            configuration = {}
        history_reset = configuration.get("history_reset_between_trials")
        policy_order = configuration.get("policy_order")
        policy_order_boundary = configuration.get("policy_order_bias_boundary")
        samples = report.get("database_samples", [])
        if not isinstance(samples, list):
            samples = []
        samples_by_scenario = collections.defaultdict(list)
        for sample in samples:
            if not isinstance(sample, dict):
                continue
            reserved = number(sample.get("gpu_reserved_devices"))
            available = number(sample.get("gpu_available_devices"))
            total = reserved + available
            if math.isfinite(total) and total > 0 and math.isfinite(reserved):
                samples_by_scenario[str(sample.get("scenario_id") or "")].append(reserved / total)

        trials = report.get("trials", [])
        if not isinstance(trials, list):
            raise SystemExit(f"report trials must be a JSON array: {path}")

        complete_count = 0
        for index, trial in enumerate(trials, start=1):
            if not isinstance(trial, dict):
                excluded.append(
                    {
                        "run_id": run_id,
                        "source_report": source,
                        "trial_index": index,
                        "reason": "trial entry is not a JSON object",
                    }
                )
                print(f"INCOMPLETE {run_id} trial #{index}: trial entry is not a JSON object")
                continue
            row = dict(trial)
            row["run_id"] = run_id
            row["source_report"] = source
            row["_history_reset_between_trials"] = history_reset
            row["_policy_order"] = policy_order
            row["_policy_order_bias_boundary"] = policy_order_boundary
            sample_ratios = samples_by_scenario.get(str(row.get("scenario_id") or ""), [])
            row["_mean_gpu_device_reservation_ratio"] = (
                statistics.mean(sample_ratios) if sample_ratios else math.nan
            )
            row["_complete"], row["_incomplete_reason"] = trial_completion(row)
            rows.append(row)
            if row["_complete"]:
                complete_count += 1
            else:
                reason = row["_incomplete_reason"]
                excluded.append(
                    {
                        "run_id": run_id,
                        "source_report": source,
                        "scenario_id": row.get("scenario_id"),
                        "policy": row.get("policy"),
                        "worker_count": row.get("worker_count"),
                        "offered_submission_rate_jobs_per_second": row.get(
                            "offered_submission_rate_jobs_per_second"
                        ),
                        "reason": reason,
                    }
                )
                print(
                    f"INCOMPLETE {run_id} {row.get('scenario_id', '(unknown trial)')}: "
                    f"{reason}"
                )

        report_summaries.append(
            {
                "run_id": run_id,
                "source_report": source,
                "source_sha256": file_sha256(path),
                "source_bytes": path.stat().st_size,
                "history_reset_between_trials": history_reset,
                "policy_order": policy_order,
                "policy_order_bias_boundary": policy_order_boundary,
                "trial_count": len(trials),
                "complete_trial_count": complete_count,
                "excluded_trial_count": len(trials) - complete_count,
                "report_errors": report.get("errors", []) or [],
            }
        )
        for error in report.get("errors", []) or []:
            print(f"REPORT ERROR {run_id}: {error}")

    if not rows:
        raise SystemExit("input reports contain no benchmark trials")
    if not any(row["_complete"] for row in rows):
        print("No complete trials; figures will show measured failure points only.")
    return rows, report_summaries, excluded


def metric_value(row, metric):
    if metric.startswith("p95_ready_wait_ms_by_workload:"):
        workload = metric.partition(":")[2]
        by_workload = row.get("p95_ready_wait_ms_by_workload")
        if not isinstance(by_workload, dict):
            return math.nan
        return number(by_workload.get(workload))
    source_field = METRICS[metric]
    if source_field is not None:
        return number(row.get(source_field))

    accepted = number(row.get("small_gpu_jobs_accepted"))
    placed = number(row.get("small_gpu_jobs_placed_on_large_gpu"))
    if not math.isfinite(accepted) or accepted <= 0 or not math.isfinite(placed):
        return math.nan
    return placed / accepted


def summarize(values):
    finite = [value for value in values if math.isfinite(value)]
    if not finite:
        return None
    return {
        "median": float(statistics.median(finite)),
        "minimum": float(min(finite)),
        "maximum": float(max(finite)),
        "count": len(finite),
    }


def aggregate(rows, axes, metrics):
    groups = collections.defaultdict(list)
    for row in rows:
        if not row["_complete"]:
            continue
        key = tuple(row.get(axis) for axis in axes)
        groups[key].append(row)

    result = {}
    for key, group in groups.items():
        axis_values = {axis: group[0].get(axis) for axis in axes}
        metric_summaries = {}
        for metric in metrics:
            summary = summarize([metric_value(row, metric) for row in group])
            if summary is not None:
                metric_summaries[metric] = summary
        if not metric_summaries:
            continue
        result[json.dumps(axis_values, sort_keys=True, separators=(",", ":"))] = {
            "axes": axis_values,
            "metrics": metric_summaries,
        }
    return result


def slug(value):
    text = str(value).strip().lower()
    return "".join(char if char.isalnum() or char in "-_." else "-" for char in text).strip("-") or "report"


def save_figure(fig, outdir, stem):
    png = outdir / f"{stem}.png"
    svg = outdir / f"{stem}.svg"
    fig.savefig(png, dpi=160)
    fig.savefig(svg, format="svg")
    svg_lines = svg.read_bytes().splitlines()
    svg.write_bytes(b"\n".join(line.rstrip(b" \t\r") for line in svg_lines) + b"\n")
    plt.close(fig)
    return [png.name, svg.name]


def summary_points(rows, axes, metrics):
    return list(aggregate(rows, axes, metrics).values())


def report_note(rows, report_id, source):
    selected = [
        row for row in rows
        if row["run_id"] == report_id and row["source_report"] == source
    ]
    if not selected:
        return ""
    reset_values = {row.get("_history_reset_between_trials") for row in selected}
    if reset_values == {True}:
        history = "reset between trials"
    elif reset_values == {False}:
        history = "history retained; diagnostic only"
    else:
        history = "reset status unknown; diagnostic only"
    return history


def rate_label(rate):
    return "unpaced" if math.isclose(rate, 0.0) else f"{rate:g} jobs/s"


def add_figure_header(fig, title, rows, report_id, source, detail=None):
    caption_parts = [f"run {report_id[-8:]}", report_note(rows, report_id, source)]
    if any(
        row.get("_policy_order_bias_boundary")
        for row in rows
        if row["run_id"] == report_id and row["source_report"] == source
    ):
        caption_parts.append("machine time/load drift may remain")
    if detail:
        caption_parts.append(detail)
    caption = " · ".join(part for part in caption_parts if part)
    lines = textwrap.wrap(caption, width=110, break_long_words=False, break_on_hyphens=False)
    fig.suptitle(title, fontsize=13, y=0.99)
    fig.text(0.5, 0.95, "\n".join(lines), ha="center", va="top", fontsize=9)
    layout = fig.get_layout_engine()
    if layout is not None:
        top = max(0.80, 0.90 - 0.035 * max(0, len(lines) - 1))
        layout.set(rect=(0, 0, 1, top))


def range_error(summary):
    median = summary["median"]
    return median, [median - summary["minimum"]], [summary["maximum"] - median]


def add_failed_points(ax, failures, x_by_worker, policy_offsets, policy_count, metric, marker_label_state):
    for row in failures:
        worker = number(row.get("worker_count"))
        policy = str(row.get("policy") or "")
        value = metric_value(row, metric)
        if not math.isfinite(worker) or not math.isfinite(value) or int(worker) not in x_by_worker:
            continue
        base = x_by_worker[int(worker)]
        offset = policy_offsets.get(policy, 0)
        x = base - 0.4 + (0.8 / max(1, policy_count)) / 2 + offset * (0.8 / max(1, policy_count))
        label = "failed / partial (not aggregated)" if not marker_label_state[0] else None
        ax.scatter([x], [value], marker="x", color="#c33", s=48, zorder=5, label=label)
        marker_label_state[0] = True


def grouped_bar_metric(ax, entries, failures, metric, policies, workers):
    policy_offsets = {policy: i for i, policy in enumerate(policies)}
    x_by_worker = {worker: index for index, worker in enumerate(workers)}
    width = 0.8 / max(1, len(policies))

    for policy in policies:
        positions = []
        values = []
        lows = []
        highs = []
        for worker in workers:
            entry = entries.get((policy, worker))
            summary = entry["metrics"].get(metric) if entry else None
            if summary is None:
                continue
            median, low, high = range_error(summary)
            positions.append(x_by_worker[worker] - 0.4 + width / 2 + policy_offsets[policy] * width)
            values.append(median)
            lows.append(low[0])
            highs.append(high[0])
        if positions:
            ax.bar(
                positions,
                values,
                width=width,
                label=policy,
                yerr=[lows, highs],
                capsize=3,
            )

    marker_label_state = [False]
    add_failed_points(
        ax, failures, x_by_worker, policy_offsets, len(policies), metric, marker_label_state
    )
    ax.set_xticks(list(x_by_worker.values()), [str(worker) for worker in workers])
    ax.set_xlabel("worker processes")
    ax.grid(axis="y", alpha=0.25)
    handles, labels = ax.get_legend_handles_labels()
    if handles:
        ax.legend(handles, labels, fontsize=8)


def grouped_workload_wait_metric(ax, complete, failures, policies, workers):
    workload_names = sorted(
        {
            name
            for row in complete + failures
            for name in (row.get("p95_ready_wait_ms_by_workload") or {})
        }
    )
    slot_count = max(1, len(policies) * len(workload_names))
    width = 0.8 / slot_count
    x_by_worker = {worker: index for index, worker in enumerate(workers)}
    failed_label_used = False

    for policy_index, policy in enumerate(policies):
        for workload_index, workload in enumerate(workload_names):
            slot = policy_index * len(workload_names) + workload_index
            positions, values, lows, highs, counts = [], [], [], [], []
            metric = f"p95_ready_wait_ms_by_workload:{workload}"
            for worker in workers:
                grouped_values = [
                    metric_value(row, metric)
                    for row in complete
                    if row.get("policy") == policy
                    and number(row.get("worker_count")) == worker
                ]
                summary = summarize(grouped_values)
                if summary is None:
                    continue
                positions.append(x_by_worker[worker] - 0.4 + width / 2 + slot * width)
                values.append(summary["median"])
                lows.append(summary["median"] - summary["minimum"])
                highs.append(summary["maximum"] - summary["median"])
                counts.append(summary["count"])
            if positions:
                bars = ax.bar(
                    positions,
                    values,
                    width=width,
                    label=f"{policy} / {workload}",
                    yerr=[lows, highs],
                    capsize=2,
                )
                for bar, count in zip(bars, counts):
                    ax.annotate(
                        f"n={count}",
                        (bar.get_x() + bar.get_width() / 2, bar.get_height()),
                        xytext=(0, 2),
                        textcoords="offset points",
                        ha="center",
                        va="bottom",
                        fontsize=6,
                        rotation=90,
                    )
            for row in failures:
                if row.get("policy") != policy or number(row.get("worker_count")) not in workers:
                    continue
                value = metric_value(row, metric)
                if not math.isfinite(value):
                    continue
                worker = int(number(row.get("worker_count")))
                x = x_by_worker[worker] - 0.4 + width / 2 + slot * width
                ax.scatter(
                    [x], [value], marker="x", color="#c33", s=40, zorder=5,
                    label="failed / partial (not aggregated)" if not failed_label_used else None,
                )
                failed_label_used = True

    ax.set_xticks(list(x_by_worker.values()), [str(worker) for worker in workers])
    ax.set_xlabel("worker processes")
    ax.grid(axis="y", alpha=0.25)
    handles, labels = ax.get_legend_handles_labels()
    if handles:
        ax.legend(handles, labels, fontsize=6, ncols=2)


def plot_cpu_worker_sweep(rows, outdir, report_id, source, rate):
    complete = [
        row for row in rows
        if row["_complete"]
        and row["run_id"] == report_id
        and row["source_report"] == source
        and row.get("workload_profile") == "cpu"
        and math.isclose(number(row.get("offered_submission_rate_jobs_per_second")), rate)
    ]
    failures = [
        row for row in rows
        if not row["_complete"]
        and row["run_id"] == report_id
        and row["source_report"] == source
        and row.get("workload_profile") == "cpu"
        and math.isclose(number(row.get("offered_submission_rate_jobs_per_second")), rate)
    ]
    policies = sorted({str(row.get("policy") or "") for row in complete + failures})
    workers = sorted(
        {int(number(row.get("worker_count"))) for row in complete + failures
         if math.isfinite(number(row.get("worker_count")))}
    )
    if not workers:
        return []

    points = summary_points(
        complete,
        ["policy", "worker_count"],
        ["throughput_jobs_per_second", "p95_ready_queue_wait_ms"],
    )
    entries = {
        (str(point["axes"].get("policy") or ""), int(point["axes"]["worker_count"])): point
        for point in points
    }
    if not any(
        math.isfinite(metric_value(row, metric))
        for row in failures
        for metric in ("throughput_jobs_per_second", "p95_ready_queue_wait_ms")
    ) and not entries:
        return []

    fig, axes = plt.subplots(1, 2, figsize=(11, 4.8), layout="constrained")
    grouped_bar_metric(
        axes[0], entries, failures, "throughput_jobs_per_second", policies, workers
    )
    axes[0].set_ylabel("whole-batch execution throughput (jobs/s)")
    axes[0].set_title("Median throughput; bars span min–max trials")

    grouped_bar_metric(
        axes[1], entries, failures, "p95_ready_queue_wait_ms", policies, workers
    )
    axes[1].set_ylabel("p95 ready-queue wait (ms)")
    axes[1].set_title("Median p95 wait; bars span min–max trials")
    add_figure_header(
        fig,
        "CPU worker scaling",
        rows,
        report_id,
        source,
        f"offered load: {rate_label(rate)} · incomplete trials marked × and excluded",
    )
    stem = f"cpu-workers-{slug(report_id)}-rate-{slug(f'{rate:g}')}"
    return save_figure(fig, outdir, stem)


def plot_cpu_load(rows, outdir, report_id, source):
    complete = [
        row for row in rows
        if row["_complete"]
        and row["run_id"] == report_id
        and row["source_report"] == source
        and row.get("workload_profile") == "cpu"
    ]
    failures = [
        row for row in rows
        if not row["_complete"]
        and row["run_id"] == report_id
        and row["source_report"] == source
        and row.get("workload_profile") == "cpu"
    ]
    if not complete and not failures:
        return []

    metrics = [
        "db_cpu_core_equivalents",
        "sampled_lock_wait_fraction",
        "observed_submission_rate_jobs_per_second",
        "throughput_jobs_per_second",
    ]
    entries = summary_points(
        complete,
        ["policy", "worker_count", "offered_submission_rate_jobs_per_second"],
        metrics,
    )
    if not entries and not any(
        math.isfinite(metric_value(row, metric)) for row in failures for metric in metrics
    ):
        return []

    fig, axes = plt.subplots(2, 2, figsize=(12, 8), layout="constrained")
    series = sorted({
        (str(point["axes"].get("policy") or ""), int(point["axes"]["worker_count"]))
        for point in entries
    } | {
        (str(row.get("policy") or ""), int(number(row.get("worker_count"))))
        for row in failures if math.isfinite(number(row.get("worker_count")))
    })
    metric_titles = [
        ("db_cpu_core_equivalents", "PostgreSQL process CPU (core-equivalents)"),
        ("sampled_lock_wait_fraction", "sampled lock-wait fraction"),
        ("observed_submission_rate_jobs_per_second", "observed submission rate (jobs/s)"),
        ("throughput_jobs_per_second", "whole-batch execution throughput (jobs/s)"),
    ]
    for ax, (metric, ylabel) in zip(axes.flat, metric_titles):
        for policy, worker_count in series:
            x_values, values, lows, highs = [], [], [], []
            for point in entries:
                axis = point["axes"]
                if axis.get("policy") != policy or int(axis["worker_count"]) != worker_count:
                    continue
                stat = point["metrics"].get(metric)
                rate = number(axis.get("offered_submission_rate_jobs_per_second"))
                if stat is None or not math.isfinite(rate):
                    continue
                median, low, high = range_error(stat)
                x_values.append(rate)
                values.append(median)
                lows.append(low[0])
                highs.append(high[0])
            if x_values:
                ax.errorbar(
                    x_values,
                    values,
                    yerr=[lows, highs],
                    marker="o",
                    capsize=3,
                    label=f"{policy}, {worker_count} workers",
                )
        marked = False
        for row in failures:
            rate = number(row.get("offered_submission_rate_jobs_per_second"))
            value = metric_value(row, metric)
            if not math.isfinite(rate) or not math.isfinite(value):
                continue
            ax.scatter(
                [rate], [value], marker="x", color="#c33", s=45,
                label="failed / partial (not aggregated)" if not marked else None,
                zorder=5,
            )
            marked = True
        ax.set_xlabel("offered submission rate (jobs/s; 0 = unpaced)")
        ax.set_ylabel(ylabel)
        ax.grid(alpha=0.25)
        if ax.get_legend_handles_labels()[0]:
            ax.legend(fontsize=7, loc="best")
    add_figure_header(
        fig,
        "CPU offered-load sweep",
        rows,
        report_id,
        source,
        "medians with min–max complete-trial ranges · × marks excluded trials",
    )
    return save_figure(fig, outdir, f"cpu-load-{slug(report_id)}")


def plot_mixed_policy(rows, outdir, report_id, source, rate):
    complete = [
        row for row in rows
        if row["_complete"]
        and row["run_id"] == report_id
        and row["source_report"] == source
        and row.get("workload_profile") == "mixed"
        and math.isclose(number(row.get("offered_submission_rate_jobs_per_second")), rate)
    ]
    failures = [
        row for row in rows
        if not row["_complete"]
        and row["run_id"] == report_id
        and row["source_report"] == source
        and row.get("workload_profile") == "mixed"
        and math.isclose(number(row.get("offered_submission_rate_jobs_per_second")), rate)
    ]
    policies = sorted({str(row.get("policy") or "") for row in complete + failures})
    workers = sorted(
        {int(number(row.get("worker_count"))) for row in complete + failures
         if math.isfinite(number(row.get("worker_count")))}
    )
    if not workers:
        return []

    metrics = [
        "throughput_jobs_per_second",
        "gpu_vram_reservation_ratio",
        "gpu_device_reservation_ratio",
        "whole_device_stranded_vram_mb",
        "small_gpu_placement_fraction",
        "max_ready_queue_wait_ms",
    ]
    points = summary_points(complete, ["policy", "worker_count"], metrics)
    entries = {
        (str(point["axes"].get("policy") or ""), int(point["axes"]["worker_count"])): point
        for point in points
    }
    if not entries and not any(
        math.isfinite(metric_value(row, metric)) for row in failures for metric in metrics
    ):
        return []

    fig, axes = plt.subplots(2, 3, figsize=(15, 9), layout="constrained")
    titles = [
        ("throughput_jobs_per_second", "whole-batch throughput (jobs/s)"),
        ("gpu_vram_reservation_ratio", "mean simulated VRAM reservation ratio"),
        ("gpu_device_reservation_ratio", "mean simulated GPU-device reservation fraction"),
        ("whole_device_stranded_vram_mb", "whole-device stranded VRAM proxy (MB)"),
        ("small_gpu_placement_fraction", "small GPU jobs placed on large devices (fraction)"),
        ("max_ready_queue_wait_ms", "maximum job ready-queue wait (ms)"),
    ]
    for ax, (metric, ylabel) in zip(axes.flat, titles):
        if ax is axes[0, 1]:
            grouped_workload_wait_metric(ax, complete, failures, policies, workers)
            ax.set_ylabel("p95 ready-queue wait by workload class (ms)")
            ax.set_title("Median class p95; min–max across complete trials")
            continue
        grouped_bar_metric(ax, entries, failures, metric, policies, workers)
        ax.set_ylabel(ylabel)
        if metric in ("gpu_vram_reservation_ratio", "gpu_device_reservation_ratio", "small_gpu_placement_fraction"):
            ax.set_ylim(bottom=0, top=1)
        if metric == "max_ready_queue_wait_ms":
            ax.text(
                0.02,
                0.98,
                "Aggregated batches have zero unfinished jobs.",
                transform=ax.transAxes,
                ha="left",
                va="top",
                fontsize=8,
            )
    add_figure_header(
        fig,
        "Mixed policy comparison",
        rows,
        report_id,
        source,
        f"offered load: {rate_label(rate)} · medians with min–max ranges · GPU values are reservation proxies",
    )
    stem = f"mixed-policy-{slug(report_id)}-rate-{slug(f'{rate:g}')}"
    return save_figure(fig, outdir, stem)


def build_summary(rows, reports, excluded):
    cpu_metrics = ["throughput_jobs_per_second", "p95_ready_queue_wait_ms"]
    load_metrics = [
        "db_cpu_core_equivalents",
        "sampled_lock_wait_fraction",
        "observed_submission_rate_jobs_per_second",
        "throughput_jobs_per_second",
    ]
    mixed_metrics = [
        "throughput_jobs_per_second",
        "p95_ready_queue_wait_ms",
        "max_ready_queue_wait_ms",
        "gpu_vram_reservation_ratio",
        "gpu_device_reservation_ratio",
        "whole_device_stranded_vram_mb",
        "small_gpu_placement_fraction",
    ]
    mixed_workloads = sorted(
        {
            name
            for row in rows
            if row.get("workload_profile") == "mixed"
            and isinstance(row.get("p95_ready_wait_ms_by_workload"), dict)
            for name in row["p95_ready_wait_ms_by_workload"]
        }
    )
    mixed_metrics.extend(
        f"p95_ready_wait_ms_by_workload:{name}" for name in mixed_workloads
    )
    return {
        "reports": reports,
        "complete_trial_count": sum(1 for row in rows if row["_complete"]),
        "excluded_trials": excluded,
        "trial_integrity": [
            {
                "run_id": row["run_id"],
                "source_report": row["source_report"],
                "scenario_id": row.get("scenario_id"),
                "trial": row.get("trial"),
                "jobs_planned": row.get("jobs_planned"),
                "jobs_accepted": row.get("jobs_accepted"),
                "jobs_succeeded": row.get("jobs_succeeded"),
                "jobs_dead_or_failed": row.get("jobs_dead_or_failed"),
                "unfinished_at_deadline": row.get("unfinished_at_deadline"),
                "complete": row["_complete"],
                "error": row.get("error") or None,
            }
            for row in rows
        ],
        "aggregates": {
            "cpu_worker_sweep": aggregate(
                [row for row in rows if row.get("workload_profile") == "cpu"],
                CPU_AXES,
                cpu_metrics,
            ),
            "cpu_offered_load": aggregate(
                [row for row in rows if row.get("workload_profile") == "cpu"],
                LOAD_AXES,
                load_metrics,
            ),
            "mixed_policy": aggregate(
                [row for row in rows if row.get("workload_profile") == "mixed"],
                MIXED_AXES,
                mixed_metrics,
            ),
        },
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("reports", nargs="+", type=Path, help="one or more experiment report.json files")
    parser.add_argument("--out", required=True, type=Path, help="directory for PNG, SVG, and summary.json")
    args = parser.parse_args()

    rows, reports, excluded = load_reports(args.reports)
    args.out.mkdir(parents=True, exist_ok=True)
    generated = []
    report_pairs = sorted({(row["run_id"], row["source_report"]) for row in rows})

    for run_id, source in report_pairs:
        cpu_rows = [
            row for row in rows
            if row["run_id"] == run_id and row["source_report"] == source
            and row.get("workload_profile") == "cpu"
        ]
        cpu_rates = sorted({
            number(row.get("offered_submission_rate_jobs_per_second"))
            for row in cpu_rows
            if math.isfinite(number(row.get("offered_submission_rate_jobs_per_second")))
        })
        for rate in cpu_rates:
            generated.extend(plot_cpu_worker_sweep(rows, args.out, run_id, source, rate))
        generated.extend(plot_cpu_load(rows, args.out, run_id, source))

        mixed_rows = [
            row for row in rows
            if row["run_id"] == run_id and row["source_report"] == source
            and row.get("workload_profile") == "mixed"
        ]
        mixed_rates = sorted({
            number(row.get("offered_submission_rate_jobs_per_second"))
            for row in mixed_rows
            if math.isfinite(number(row.get("offered_submission_rate_jobs_per_second")))
        })
        for rate in mixed_rates:
            generated.extend(plot_mixed_policy(rows, args.out, run_id, source, rate))

    summary = build_summary(rows, reports, excluded)
    summary["figures"] = generated
    summary_path = args.out / "summary.json"
    with summary_path.open("w", encoding="utf-8") as summary_file:
        json.dump(summary, summary_file, indent=2, sort_keys=True)
        summary_file.write("\n")

    print(
        f"{summary['complete_trial_count']} complete trial(s), "
        f"{len(excluded)} excluded incomplete/failed trial(s) across {len(reports)} report(s)"
    )
    if not generated:
        print("No figures generated: no recognized measured CPU or mixed-workload fields were present.")
    else:
        print(f"Wrote {len(generated)} figure file(s) and {summary_path}")
        for name in generated:
            print(f"  {name}")
    if not any(row["_complete"] for row in rows):
        raise SystemExit("no complete trials to aggregate; failure points and summary were retained")


if __name__ == "__main__":
    main()
