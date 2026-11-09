"""Synthetic cluster metrics for test_prometheus_live.py, as OpenMetrics for backfill.

Samples every 60 s from 2026-10-01 to 2026-11-09 15:00 UTC, labelled as
kube-prometheus-stack labels them (job, metrics_path, image, pod, node):

- waldur-full: 2 cores, 4 GiB and 10 GiB stored from Oct 1, with cAdvisor's pod-level
  rollup row (container="");
- waldur-half: 4 cores, 4 GiB and 1 GPU from Oct 16;
- kube-state-metrics kube_pod_status_phase for every pod with a GPU request;
- waldur-gpu: init-container GPU requests, a Succeeded pod still carrying its request,
  a Pending pod, and one container reported by two kube-state-metrics replicas;
- negative "tenant-published" series without the pinned job (waldur-full, waldur-half);
- a duplicate /metrics/resource source for waldur-full (no image label);
- waldur-spoof: a negative series that does carry the pinned labels (honorLabels case);
- waldur-rwx: one RWX PVC reported by two kubelets, plus its KSM capacity request;
- waldur-burst: a 2 GiB container and a 1-GPU pod running :10-:40 every hour;
- waldur-straddle: a 1 GiB container and a 1-GPU pod running :55-:05 every hour.

No staleness markers: an OpenMetrics backfill cannot write them, so a series that stops
stays visible for Prometheus's 5-minute lookback.

    python generate_openmetrics.py synthetic.om
    promtool tsdb create-blocks-from openmetrics synthetic.om <prometheus data dir>
"""

import datetime as dt
import sys

UTC = dt.timezone.utc
T0 = int(dt.datetime(2026, 10, 1, tzinfo=UTC).timestamp())
THALF = int(dt.datetime(2026, 10, 16, tzinfo=UTC).timestamp())
TDONE = int(dt.datetime(2026, 10, 11, tzinfo=UTC).timestamp())
TEND = int(dt.datetime(2026, 11, 9, 15, 0, tzinfo=UTC).timestamp())
STEP = 60
GiB = 1024**3

CADVISOR = {"job": "kubelet", "metrics_path": "/metrics/cadvisor", "node": "n1"}
RESOURCE = {"job": "kubelet", "metrics_path": "/metrics/resource", "node": "n1"}
KUBELET = {"job": "kubelet", "metrics_path": "/metrics"}
KSM = {"job": "kube-state-metrics", "instance": "ksm-0"}
TENANT = {"job": "tenant-exporter"}

families: dict[str, list[str]] = {}
TYPES = {
    "container_cpu_usage_seconds": "counter",
    "container_memory_working_set_bytes": "gauge",
    "kubelet_volume_stats_used_bytes": "gauge",
    "kube_pod_container_resource_requests": "gauge",
    "kube_pod_init_container_resource_requests": "gauge",
    "kube_pod_status_phase": "gauge",
    "kube_persistentvolumeclaim_resource_requests_storage_bytes": "gauge",
}


def series(name, labels, start, fn, end=TEND):
    family = name[: -len("_total")] if name.endswith("_total") else name
    lab = ",".join(f'{k}="{v}"' for k, v in labels.items())
    lines = families.setdefault(family, [])
    for t in range(start, end + 1, STEP):
        value = fn(t)
        if value is not None:
            lines.append(f"{name}{{{lab}}} {value} {t}\n")


def phases(namespace, pod, start, end, running_until, phase_after="Succeeded"):
    """kube_pod_status_phase for a pod: 1 for its phase, 0 for the others."""
    for phase in ("Pending", "Running", "Succeeded", "Failed"):
        def fn(t, phase=phase):
            current = "Running" if t < running_until else phase_after
            return 1 if phase == current else 0
        series("kube_pod_status_phase", {**KSM, "namespace": namespace, "pod": pod, "phase": phase}, start, fn, end)


# --- The original series, now with the scrape labels --------------------------------
full_c = {**CADVISOR, "namespace": "waldur-full", "pod": "full-0"}
half_c = {**CADVISOR, "namespace": "waldur-half", "pod": "half-0"}
series("container_cpu_usage_seconds_total", {**full_c, "container": "app", "image": "app:1"}, T0, lambda t: 2 * (t - T0))
# cAdvisor's pod-level rollup row: no container, no image.
series("container_cpu_usage_seconds_total", {**full_c, "container": "", "image": ""}, T0, lambda t: 2 * (t - T0))
series("container_cpu_usage_seconds_total", {**half_c, "container": "app", "image": "app:1"}, THALF, lambda t: 4 * (t - THALF))
series("container_memory_working_set_bytes", {**full_c, "container": "app", "image": "app:1"}, T0, lambda t: 4 * GiB)
series("container_memory_working_set_bytes", {**full_c, "container": "", "image": ""}, T0, lambda t: 4 * GiB)
series("container_memory_working_set_bytes", {**half_c, "container": "app", "image": "app:1"}, THALF, lambda t: 4 * GiB)
series("kubelet_volume_stats_used_bytes", {**KUBELET, "namespace": "waldur-full", "persistentvolumeclaim": "data", "node": "n1"}, T0, lambda t: 10 * GiB)
series("kube_pod_container_resource_requests", {**KSM, "namespace": "waldur-half", "pod": "half-0", "container": "app", "resource": "nvidia_com_gpu", "unit": "integer"}, THALF, lambda t: 1)
phases("waldur-half", "half-0", THALF, TEND, running_until=TEND + 1)

# --- Duplicate /metrics/resource source (same container, no image label) ------------
series("container_cpu_usage_seconds_total", {**RESOURCE, "namespace": "waldur-full", "pod": "full-0", "container": "app"}, T0, lambda t: 2 * (t - T0))
series("container_memory_working_set_bytes", {**RESOURCE, "namespace": "waldur-full", "pod": "full-0", "container": "app"}, T0, lambda t: 4 * GiB)

# --- Negative series a tenant publishes under its own job ---------------------------
series("container_memory_working_set_bytes", {**TENANT, "namespace": "waldur-full", "pod": "full-0", "container": "app", "image": "app:1"}, T0, lambda t: -4 * GiB)
series("kube_pod_container_resource_requests", {**TENANT, "namespace": "waldur-half", "pod": "half-0", "container": "app", "resource": "nvidia_com_gpu", "unit": "integer"}, THALF, lambda t: -1)

# --- waldur-spoof: a negative series that carries the pinned labels ------------------
spoof_c = {**CADVISOR, "namespace": "waldur-spoof"}
series("container_memory_working_set_bytes", {**spoof_c, "pod": "spoof-0", "container": "app", "image": "app:1"}, T0, lambda t: 2 * GiB)
series("container_memory_working_set_bytes", {**spoof_c, "pod": "spoof-x", "container": "evil", "image": "evil:1"}, T0, lambda t: -2 * GiB)

# --- waldur-rwx: one RWX PVC mounted on two nodes ------------------------------------
for node in ("n1", "n2"):
    series("kubelet_volume_stats_used_bytes", {**KUBELET, "namespace": "waldur-rwx", "persistentvolumeclaim": "shared", "node": node}, T0, lambda t: 5 * GiB)
series("kube_persistentvolumeclaim_resource_requests_storage_bytes", {**KSM, "namespace": "waldur-rwx", "persistentvolumeclaim": "shared"}, T0, lambda t: 20 * GiB)

# --- waldur-gpu: init containers, finished and pending pods, a duplicated KSM --------
gpu = {**KSM, "namespace": "waldur-gpu", "resource": "nvidia_com_gpu", "unit": "integer"}
# train-0: init container asks 4, the two containers 1 each -> the pod holds 4.
series("kube_pod_init_container_resource_requests", {**gpu, "pod": "train-0", "container": "prep"}, T0, lambda t: 4)
series("kube_pod_container_resource_requests", {**gpu, "pod": "train-0", "container": "main"}, T0, lambda t: 1)
series("kube_pod_container_resource_requests", {**gpu, "pod": "train-0", "container": "main", "instance": "ksm-1"}, T0, lambda t: 1)
series("kube_pod_container_resource_requests", {**gpu, "pod": "train-0", "container": "side"}, T0, lambda t: 1)
phases("waldur-gpu", "train-0", T0, TEND, running_until=TEND + 1)
# done-0: 2 GPUs, Running until Oct 11 00:00, then Succeeded with the request still there.
series("kube_pod_container_resource_requests", {**gpu, "pod": "done-0", "container": "main"}, T0, lambda t: 2)
phases("waldur-gpu", "done-0", T0, TEND, running_until=TDONE)
# wait-0: 1 GPU, Pending all along.
series("kube_pod_container_resource_requests", {**gpu, "pod": "wait-0", "container": "main"}, T0, lambda t: 1)
phases("waldur-gpu", "wait-0", T0, TEND, running_until=T0, phase_after="Pending")


# --- Sub-step jobs --------------------------------------------------------------------
def job_pods(namespace, first_minute, run_minutes, linger_minutes, memory):
    """One pod per hour, running ``run_minutes`` from ``first_minute`` past the hour.

    The container's memory series exists while it runs; the pod and its GPU request
    linger ``linger_minutes`` longer as Succeeded, as a finished Job's pod does.
    """
    hour = T0
    while hour <= TEND:
        start = hour + first_minute * 60
        stop = start + run_minutes * 60  # first instant the container is gone
        gone = stop + linger_minutes * 60
        if start >= T0 and gone <= TEND:
            pod = f"{namespace.split('-')[1]}-{(hour - T0) // 3600}"
            series("container_memory_working_set_bytes", {**CADVISOR, "namespace": namespace, "pod": pod, "container": "job", "image": "job:1"}, start, lambda t: memory, stop - STEP)
            series("kube_pod_container_resource_requests", {**KSM, "namespace": namespace, "pod": pod, "container": "job", "resource": "nvidia_com_gpu", "unit": "integer"}, start, lambda t: 1, gone - STEP)
            phases(namespace, pod, start, gone - STEP, running_until=stop)
        hour += 3600


job_pods("waldur-burst", first_minute=10, run_minutes=30, linger_minutes=10, memory=2 * GiB)
job_pods("waldur-straddle", first_minute=55, run_minutes=10, linger_minutes=5, memory=1 * GiB)

with open(sys.argv[1], "w") as out:
    for family, lines in families.items():
        out.write(f"# TYPE {family} {TYPES[family]}\n")
        out.writelines(lines)
    out.write("# EOF\n")
print({family: len(lines) for family, lines in families.items()})
