from pydantic import BaseModel
import requests
import subprocess
import threading
import time
import os
import tempfile
import re


class CodeMetrics(BaseModel):
    response_text: str
    duration_seconds: float
    peak_ram_mb: float
    peak_gpu_mb: float


class ExecutionMetrics(BaseModel):
    stdout: str
    stderr: str
    success: bool
    duration_seconds: float


def _parse_mem_to_mb(value: str) -> float:
    m = re.match(r"([\d.]+)\s*([A-Za-z]+)", value.strip())
    if not m:
        raise ValueError(f"Invalid format: {value}")

    num = float(m.group(1))
    unit = m.group(2)

    units = {
        "B": 1 / (1024 * 1024),
        "KiB": 1 / 1024,
        "MiB": 1,
        "GiB": 1024,
        "Gi": 1024,
        "TiB": 1024 * 1024,
    }

    if unit not in units:
        raise ValueError(f"Unsupported memory unit: {unit}")

    return num * units[unit]


def _get_container_ram_mb(container_id: str) -> float:
    result = subprocess.run(
        ["docker", "stats", "--no-stream", "--format", "{{.MemUsage}}", container_id],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return 0.0

    output = result.stdout.strip()
    if not output:
        return 0.0

    used = output.split("/")[0].strip()
    return _parse_mem_to_mb(used)


def _get_container_pids(container_id: str) -> set[int]:
    result = subprocess.run(
        ["docker", "top", container_id, "-eo", "pid"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return set()

    lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    if len(lines) <= 1:
        return set()

    pids = set()
    for line in lines[1:]:
        try:
            pids.add(int(line))
        except ValueError:
            pass
    return pids


def _get_container_gpu_mb(container_id: str) -> float:
    pids = _get_container_pids(container_id)
    if not pids:
        return 0.0

    result = subprocess.run(
        [
            "nvidia-smi",
            "--query-compute-apps=pid,used_gpu_memory",
            "--format=csv,noheader,nounits",
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return 0.0

    total = 0.0
    for line in result.stdout.splitlines():
        parts = [part.strip() for part in line.split(",")]
        if len(parts) != 2:
            continue
        try:
            pid = int(parts[0])
            used_mb = float(parts[1])
        except ValueError:
            continue
        if pid in pids:
            total += used_mb

    return total


def get_code(
    container_id: str,
    base_url: str,
    message: str,
    schema: dict,
    timeout: float = 30.0,
    sample_interval: float = 0.2,
) -> CodeMetrics:
    result: dict = {}
    error: dict = {}

    def _do_request() -> None:
        try:
            response = requests.post(
                f"{base_url}/chat",
                json={"message": message, "schema": schema},
                timeout=timeout,
            )
            result["response"] = response
        except Exception as exc:
            error["exception"] = exc

    start = time.perf_counter()
    thread = threading.Thread(target=_do_request)
    thread.start()

    peak_ram_mb = 0.0
    peak_gpu_mb = 0.0

    while thread.is_alive():
        peak_ram_mb = max(peak_ram_mb, _get_container_ram_mb(container_id))
        peak_gpu_mb = max(peak_gpu_mb, _get_container_gpu_mb(container_id))
        time.sleep(sample_interval)

    thread.join()
    duration_seconds = time.perf_counter() - start

    peak_ram_mb = max(peak_ram_mb, _get_container_ram_mb(container_id))
    peak_gpu_mb = max(peak_gpu_mb, _get_container_gpu_mb(container_id))

    if "exception" in error:
        raise error["exception"]

    response = result["response"]
    response.raise_for_status()
    data = response.json()
    code_pure = data.get("response", response.text)

    return CodeMetrics(
        response_text=code_pure,
        duration_seconds=duration_seconds,
        peak_ram_mb=peak_ram_mb,
        peak_gpu_mb=peak_gpu_mb,
    )


def execute_code(container_id: str, code: str) -> ExecutionMetrics:
    wrapped_code = (
        "import polars as pl\n"
        f"{code}\n"
        "print(result)\n"
    )

    with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False) as f:
        f.write(wrapped_code)
        host_path = f.name

    container_path = f"/tmp/{os.path.basename(host_path)}"

    try:
        copy_result = subprocess.run(
            ["docker", "cp", host_path, f"{container_id}:{container_path}"],
            capture_output=True,
            text=True,
        )
        if copy_result.returncode != 0:
            return ExecutionMetrics(
                stdout="",
                stderr=copy_result.stderr,
                success=False,
                duration_seconds=0.0,
            )

        start = time.perf_counter()
        run_result = subprocess.run(
            ["docker", "exec", container_id, "python", container_path],
            capture_output=True,
            text=True,
        )
        duration_seconds = time.perf_counter() - start

        return ExecutionMetrics(
            stdout=run_result.stdout,
            stderr=run_result.stderr,
            success=run_result.returncode == 0,
            duration_seconds=duration_seconds,
        )
    finally:
        os.remove(host_path)