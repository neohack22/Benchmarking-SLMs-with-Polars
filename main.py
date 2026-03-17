from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import requests
import subprocess
import shlex
import socket
import time

from helpers import get_code, execute_code
from questions import build_benchmark_inputs
from providers import RunProviderRequest, call_provider_api

app = FastAPI()


class RunRepoRequest(BaseModel):
    repo_url: str


class GeneratedAnswer(BaseModel):
    id: str
    question: str
    code: str
    generation_duration_seconds: float
    peak_ram_mb: float
    peak_gpu_mb: float


class ExecutedAnswer(BaseModel):
    id: str
    stdout: str
    stderr: str
    success: bool
    execution_duration_seconds: float
    exact_match: bool | None = None


class RunRepoResponse(BaseModel):
    generator_container_id: str
    executor_container_id: str
    url: str
    generated_answers: list[GeneratedAnswer]
    executed_answers: list[ExecutedAnswer]
    generator_logs: str
    executor_logs: str


class RunProviderResponse(BaseModel):
    provider: str
    model: str
    total_generation_duration_seconds: float
    total_execution_duration_seconds: float
    generated_answers: list[GeneratedAnswer]
    executed_answers: list[ExecutedAnswer]


def get_free_port() -> int:
    s = socket.socket()
    s.bind(("", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def wait_until_up(url: str, timeout: float = 3600.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(url, timeout=1)
            if r.status_code < 500:
                return
        except requests.RequestException:
            pass
        time.sleep(1)
    raise HTTPException(status_code=500, detail="Container server did not start in time")


def run_executor_container() -> str:
    result = subprocess.run(
        ["docker", "run", "-d", "polars-executor:latest"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise HTTPException(
            status_code=500,
            detail=result.stderr.strip() or result.stdout.strip(),
        )
    return result.stdout.strip()


questions = build_benchmark_inputs()


def get_expected_output(question_id: str):
    return next((q.get("expected_output") for q in questions if q["id"] == question_id), None)


@app.post("/run-repo", response_model=RunRepoResponse)
def run_repo(payload: RunRepoRequest) -> RunRepoResponse:
    generator_container_id = None
    executor_container_id = None
    port = get_free_port()
    repo_url = shlex.quote(payload.repo_url)
    base_url = f"http://127.0.0.1:{port}"

    cmd = [
        "docker", "run", "-d",
        "--gpus", "all",
        "-p", f"{port}:8000",
        "gpu-fastapi-base:cu121",
        "sh", "-lc",
        (
            f"git clone {repo_url} /app && "
            "cd /app && "
            "uv pip install --system -r requirements.txt && "
            "uvicorn main:app --host 0.0.0.0 --port 8000"
        ),
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise HTTPException(
                status_code=500,
                detail=result.stderr.strip() or result.stdout.strip(),
            )

        generator_container_id = result.stdout.strip()
        executor_container_id = run_executor_container()

        wait_until_up(f"{base_url}/docs")

        generated_answers = []
        for question in questions:
            generation = get_code(
                container_id=generator_container_id,
                base_url=base_url,
                message=question["question"],
                schema=question["datasets"],
            )
            generated_answers.append(
                GeneratedAnswer(
                    id=question["id"],
                    question=question["question"],
                    code=generation.response_text,
                    generation_duration_seconds=generation.duration_seconds,
                    peak_ram_mb=generation.peak_ram_mb,
                    peak_gpu_mb=generation.peak_gpu_mb,
                )
            )

        executed_answers = []
        for item in generated_answers:
            execution = execute_code(
                container_id=executor_container_id,
                code=item.code,
            )

            expected_output = get_expected_output(item.id)
            exact_match = None
            if expected_output is not None:
                exact_match = execution.stdout.strip() == str(expected_output).strip()

            executed_answers.append(
                ExecutedAnswer(
                    id=item.id,
                    stdout=execution.stdout,
                    stderr=execution.stderr,
                    success=execution.success,
                    execution_duration_seconds=execution.duration_seconds,
                    exact_match=exact_match,
                )
            )

        generator_logs_result = subprocess.run(
            ["docker", "logs", generator_container_id],
            capture_output=True,
            text=True,
        )
        generator_logs = (generator_logs_result.stdout + "\n" + generator_logs_result.stderr).strip()

        executor_logs_result = subprocess.run(
            ["docker", "logs", executor_container_id],
            capture_output=True,
            text=True,
        )
        executor_logs = (executor_logs_result.stdout + "\n" + executor_logs_result.stderr).strip()

        return RunRepoResponse(
            generator_container_id=generator_container_id,
            executor_container_id=executor_container_id,
            url=base_url,
            generated_answers=generated_answers,
            executed_answers=executed_answers,
            generator_logs=generator_logs,
            executor_logs=executor_logs,
        )

    except requests.RequestException:
        raise HTTPException(status_code=500, detail="Request to generator container failed")

    finally:
        if generator_container_id:
            generator_logs_result = subprocess.run(
                ["docker", "logs", generator_container_id],
                capture_output=True,
                text=True,
            )
            generator_logs = (generator_logs_result.stdout + "\n" + generator_logs_result.stderr).strip()

            print(generator_logs)
            subprocess.run(
                ["docker", "rm", "-f", generator_container_id],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        if executor_container_id:
            executor_logs_result = subprocess.run(
                ["docker", "logs", executor_container_id],
                capture_output=True,
                text=True,
            )
            executor_logs = (executor_logs_result.stdout + "\n" + executor_logs_result.stderr).strip()
            
            print(executor_logs)
            subprocess.run(
                ["docker", "rm", "-f", executor_container_id],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )


@app.post("/run-provider-experiment", response_model=RunProviderResponse)
def run_provider_experiment(payload: RunProviderRequest) -> RunProviderResponse:
    executor_container_id = None

    try:
        executor_container_id = run_executor_container()

        generated_answers = []
        total_generation_duration_seconds = 0.0

        for question in questions:
            generation = call_provider_api(
                provider=payload.config.provider,
                model=payload.config.model_name,
                api_key=payload.config.api_key,
                prompt=question["question"],
                schema=question["datasets"],
                temp=payload.config.temperature,
                max_tokens=payload.config.max_tokens,
                extra_system_prompt=payload.config.system_prompt,
            )

            total_generation_duration_seconds += generation.duration_seconds

            generated_answers.append(
                GeneratedAnswer(
                    id=question["id"],
                    question=question["question"],
                    code=generation.response_text,
                    generation_duration_seconds=generation.duration_seconds,
                    peak_ram_mb=generation.peak_ram_mb,
                    peak_gpu_mb=generation.peak_gpu_mb,
                )
            )

        executed_answers = []
        total_execution_duration_seconds = 0.0

        for item in generated_answers:
            execution = execute_code(executor_container_id, item.code)
            total_execution_duration_seconds += execution.duration_seconds

            expected_output = get_expected_output(item.id)
            exact_match = None
            if expected_output is not None:
                exact_match = execution.stdout.strip() == str(expected_output).strip()

            executed_answers.append(
                ExecutedAnswer(
                    id=item.id,
                    stdout=execution.stdout,
                    stderr=execution.stderr,
                    success=execution.success,
                    execution_duration_seconds=execution.duration_seconds,
                    exact_match=exact_match,
                )
            )

        return RunProviderResponse(
            provider=payload.config.provider.value,
            model=payload.config.model_name,
            total_generation_duration_seconds=total_generation_duration_seconds,
            total_execution_duration_seconds=total_execution_duration_seconds,
            generated_answers=generated_answers,
            executed_answers=executed_answers,
        )

    except requests.HTTPError as exc:
        detail = exc.response.text if exc.response is not None else str(exc)
        raise HTTPException(status_code=502, detail=detail)

    except requests.RequestException as exc:
        raise HTTPException(status_code=502, detail=str(exc))

    finally:
        if executor_container_id:
            subprocess.run(
                ["docker", "rm", "-f", executor_container_id],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )