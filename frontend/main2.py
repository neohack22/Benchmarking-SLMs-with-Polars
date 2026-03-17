from __future__ import annotations

import json
import time
import uuid
from pathlib import Path

import requests
import streamlit as st

BACKEND_URL = "http://localhost:8000"
HISTORY_PATH = Path("benchmark_history.json")
PROVIDER_MODELS = {
    "openai": ["gpt-5.4", "gpt-5-mini", "gpt-4.1"],
    "groq": ["openai/gpt-oss-120b", "openai/gpt-oss-20b", "llama-3.3-70b-versatile", "llama-3.1-8b-instant"],
    "cerebras": ["gpt-oss-120b", "llama3.1-8b", "zai-glm-4.7"],
}
HUGGINGFACE_DEFAULTS = {
    "repo_id": "Qwen/Qwen2.5-Coder-7B-Instruct",
    "temperature": 0.1,
    "max_new_tokens": 1024,
    "top_p": 0.95,
}


def load_history() -> list[dict]:
    if not HISTORY_PATH.exists():
        return []
    return json.loads(HISTORY_PATH.read_text(encoding="utf-8"))


def save_run(run: dict) -> None:
    history = load_history()
    history.insert(0, run)
    HISTORY_PATH.write_text(json.dumps(history, indent=2, ensure_ascii=False), encoding="utf-8")


def post_json(path: str, payload: dict) -> dict:
    response = requests.post(f"{BACKEND_URL}{path}", json=payload, timeout=None)
    response.raise_for_status()
    return response.json()


def count_successes(executed_answers: list[dict]) -> int:
    return sum(1 for item in executed_answers if item.get("success"))


def merge_rows(generated_answers: list[dict], executed_answers: list[dict]) -> list[dict]:
    executed_by_id = {item["id"]: item for item in executed_answers}
    rows = []
    for generated in generated_answers:
        executed = executed_by_id.get(generated["id"], {})
        rows.append(
            {
                "id": generated["id"],
                "question": generated["question"],
                "code": generated["code"],
                "stdout": executed.get("stdout", ""),
                "stderr": executed.get("stderr", ""),
                "success": executed.get("success", False),
            }
        )
    return rows


def build_run_record(mode: str, label: str, config: dict, payload: dict) -> dict:
    generated_answers = payload.get("generated_answers", [])
    executed_answers = payload.get("executed_answers", [])
    total_duration_seconds = payload.get("total_duration_seconds")
    if total_duration_seconds is None:
        total_duration_seconds = payload.get("duration_seconds")
    return {
        "run_id": str(uuid.uuid4()),
        "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "mode": mode,
        "label": label,
        "config": config,
        "total_questions": len(generated_answers),
        "success_count": count_successes(executed_answers),
        "total_duration_seconds": total_duration_seconds,
        "payload": payload,
        "rows": merge_rows(generated_answers, executed_answers),
    }


def render_stat_card(title: str, value: str) -> None:
    st.markdown(
        f"""
        <div style="padding:16px;border:1px solid rgba(128,128,128,.25);border-radius:16px;background:rgba(255,255,255,.02)">
            <div style="font-size:0.9rem;opacity:.75">{title}</div>
            <div style="font-size:1.35rem;font-weight:600;margin-top:6px">{value}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_run_overview(run: dict) -> None:
    duration = run.get("total_duration_seconds")
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        render_stat_card("Type", run["mode"])
    with c2:
        render_stat_card("Succès", f"{run['success_count']} / {run['total_questions']}")
    with c3:
        render_stat_card("Durée", "-" if duration is None else f"{duration:.2f}s")
    with c4:
        render_stat_card("Créé le", run["created_at"])


def render_question_details(row: dict) -> None:
    status = "✅" if row["success"] else "❌"
    with st.expander(f"{status} {row['id']} — {row['question']}"):
        st.markdown("**Code généré**")
        st.code(row["code"], language="python")
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**stdout**")
            st.code(row["stdout"] or "", language="text")
        with col2:
            st.markdown("**stderr**")
            st.code(row["stderr"] or "", language="text")


def render_run_details(run: dict) -> None:
    st.subheader(run["label"])
    st.caption(f"{run['mode']} · {run['created_at']}")
    render_run_overview(run)

    with st.expander("Configuration", expanded=True):
        st.json(run.get("config", {}))

    generator_logs = run.get("payload", {}).get("generator_logs")
    executor_logs = run.get("payload", {}).get("executor_logs")
    if generator_logs or executor_logs:
        with st.expander("Logs"):
            if generator_logs:
                st.markdown("**Generator logs**")
                st.code(generator_logs, language="text")
            if executor_logs:
                st.markdown("**Executor logs**")
                st.code(executor_logs, language="text")

    st.subheader("Questions")
    for row in run["rows"]:
        render_question_details(row)


def render_history(history: list[dict]) -> None:
    st.title("Historique des expériences")
    if not history:
        st.info("Aucune expérience enregistrée pour le moment.")
        return

    if "selected_run_id" not in st.session_state:
        st.session_state.selected_run_id = history[0]["run_id"]

    left, right = st.columns([1, 2])

    with left:
        st.subheader("Expériences")
        for run in history:
            success = f"{run['success_count']}/{run['total_questions']}"
            label = f"{run['label']}\n{run['mode']} · {success}"
            if st.button(label, key=run["run_id"], use_container_width=True):
                st.session_state.selected_run_id = run["run_id"]

    with right:
        selected = next((run for run in history if run["run_id"] == st.session_state.selected_run_id), history[0])
        render_run_details(selected)


def run_repo_experiment(repo_url: str) -> dict:
    return post_json("/run-repo", {"repo_url": repo_url})


def run_provider_experiment(provider: str, model_name: str, api_key: str, temperature: float, max_tokens: int) -> dict:
    return post_json(
        "/run-provider-experiment",
        {
            "config": {
                "provider": provider,
                "model_name": model_name,
                "api_key": api_key,
                "temperature": temperature,
                "max_tokens": max_tokens,
            }
        },
    )


def save_completed_run(mode: str, label: str, config: dict, payload: dict) -> dict:
    run = build_run_record(mode, label, config, payload)
    save_run(run)
    st.session_state.selected_run_id = run["run_id"]
    return run


def render_repo_form() -> None:
    st.markdown("### Repository GitHub")
    with st.form("repo_form"):
        label = st.text_input("Nom de l'expérience", value="Repo experiment")
        repo_url = st.text_input("URL du repo")
        submitted = st.form_submit_button("Lancer l'expérience", use_container_width=True)
        if submitted:
            if not repo_url.strip():
                st.error("L'URL du repo est requise.")
                return
            with st.spinner("Exécution du benchmark en cours..."):
                payload = run_repo_experiment(repo_url.strip())
            run = save_completed_run("repo", label, {"repo_url": repo_url.strip()}, payload)
            st.success("Expérience terminée.")
            render_run_details(run)


def render_provider_form() -> None:
    st.markdown("### Cloud provider")
    with st.form("provider_form"):
        label = st.text_input("Nom de l'expérience", value="Provider experiment")
        provider = st.selectbox("Provider", list(PROVIDER_MODELS.keys()))
        model_name = st.selectbox("Modèle", PROVIDER_MODELS[provider])
        api_key = st.text_input("API key", type="password")
        temperature = st.slider("Temperature", 0.0, 2.0, 0.1, 0.1)
        max_tokens = st.number_input("Max tokens", min_value=1, value=1024, step=1)
        submitted = st.form_submit_button("Lancer l'expérience", use_container_width=True)
        if submitted:
            if not api_key.strip():
                st.error("La clé API est requise.")
                return
            with st.spinner("Exécution du benchmark en cours..."):
                payload = run_provider_experiment(
                    provider,
                    model_name,
                    api_key.strip(),
                    float(temperature),
                    int(max_tokens),
                )
            run = save_completed_run(
                "provider",
                label,
                {
                    "provider": provider,
                    "model_name": model_name,
                    "temperature": float(temperature),
                    "max_tokens": int(max_tokens),
                },
                payload,
            )
            st.success("Expérience terminée.")
            render_run_details(run)


def render_huggingface_form() -> None:
    st.markdown("### Hugging Face")
    st.info("L'interface est prête, mais ton backend actuel n'expose pas encore de route Hugging Face dédiée.")
    with st.form("hf_form"):
        label = st.text_input("Nom de l'expérience", value="Hugging Face experiment")
        repo_id = st.text_input("Model repo id", value=HUGGINGFACE_DEFAULTS["repo_id"])
        api_key = st.text_input("HF token", type="password")
        temperature = st.slider("Temperature", 0.0, 2.0, HUGGINGFACE_DEFAULTS["temperature"], 0.1, key="hf_temp")
        top_p = st.slider("Top p", 0.0, 1.0, HUGGINGFACE_DEFAULTS["top_p"], 0.01)
        max_new_tokens = st.number_input("Max new tokens", min_value=1, value=HUGGINGFACE_DEFAULTS["max_new_tokens"], step=1)
        submitted = st.form_submit_button("Lancer l'expérience", use_container_width=True)
        if submitted:
            st.warning("Ajoute d'abord un endpoint backend du type /run-huggingface-experiment pour rendre ce formulaire exécutable.")
            st.json(
                {
                    "label": label,
                    "repo_id": repo_id,
                    "has_api_key": bool(api_key.strip()),
                    "temperature": float(temperature),
                    "top_p": float(top_p),
                    "max_new_tokens": int(max_new_tokens),
                }
            )


def render_create_experiment() -> None:
    st.title("Créer une nouvelle expérience")
    mode = st.segmented_control(
        "Mode",
        ["Repo", "Hugging Face", "Provider"],
        default="Repo",
        key="create_mode",
    )

    if mode == "Repo":
        render_repo_form()
    elif mode == "Hugging Face":
        render_huggingface_form()
    else:
        render_provider_form()


st.set_page_config(page_title="Benchmark Dashboard", layout="wide")

with st.sidebar:
    st.title("Benchmark")
    page = st.radio("Navigation", ["Créer une nouvelle expérience", "Historique des expériences"])

history = load_history()

if page == "Créer une nouvelle expérience":
    render_create_experiment()
else:
    render_history(history)
