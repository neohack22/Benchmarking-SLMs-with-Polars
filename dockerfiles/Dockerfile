FROM nvidia/cuda:12.1.1-runtime-ubuntu22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV PATH="/root/.local/bin:${PATH}"

RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-venv \
    curl \
    git \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN curl -LsSf https://astral.sh/uv/install.sh | sh

RUN uv pip install --system --index-url https://download.pytorch.org/whl/cu121 \
    torch \
    torchvision \
    torchaudio

RUN uv pip install --system \
    fastapi \
    uvicorn \
    transformers \
    accelerate \
    pydantic \
    requests
    
RUN python3 -c "import torch; print(torch.__version__)"

WORKDIR /app