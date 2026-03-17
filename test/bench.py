import time
from typing import Any
from pydantic import BaseModel

import sys
from pathlib import Path

# This line allows bench.py to "see" providers.py in the parent folder
sys.path.append(str(Path(__file__).parent.parent))

from providers import call_provider_api, ProviderName
# Assuming your logic above is in a file named providers.py
# from providers import call_provider_api, ProviderName, CodeMetrics

# --- TEST CASES FROM YOUR LOGS ---
BENCHMARK_SUITE = [
    {
        "id": "q1",
        "question": "Count premium customers by country from customers.",
        "schema": {'customers': {'file_name': 'customers.parquet', 'schema': {'customer_id': 'int64', 'country': 'string', 'is_premium': 'bool'}}}
    },
    {
        "id": "q2",
        "question": "Compute total revenue and average discount_amount for completed orders grouped by channel from orders.",
        "schema": {'orders': {'file_name': 'orders.parquet', 'schema': {'order_id': 'int64', 'status': 'string', 'channel': 'string', 'amount': 'float64', 'discount_amount': 'float64'}}}
    }
]

def run_full_benchmark(provider_str, model, api_key, system_prompt_extra):
    provider = ProviderName(provider_str)
    generated_results = []
    
    print(f"🚀 Starting Benchmark: {provider_str} - {model}")
    
    for test_case in BENCHMARK_SUITE:
        print(f"  📝 Testing: {test_case['id']}...")
        try:
            metrics = call_provider_api(
                provider=provider,
                model=model,
                api_key=api_key,
                prompt=test_case['question'],
                schema=test_case['schema'],
                temp=0.1,
                max_tokens=1024,
                extra_system_prompt=system_prompt_extra
            )
            
            generated_results.append({
                "id": test_case["id"],
                "question": test_case["question"],
                "code": metrics.response_text,
                "generation_duration_seconds": metrics.duration_seconds,
                "peak_ram_mb": metrics.peak_ram_mb,
                "peak_gpu_mb": metrics.peak_gpu_mb
            })
        except Exception as e:
            print(f"  ❌ Error on {test_case['id']}: {e}")

    return {
        "generated_answers": generated_results,
        "total_generation_duration_seconds": sum(r["generation_duration_seconds"] for r in generated_results),
        "generator_logs": f"Executed {len(generated_results)} queries on {model}"
    }

# --- EXAMPLE EXECUTION ---
if __name__ == "__main__":
    # Replace these with your actual keys for the hackathon
    MY_API_KEY = "csk-v9vfrcdcyetvrp8e596p84xrx42pexvxt65rwcpffhf6fxk9"
    
    # Change to "cerebras" or "xai" as needed
    results = run_full_benchmark(
        provider_str="cerebras", 
        model="zai-glm-4.7", 
        api_key=MY_API_KEY,
        system_prompt_extra="Generate only polars code, you'll be provided a question and schemas."
    )
    
    print("\n✅ Benchmark Complete!")
    print(f"Total Time: {results['total_generation_duration_seconds']:.2f}s")
    print(results)