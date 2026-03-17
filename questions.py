POLARS_BENCHMARK_QUESTIONS = [
    {
        "id": "q01",
        "datasets": ["customers"],
        "question": "Count premium customers by country from customers.",
    },
    {
        "id": "q02",
        "datasets": ["orders"],
        "question": "Compute total revenue and average discount_amount for completed orders grouped by channel from orders.",
    },
    {
        "id": "q03",
        "datasets": ["products"],
        "question": "Find the top 10 brands by average base_price among active products from products.",
    },
    {
        "id": "q04",
        "datasets": ["events"],
        "question": "Count events grouped by event_type and device from events.",
    },
    {
        "id": "q05",
        "datasets": ["order_items"],
        "question": "Compute total quantity and gross sales per product_id from order_items where gross_sales = quantity * unit_price.",
    },
    {
        "id": "q06",
        "datasets": ["customers", "orders"],
        "question": "For each customer segment, compute completed order count, total revenue, and average order amount using customers and orders.",
    },
    {
        "id": "q07",
        "datasets": ["order_items", "products"],
        "question": "Compute net sales by category using order_items and products where net_sales = quantity * unit_price - item_discount.",
    },
    {
        "id": "q08",
        "datasets": ["customers", "events"],
        "question": "Compute average number of sessions per customer grouped by is_premium using customers and events.",
    },
    {
        "id": "q09",
        "datasets": ["orders"],
        "question": "Compute daily revenue for completed orders and a 7-day rolling average from orders.",
    },
    {
        "id": "q10",
        "datasets": ["customers", "orders"],
        "question": "Find top 20 customers by lifetime_value with at least 5 completed orders using customers and orders.",
    },
    {
        "id": "q11",
        "datasets": ["products", "order_items"],
        "question": "Compute brand share of quantity sold within each category using products and order_items.",
    },
    {
        "id": "q12",
        "datasets": ["events"],
        "question": "Compute time difference in seconds between consecutive events per customer from events.",
    },
    {
        "id": "q13",
        "datasets": ["customers", "orders", "events"],
        "question": "Compute conversion rate per country as customers with completed orders divided by customers with events using customers, orders, and events.",
    },
    {
        "id": "q14",
        "datasets": ["products", "order_items"],
        "question": "Find top 3 products by net sales within each category using products and order_items.",
    },
    {
        "id": "q15",
        "datasets": ["customers", "orders"],
        "question": "Compute median order amount and 90th percentile payment_delay_days per segment and month using customers and orders.",
    },
]


DATASET_SCHEMAS = {
    "customers": {
        "file_name": "data/customers.csv",
        "format": "csv",
        "schema": {
            "customer_id": "int64",
            "country": "string",
            "city": "string",
            "signup_date": "date",
            "segment": "string",
            "age": "int32",
            "is_premium": "bool",
            "churned": "bool",
            "lifetime_value": "float64",
        },
    },
    "orders": {
        "file_name": "data/orders.csv",
        "format": "csv",
        "schema": {
            "order_id": "int64",
            "customer_id": "int64",
            "order_date": "datetime",
            "status": "string",
            "channel": "string",
            "currency": "string",
            "amount": "float64",
            "discount_amount": "float64",
            "payment_delay_days": "int32",
        },
    },
    "order_items": {
        "file_name": "data/order_items.csv",
        "format": "csv",
        "schema": {
            "order_item_id": "int64",
            "order_id": "int64",
            "product_id": "int64",
            "quantity": "int32",
            "unit_price": "float64",
            "item_discount": "float64",
            "warehouse_id": "int32",
        },
    },
    "products": {
        "file_name": "data/products.csv",
        "format": "csv",
        "schema": {
            "product_id": "int64",
            "category": "string",
            "sub_category": "string",
            "brand": "string",
            "launch_date": "date",
            "is_active": "bool",
            "base_price": "float64",
            "weight_grams": "float64",
        },
    },
    "events": {
        "file_name": "data/events.csv",
        "format": "csv",
        "schema": {
            "event_id": "int64",
            "customer_id": "int64",
            "session_id": "string",
            "event_time": "datetime",
            "event_type": "string",
            "device": "string",
            "source": "string",
            "page": "string",
            "duration_sec": "float64",
        },
    },
}


def build_benchmark_inputs():
    items = []
    for q in POLARS_BENCHMARK_QUESTIONS:
        items.append({
            "id": q["id"],
            "question": q["question"],
            "datasets": {
                name: DATASET_SCHEMAS[name]
                for name in q["datasets"]
            },
        })
    return items