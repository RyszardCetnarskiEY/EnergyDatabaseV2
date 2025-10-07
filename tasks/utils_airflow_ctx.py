from typing import Dict, Any

def safe_get_context() -> Dict[str, Any]:
    try:
        from airflow.operators.python import get_current_context
        return get_current_context() or {}
    except Exception:
        return {}