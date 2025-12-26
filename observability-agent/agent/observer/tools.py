from langchain_core.tools import tool
from datetime import datetime,timezone,UTC
import requests
import pandas as pd
import pytz
import json

# @tool
# def query_loki(query: str, start: str, end: str, limit: int = 100) -> list:
#     """Query Loki logs using LogQL and return structured JSON.

#     Args:
#         query: LogQL query string.
#         start: Start datetime (IST, format "%Y-%m-%d %H:%M:%S").
#         end: End datetime (IST, format "%Y-%m-%d %H:%M:%S").
#         limit: Max number of log lines (default 100).

#     Returns:
#         list: List of logs as dicts with keys: [timestamp, log, level].
#     """
#     import requests
#     import pytz
#     from datetime import datetime

#     try:
#         # Convert IST to UTC nanoseconds (Loki expects UTC epoch ns)
#         ist = pytz.timezone("Asia/Kolkata")
#         start_dt = ist.localize(datetime.strptime(start, "%Y-%m-%d %H:%M:%S")).astimezone(pytz.UTC)
#         end_dt = ist.localize(datetime.strptime(end, "%Y-%m-%d %H:%M:%S")).astimezone(pytz.UTC)

#         start_ns = int(start_dt.timestamp() * 1e9)
#         end_ns = int(end_dt.timestamp() * 1e9)

#         url = "http://host.docker.internal:3000/loki/api/v1/query_range"
#         # url2 = "http://localhost:3000/loki/api/v1/query_range"
#         params = {
#             "query": query,
#             "start": str(start_ns),
#             "end": str(end_ns),
#             "limit": limit,
#         }

#         response = requests.get(url, params=params)
#         response.raise_for_status()
#         data = response.json()

#         results = []
#         for stream in data.get("data", {}).get("result", []):
#             for ts, log in stream.get("values", []):
#                 # Convert Loki's ns timestamp → human readable
#                 ts_ms = int(int(ts) / 1e6)
#                 timestamp = datetime.utcfromtimestamp(ts_ms / 1000.0).strftime("%Y-%m-%d %H:%M:%S")

#                 # Auto-detect log level if present
#                 level = "INFO"
#                 if "ERROR" in log.upper():
#                     level = "ERROR"
#                 elif "WARN" in log.upper():
#                     level = "WARNING"

#                 results.append({
#                     "timestamp": timestamp,
#                     "log": log,
#                     "level": level
#                 })

#         return results if results else [{"message": "No logs found for the given query and time range."}]

#     except Exception as e:
#         return [{"error": f"Error executing Loki query: {str(e)}"}]

@tool
def query_loki(query: str, start: str, end: str, limit: int = 100) -> list:
    """Query Loki logs using LogQL and return structured JSON.
    
    Args:
        query: LogQL query string (e.g., {service_name="sales-logs"}).
        start: Start datetime (UTC, format "%Y-%m-%d %H:%M:%S").
        end: End datetime (UTC, format "%Y-%m-%d %H:%M:%S").
        limit: Max number of log lines (default 100).
    
    Returns:
        list: List of logs as dicts with keys: [timestamp, log, level].
    """
    import requests
    from datetime import datetime
    
    try:
        # Parse UTC datetime strings
        start_dt = datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
        end_dt = datetime.strptime(end, "%Y-%m-%d %H:%M:%S")
        
        # Convert to nanoseconds (Loki expects UTC epoch ns)
        start_ns = int(start_dt.timestamp() * 1e9)
        end_ns = int(end_dt.timestamp() * 1e9)
        
        url = "http://host.docker.internal:3000/loki/api/v1/query_range"
        
        params = {
            "query": query,
            "start": str(start_ns),
            "end": str(end_ns),
            "limit": limit,
        }
        
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        results = []
        for stream in data.get("data", {}).get("result", []):
            for ts, log in stream.get("values", []):
                # Convert Loki's ns timestamp → human readable
                ts_ms = int(int(ts) / 1e6)
                timestamp = datetime.utcfromtimestamp(ts_ms / 1000.0).strftime("%Y-%m-%d %H:%M:%S")
                
                # Auto-detect log level if present
                level = "INFO"
                if "ERROR" in log.upper():
                    level = "ERROR"
                elif "WARN" in log.upper():
                    level = "WARNING"
                
                results.append({
                    "timestamp": timestamp,
                    "log": log,
                    "level": level
                })
        
        return results if results else [{"message": "No logs found for the given query and time range."}]
        
    except Exception as e:
        return [{"error": f"Error executing Loki query: {str(e)}"}]


@tool
def query_prometheus(promql: str, start: str, end: str, step: str = "60s"):
    """
    Query Prometheus using query_range API and return parsed JSON.
    
    Args:
        promql (str): The PromQL query string.
        start (str): Start time in "%Y-%m-%d %H:%M:%S" IST format.
        end (str): End time in "%Y-%m-%d %H:%M:%S" IST format.
        step (str): Query step (default: 60s).
    
    Returns:
        list: Parsed results with pod name, timestamp, and value.
    """
    url = "http://host.docker.internal:9090/api/v1/query_range"
    # url2 = "http://localhost:9090/api/v1/query_range"

    params = {
        "query": promql,
        "start": int(datetime.strptime(start, "%Y-%m-%d %H:%M:%S").timestamp()),
        "end": int(datetime.strptime(end, "%Y-%m-%d %H:%M:%S").timestamp()),
        "step": step
    }

    response = requests.get(url, params=params)
    data = response.json()

    results = []
    for item in data.get("data", {}).get("result", []):
        pod_name = item["metric"].get("pod", "unknown")
        values = []
        for ts, val in item["values"]:
            ts_float = float(ts)
            timestamp = datetime.fromtimestamp(ts_float, UTC).strftime("%Y-%m-%d %H:%M:%S")
            values.append({
                "timestamp": timestamp,
                "value": float(val)
            })
        results.append({
            "metric": {"pod": pod_name},
            "values": values
        })
    return results
    
@tool
def get_current_datetime_utc() -> str:
    """Get the current date and time in UTC in format YYYY-MM-DD HH:MM:SS."""

    utc = pytz.timezone("UTC")
    now_utc = datetime.now(utc)
    return now_utc.strftime("%Y-%m-%d %H:%M:%S")
   


# x = query_loki(
#     query='{service_name="sales-logs"}',
#     start="2025-12-15 18:35:00",
#     end="2025-12-16 23:45:00",
#     limit=100
# )

# print(json.dumps(x, indent=2))

# response = query_prometheus(
#     promql='sum by (pod) (container_memory_working_set_bytes{namespace="default",pod=~"spark-sales-job-driver",container!="POD"}) / 1024 / 1024',
#     start="2025-12-13 18:50:00",
#     end="2025-12-13 21:55:00",
#     step="60s"
# )

# print(json.dumps(response, indent=2))