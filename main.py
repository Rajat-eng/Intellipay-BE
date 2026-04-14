import os
from urllib.parse import urlparse

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
import redshift_connector

load_dotenv()

app = FastAPI(title="Intellipay API", version="1.0.0")


def parse_redshift_host(raw_host: str, default_port: int) -> tuple[str, int]:
    host_value = raw_host.strip()
    if not host_value:
        raise HTTPException(status_code=500, detail="REDSHIFT_HOST cannot be empty")

    if "://" not in host_value:
        return host_value, default_port

    normalized_url = host_value
    if normalized_url.startswith("jdbc:"):
        normalized_url = normalized_url.removeprefix("jdbc:")

    parsed = urlparse(normalized_url)
    if parsed.scheme != "redshift" or not parsed.hostname:
        raise HTTPException(
            status_code=500,
            detail="REDSHIFT_HOST must be a hostname or a jdbc:redshift:// URL",
        )

    return parsed.hostname, parsed.port or default_port


def get_redshift_config() -> dict[str, str | int | bool]:
    required_env_vars = [
        "REDSHIFT_HOST",
        "REDSHIFT_DB",
        "REDSHIFT_USER",
        "REDSHIFT_PASSWORD",
    ]
    missing = [name for name in required_env_vars if not os.getenv(name)]
    if missing:
        missing_csv = ", ".join(missing)
        raise HTTPException(status_code=500, detail=f"Missing environment variables: {missing_csv}")

    default_port = int(os.getenv("REDSHIFT_PORT", "5439"))
    host, port = parse_redshift_host(os.environ["REDSHIFT_HOST"], default_port)
    sslmode = os.getenv("REDSHIFT_SSLMODE", "require").lower()
    ssl_enabled = os.getenv("REDSHIFT_SSLMODE", "require").lower() != "disable"
    timeout = int(os.getenv("REDSHIFT_CONNECT_TIMEOUT", "10"))

    if sslmode not in {"require", "disable"}:
        raise HTTPException(status_code=500, detail="REDSHIFT_SSLMODE must be either require or disable")

    return {
        "host": host,
        "port": port,
        "database": os.environ["REDSHIFT_DB"],
        "user": os.environ["REDSHIFT_USER"],
        "password": os.environ["REDSHIFT_PASSWORD"],
        "ssl": ssl_enabled,
        "timeout": timeout,
    }


def open_redshift_connection() -> redshift_connector.Connection:
    cfg = get_redshift_config()
    return redshift_connector.connect(
        host=cfg["host"],
        database=cfg["database"],
        port=cfg["port"],
        user=cfg["user"],
        password=cfg["password"],
        ssl=cfg["ssl"],
        timeout=cfg["timeout"],
    )


@app.get("/")
def read_root() -> dict[str, str]:
    return {"message": "FastAPI server is running"}


@app.get("/health")
def health_check() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/payments")
def get_payments() -> dict[str, object]:
    conn: redshift_connector.Connection | None = None
    cursor: redshift_connector.Cursor | None = None
    try:
        conn = open_redshift_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT *
            FROM payment100m
            LIMIT 100;
        """)

        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()
        data = [dict(zip(columns, row)) for row in rows]

        return {
            "row_count": len(data),
            "data": data,
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Redshift query failed: {exc}") from exc
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()
            
