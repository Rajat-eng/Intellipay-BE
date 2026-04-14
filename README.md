# FastAPI Server

## Run the server

```bash
uvicorn main:app --reload
```

Then open:
- http://127.0.0.1:8000/
- http://127.0.0.1:8000/health
- http://127.0.0.1:8000/redshift/ping
- http://127.0.0.1:8000/docs

## Redshift configuration

Set these environment variables before running the server:

- REDSHIFT_HOST: either the Redshift hostname or a full `jdbc:redshift://...` URL
- REDSHIFT_PORT (optional, default: 5439)
- REDSHIFT_DB
- REDSHIFT_USER
- REDSHIFT_PASSWORD
- REDSHIFT_SSLMODE (optional, default: require)
- REDSHIFT_CONNECT_TIMEOUT (optional, default: 10)

If `REDSHIFT_HOST` is provided as a JDBC URL, the app will extract the hostname
and port automatically before opening the connection.

Use this endpoint to test the Redshift connection:

- GET /redshift/ping
