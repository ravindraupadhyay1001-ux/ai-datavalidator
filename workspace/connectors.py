"""
Data source connectors.

Every connector exposes:
  fetch()            -> pandas.DataFrame
  test_connection()  -> bool

Connector.from_type(source_type, config) returns the right connector instance.
Driver libraries are imported lazily inside each method so the app still runs
when an optional driver isn't installed — calling a connector whose driver is
missing raises a clear error only at fetch time.

All file-bearing connectors parse raw bytes through _parse_bytes(), which
handles Parquet, Avro, ORC, Excel, JSON/NDJSON, XML, HTML table, TSV and CSV,
detecting the format from magic bytes when the extension is unknown.
"""

import io
import json
import re as _re
from abc import ABC, abstractmethod
from datetime import date as _date, timedelta as _timedelta

import pandas as pd


# ==========================================================================
# Dynamic date-token substitution
# ==========================================================================
# Lets a connection config reference "today's file" without hardcoding a date
# -- e.g. an SFTP remote_path of "/feeds/trades_{YYYYMMDD}.csv" resolves to
# today's date on every fetch, so a daily EOD job never needs manual editing.
# Applied once to every string config value in BaseConnector.__init__, so
# every connector type supports it automatically with no per-class code.
_DATE_TOKEN_RE = _re.compile(
    r"\{(YYYYMMDD|YYYY|D)([+-]\d+)?\}|\{strftime:([^{}]+?)([+-]\d+)?\}"
)


def _resolve_date_tokens(value: str) -> str:
    if not isinstance(value, str) or "{" not in value:
        return value

    def _sub(m: "_re.Match") -> str:
        base, offset, fmt, fmt_offset = m.groups()
        if fmt is not None:
            off = int(fmt_offset) if fmt_offset else 0
            return (_date.today() + _timedelta(days=off)).strftime(fmt)
        off = int(offset) if offset else 0
        target = _date.today() + _timedelta(days=off)
        if base in ("YYYYMMDD", "D"):
            return target.strftime("%Y%m%d")
        if base == "YYYY":
            return target.strftime("%Y")
        return m.group(0)

    return _DATE_TOKEN_RE.sub(_sub, value)


def _resolve_config_date_tokens(config: dict) -> dict:
    return {k: (_resolve_date_tokens(v) if isinstance(v, str) else v)
            for k, v in (config or {}).items()}


# ==========================================================================
# Shared byte parser
# ==========================================================================
def _parse_bytes(raw: bytes, hint: str = "", delimiter: str = None) -> pd.DataFrame:
    hint = (hint or "").lower()

    # magic-byte detection
    if raw[:4] == b"PAR1":
        return pd.read_parquet(io.BytesIO(raw)).astype(str)
    if raw[:3] == b"Obj":  # Avro
        import fastavro
        records = list(fastavro.reader(io.BytesIO(raw)))
        return pd.json_normalize(records).astype(str)
    if raw[:2] == b"PK":  # zip-container -> xlsx
        return pd.read_excel(io.BytesIO(raw), dtype=str)
    if raw[:4] == b"ORC\x00" or hint.endswith(".orc"):
        import pyarrow.orc as orc
        return orc.ORCFile(io.BytesIO(raw)).read().to_pandas().astype(str)

    if hint.endswith((".xlsx", ".xls")):
        return pd.read_excel(io.BytesIO(raw), dtype=str)
    if hint.endswith(".parquet"):
        return pd.read_parquet(io.BytesIO(raw)).astype(str)

    text = raw.decode("utf-8", errors="replace").lstrip()
    if hint.endswith(".json") or text[:1] in "[{":
        try:
            data = json.loads(text)
            if isinstance(data, list):
                return pd.json_normalize(data).astype(str)
            if isinstance(data, dict):
                for v in data.values():
                    if isinstance(v, list) and v and isinstance(v[0], dict):
                        return pd.json_normalize(v).astype(str)
                return pd.json_normalize([data]).astype(str)
        except Exception:
            # NDJSON
            try:
                rows = [json.loads(ln) for ln in text.splitlines() if ln.strip()]
                if rows:
                    return pd.json_normalize(rows).astype(str)
            except Exception:
                pass
    if hint.endswith((".xml",)) or text[:1] == "<":
        if "<table" in text.lower():
            return pd.read_html(io.StringIO(text))[0].astype(str)
        try:
            return pd.read_xml(io.StringIO(text)).astype(str)
        except Exception:
            pass

    sep = delimiter
    if sep is None:
        import csv
        try:
            sep = csv.Sniffer().sniff("\n".join(text.splitlines()[:50]),
                                      delimiters=",\t|;").delimiter
        except Exception:
            sep = "\t" if hint.endswith(".tsv") else ","
    return pd.read_csv(io.StringIO(text), sep=sep, engine="python",
                       dtype=str, keep_default_na=False)


# ==========================================================================
# Base
# ==========================================================================
class BaseConnector(ABC):
    def __init__(self, config: dict):
        self.config = _resolve_config_date_tokens(config or {})

    @abstractmethod
    def fetch(self) -> pd.DataFrame:
        ...

    @abstractmethod
    def test_connection(self) -> bool:
        ...

    @staticmethod
    def from_type(source_type: str, config: dict) -> "BaseConnector":
        st = (source_type or "").lower()
        registry = {
            "sftp": SFTPConnector, "ftp": FTPConnector, "s3": S3Connector,
            "mssql": MSSQLConnector, "postgres": PostgresConnector,
            "postgresql": PostgresConnector, "mysql": MySQLConnector,
            "mariadb": MySQLConnector, "oracle": OracleConnector,
            "snowflake": SnowflakeConnector, "api": APIConnector, "rest": APIConnector,
            "azure_blob": AzureBlobConnector, "azureblob": AzureBlobConnector,
            "gcs": GCSConnector, "kafka": KafkaConnector,
            "salesforce": SalesforceConnector, "sharepoint": SharePointConnector,
            "databricks": DatabricksConnector,
            "azure_sql": AzureSQLConnector, "azuresql": AzureSQLConnector,
            "db2": DB2Connector,
            "bloomberg": BloombergConnector, "refinitiv": RefinitivConnector,
            "murex": MurexConnector, "calypso": CalypsoConnector,
        }
        if st not in registry:
            raise ValueError(f"Unknown source type '{source_type}'.")
        return registry[st](config)


# ==========================================================================
# File-transfer connectors
# ==========================================================================
class SFTPConnector(BaseConnector):
    def _client(self):
        import paramiko
        c = self.config
        transport = paramiko.Transport((c["host"], int(c.get("port", 22))))
        pkey = None
        if c.get("private_key_path"):
            pkey = paramiko.RSAKey.from_private_key_file(c["private_key_path"])
        transport.connect(username=c.get("username"),
                          password=c.get("password") or None, pkey=pkey)
        return paramiko.SFTPClient.from_transport(transport), transport

    def fetch(self) -> pd.DataFrame:
        sftp, transport = self._client()
        try:
            with sftp.open(self.config["remote_path"], "rb") as fh:
                raw = fh.read()
        finally:
            sftp.close(); transport.close()
        return _parse_bytes(raw, self.config["remote_path"],
                            self.config.get("delimiter"))

    def test_connection(self) -> bool:
        sftp, transport = self._client()
        try:
            sftp.listdir(".")
            return True
        finally:
            sftp.close(); transport.close()


class FTPConnector(BaseConnector):
    def _client(self):
        from ftplib import FTP
        c = self.config
        ftp = FTP()
        ftp.connect(c["host"], int(c.get("port", 21)))
        ftp.login(c.get("username", "anonymous"), c.get("password", ""))
        return ftp

    def fetch(self) -> pd.DataFrame:
        ftp = self._client()
        buf = io.BytesIO()
        try:
            ftp.retrbinary(f"RETR {self.config['remote_path']}", buf.write)
        finally:
            ftp.quit()
        return _parse_bytes(buf.getvalue(), self.config["remote_path"],
                            self.config.get("delimiter"))

    def test_connection(self) -> bool:
        ftp = self._client()
        try:
            ftp.nlst()
            return True
        finally:
            ftp.quit()


class S3Connector(BaseConnector):
    def _client(self):
        import boto3
        c = self.config
        kw = {"region_name": c.get("region", "us-east-1")}
        if c.get("aws_access_key_id"):
            kw["aws_access_key_id"] = c["aws_access_key_id"]
            kw["aws_secret_access_key"] = c.get("aws_secret_access_key")
        return boto3.client("s3", **kw)

    def fetch(self) -> pd.DataFrame:
        s3 = self._client()
        obj = s3.get_object(Bucket=self.config["bucket"], Key=self.config["key"])
        raw = obj["Body"].read()
        return _parse_bytes(raw, self.config["key"], self.config.get("delimiter"))

    def test_connection(self) -> bool:
        s3 = self._client()
        s3.head_bucket(Bucket=self.config["bucket"])
        return True


class AzureBlobConnector(BaseConnector):
    """config: connection_string (or account_url + sas_token/credential),
    container, blob."""
    def _client(self):
        from azure.storage.blob import BlobServiceClient
        c = self.config
        if c.get("connection_string"):
            return BlobServiceClient.from_connection_string(c["connection_string"])
        return BlobServiceClient(account_url=c["account_url"], credential=c.get("sas_token") or c.get("credential"))

    def fetch(self) -> pd.DataFrame:
        c = self.config
        blob = self._client().get_blob_client(container=c["container"], blob=c["blob"])
        raw = blob.download_blob().readall()
        return _parse_bytes(raw, c["blob"], c.get("delimiter"))

    def test_connection(self) -> bool:
        c = self.config
        container = self._client().get_container_client(c["container"])
        container.get_container_properties()
        return True


class GCSConnector(BaseConnector):
    """config: bucket, blob, credentials_json (path or inline JSON, optional
    -- falls back to Application Default Credentials if omitted)."""
    def _client(self):
        from google.cloud import storage
        c = self.config
        if c.get("credentials_json"):
            from google.oauth2 import service_account
            import json as _json
            info = c["credentials_json"]
            if isinstance(info, str):
                info = _json.loads(info) if info.strip().startswith("{") else info
            creds = (service_account.Credentials.from_service_account_info(info)
                     if isinstance(info, dict)
                     else service_account.Credentials.from_service_account_file(info))
            return storage.Client(credentials=creds, project=c.get("project"))
        return storage.Client(project=c.get("project"))

    def fetch(self) -> pd.DataFrame:
        c = self.config
        bucket = self._client().bucket(c["bucket"])
        raw = bucket.blob(c["blob"]).download_as_bytes()
        return _parse_bytes(raw, c["blob"], c.get("delimiter"))

    def test_connection(self) -> bool:
        c = self.config
        bucket = self._client().bucket(c["bucket"])
        return bucket.exists()


class KafkaConnector(BaseConnector):
    """config: bootstrap_servers, topic, group_id (optional), max_records
    (default 5000), consumer_timeout_ms (default 10000), plus any
    kafka-python security config (security_protocol, sasl_*, ssl_*)."""
    def _consumer(self):
        from kafka import KafkaConsumer
        c = self.config
        extra = {k: v for k, v in c.items()
                 if k not in ("bootstrap_servers", "topic", "group_id", "max_records")}
        return KafkaConsumer(
            c["topic"],
            bootstrap_servers=c["bootstrap_servers"],
            group_id=c.get("group_id"),
            auto_offset_reset="earliest",
            consumer_timeout_ms=int(c.get("consumer_timeout_ms", 10000)),
            value_deserializer=lambda v: v,
            **extra,
        )

    def fetch(self) -> pd.DataFrame:
        c = self.config
        max_records = int(c.get("max_records", 5000))
        consumer = self._consumer()
        records = []
        try:
            for msg in consumer:
                records.append(msg.value)
                if len(records) >= max_records:
                    break
        finally:
            consumer.close()
        if not records:
            return pd.DataFrame()
        joined = b"\n".join(records)
        return _parse_bytes(joined, "x.json" if joined[:1] in (b"{", b"[") else "x.csv",
                            c.get("delimiter"))

    def test_connection(self) -> bool:
        from kafka import KafkaAdminClient
        c = self.config
        admin = KafkaAdminClient(bootstrap_servers=c["bootstrap_servers"])
        try:
            topics = admin.list_topics()
            return c["topic"] in topics
        finally:
            admin.close()


class SalesforceConnector(BaseConnector):
    """config: username, password, security_token, domain (optional,
    'login' or 'test'), and either soql or object_name (+ fields list)."""
    def _client(self):
        from simple_salesforce import Salesforce
        c = self.config
        return Salesforce(username=c["username"], password=c["password"],
                          security_token=c["security_token"],
                          domain=c.get("domain", "login"))

    def _soql(self) -> str:
        c = self.config
        if c.get("soql"):
            return c["soql"]
        obj = c.get("object_name")
        if not obj:
            raise ValueError("Provide either 'soql' or 'object_name'.")
        fields = c.get("fields") or ["Id", "Name"]
        return f"SELECT {', '.join(fields)} FROM {obj}"

    def fetch(self) -> pd.DataFrame:
        sf = self._client()
        result = sf.query_all(self._soql())
        records = [{k: v for k, v in r.items() if k != "attributes"} for r in result["records"]]
        return pd.json_normalize(records).astype(str)

    def test_connection(self) -> bool:
        self._client().query("SELECT Id FROM Organization LIMIT 1")
        return True


class SharePointConnector(BaseConnector):
    """config: site_url, file_path (drive-relative), tenant_id, client_id,
    client_secret -- OAuth2 client-credentials flow via Microsoft Graph
    REST API (no heavyweight SDK dependency, reuses httpx)."""
    def _token(self, httpx):
        c = self.config
        url = f"https://login.microsoftonline.com/{c['tenant_id']}/oauth2/v2.0/token"
        data = {
            "client_id": c["client_id"], "client_secret": c["client_secret"],
            "scope": "https://graph.microsoft.com/.default",
            "grant_type": "client_credentials",
        }
        r = httpx.post(url, data=data, timeout=30)
        r.raise_for_status()
        return r.json()["access_token"]

    def _graph_get(self, path):
        import httpx
        token = self._token(httpx)
        c = self.config
        site = c["site_url"].split("://", 1)[-1]
        host, site_path = site.split("/", 1)
        with httpx.Client(timeout=30, headers={"Authorization": f"Bearer {token}"}) as client:
            site_resp = client.get(f"https://graph.microsoft.com/v1.0/sites/{host}:/{site_path}")
            site_resp.raise_for_status()
            site_id = site_resp.json()["id"]
            return client.get(f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:/{path}")

    def fetch(self) -> pd.DataFrame:
        c = self.config
        meta = self._graph_get(c["file_path"])
        meta.raise_for_status()
        download_url = meta.json()["@microsoft.graph.downloadUrl"]
        import httpx
        raw = httpx.get(download_url, timeout=60).content
        return _parse_bytes(raw, c["file_path"], c.get("delimiter"))

    def test_connection(self) -> bool:
        r = self._graph_get(self.config["file_path"])
        return r.status_code < 400




# ==========================================================================
# SQL connectors
# ==========================================================================
class _SQLConnector(BaseConnector):
    def _connect(self):
        raise NotImplementedError

    def _query_sql(self) -> str:
        c = self.config
        if c.get("query"):
            return c["query"]
        table = c.get("table")
        if not table:
            raise ValueError("Provide either 'query' or 'table'.")
        return f"SELECT * FROM {table}"

    def fetch(self) -> pd.DataFrame:
        conn = self._connect()
        try:
            return pd.read_sql(self._query_sql(), conn).astype(str)
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def test_connection(self) -> bool:
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
            return True
        finally:
            try:
                conn.close()
            except Exception:
                pass


class MSSQLConnector(_SQLConnector):
    _DRIVERS = [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "ODBC Driver 13 for SQL Server",
        "SQL Server Native Client 11.0",
        "SQL Server",
    ]

    def _connect(self):
        import pyodbc
        c = self.config
        last = None
        for drv in self._DRIVERS:
            try:
                if (c.get("auth_type") or "").lower() == "windows":
                    cs = (f"DRIVER={{{drv}}};SERVER={c['host']};"
                          f"DATABASE={c['database']};Trusted_Connection=yes;"
                          f"TrustServerCertificate=yes;")
                else:
                    cs = (f"DRIVER={{{drv}}};SERVER={c['host']},{c.get('port',1433)};"
                          f"DATABASE={c['database']};UID={c.get('username')};"
                          f"PWD={c.get('password')};TrustServerCertificate=yes;")
                return pyodbc.connect(cs, timeout=10)
            except Exception as e:
                last = e
        raise RuntimeError(f"Could not connect with any ODBC driver: {last}")


class PostgresConnector(_SQLConnector):
    def _connect(self):
        import psycopg2
        c = self.config
        return psycopg2.connect(
            host=c["host"], port=int(c.get("port", 5432)), dbname=c["database"],
            user=c.get("username"), password=c.get("password"),
        )


class MySQLConnector(_SQLConnector):
    def _connect(self):
        import pymysql
        c = self.config
        return pymysql.connect(
            host=c["host"], port=int(c.get("port", 3306)), database=c["database"],
            user=c.get("username"), password=c.get("password"),
        )


class OracleConnector(_SQLConnector):
    def _connect(self):
        c = self.config
        dsn_args = dict(host=c["host"], port=int(c.get("port", 1521)),
                        service_name=c.get("service_name"))
        try:
            import oracledb
            dsn = oracledb.makedsn(**dsn_args)
            return oracledb.connect(user=c.get("username"),
                                    password=c.get("password"), dsn=dsn)
        except ImportError:
            import cx_Oracle
            dsn = cx_Oracle.makedsn(**dsn_args)
            return cx_Oracle.connect(c.get("username"), c.get("password"), dsn)


class SnowflakeConnector(_SQLConnector):
    def _connect(self):
        import snowflake.connector
        c = self.config
        return snowflake.connector.connect(
            account=c["account"], user=c.get("username"), password=c.get("password"),
            warehouse=c.get("warehouse"), database=c.get("database"),
            schema=c.get("schema"),
        )


class DatabricksConnector(_SQLConnector):
    """config: server_hostname, http_path, access_token, plus 'query' or
    'table' like the other SQL connectors."""
    def _connect(self):
        from databricks import sql as databricks_sql
        c = self.config
        return databricks_sql.connect(
            server_hostname=c["server_hostname"], http_path=c["http_path"],
            access_token=c["access_token"],
        )


class AzureSQLConnector(_SQLConnector):
    """config: server ('myserver.database.windows.net'), database, username,
    password, port (default 1433), auth_type ('sql' default |
    'aad_password' | 'aad_msi' | 'aad_default'), plus 'query' or 'table'
    like the other SQL connectors. Same TDS/pyodbc protocol as
    MSSQLConnector, but Azure SQL always requires Encrypt=yes and supports
    Azure AD auth modes on top of plain SQL login."""
    _DRIVERS = MSSQLConnector._DRIVERS

    def _connect(self):
        import pyodbc
        c = self.config
        auth_type = (c.get("auth_type") or "sql").lower()
        last = None
        for drv in self._DRIVERS:
            try:
                base = (f"DRIVER={{{drv}}};SERVER=tcp:{c['server']},{c.get('port', 1433)};"
                        f"DATABASE={c['database']};Encrypt=yes;TrustServerCertificate=no;"
                        f"Connection Timeout=10;")
                if auth_type == "sql":
                    cs = base + f"UID={c.get('username')};PWD={c.get('password')};"
                elif auth_type == "aad_password":
                    cs = base + (f"Authentication=ActiveDirectoryPassword;"
                                 f"UID={c.get('username')};PWD={c.get('password')};")
                elif auth_type == "aad_msi":
                    cs = base + "Authentication=ActiveDirectoryMsi;"
                else:
                    cs = base + "Authentication=ActiveDirectoryDefault;"
                return pyodbc.connect(cs, timeout=10)
            except Exception as e:
                last = e
        raise RuntimeError(f"Could not connect with any ODBC driver: {last}")


class DB2Connector(_SQLConnector):
    """config: host, port (default 50000), database, username, password,
    plus 'query' or 'table' like the other SQL connectors. Uses IBM's
    ibm_db_dbi DB-API wrapper (pip install ibm_db) for DB2 for LUW/i/z-OS."""
    def _connect(self):
        import ibm_db
        import ibm_db_dbi
        c = self.config
        conn_str = (f"DATABASE={c['database']};HOSTNAME={c['host']};"
                    f"PORT={c.get('port', 50000)};PROTOCOL=TCPIP;"
                    f"UID={c.get('username')};PWD={c.get('password')};")
        raw = ibm_db.connect(conn_str, "", "")
        return ibm_db_dbi.Connection(raw)


# ==========================================================================
# Market-data / trading-system connectors
#
# Bloomberg and Refinitiv follow their respective vendor SDK/API contracts
# exactly (blpapi's Session/Request model; Refinitiv Data Platform's OAuth2
# REST API). Murex and Calypso have no single universal API across bank
# deployments -- most integrations go through either a REST connectivity
# layer (assumed here) or the platform's own reporting database (in which
# case, use the 'mssql'/'oracle'/'postgres' connector type against that
# database directly instead of these). None of the four are verifiable in
# this environment without a live licensed connection.
# ==========================================================================
class BloombergConnector(BaseConnector):
    """config: host (default 'localhost'), port (default 8194 -- Bloomberg
    Desktop API via a running Bloomberg Terminal, or a B-PIPE server
    endpoint), securities (list of Bloomberg tickers), fields (list of field
    mnemonics, e.g. ['PX_LAST','SECURITY_NAME']), request_type
    ('ReferenceData' default or 'HistoricalData'), start_date/end_date
    (YYYYMMDD, HistoricalData only). Requires the 'blpapi' package."""
    def _session(self):
        import blpapi
        c = self.config
        opts = blpapi.SessionOptions()
        opts.setServerHost(c.get("host", "localhost"))
        opts.setServerPort(int(c.get("port", 8194)))
        session = blpapi.Session(opts)
        if not session.start():
            raise RuntimeError("Could not start Bloomberg API session.")
        if not session.openService("//blp/refdata"):
            raise RuntimeError("Could not open //blp/refdata service.")
        return session

    def fetch(self) -> pd.DataFrame:
        import blpapi
        c = self.config
        session = self._session()
        try:
            service = session.getService("//blp/refdata")
            req_type = c.get("request_type", "ReferenceData")
            request = service.createRequest(f"{req_type}Request")
            for sec in c.get("securities", []):
                request.getElement("securities").appendValue(sec)
            for f in c.get("fields", []):
                request.getElement("fields").appendValue(f)
            if req_type == "HistoricalData":
                request.set("startDate", c.get("start_date", ""))
                request.set("endDate", c.get("end_date", ""))
            session.sendRequest(request)
            rows = []
            while True:
                event = session.nextEvent(500)
                for msg in event:
                    if msg.hasElement("securityData"):
                        sec_data = msg.getElement("securityData")
                        sec_name = sec_data.getElementAsString("security")
                        field_data = sec_data.getElement("fieldData")
                        if req_type == "HistoricalData":
                            for i in range(field_data.numValues()):
                                row = {"security": sec_name}
                                point = field_data.getValueAsElement(i)
                                for j in range(point.numElements()):
                                    el = point.getElement(j)
                                    row[str(el.name())] = str(el.getValueAsString())
                                rows.append(row)
                        else:
                            row = {"security": sec_name}
                            for j in range(field_data.numElements()):
                                el = field_data.getElement(j)
                                row[str(el.name())] = str(el.getValueAsString())
                            rows.append(row)
                if event.eventType() == blpapi.Event.RESPONSE:
                    break
            return pd.DataFrame(rows).astype(str)
        finally:
            session.stop()

    def test_connection(self) -> bool:
        session = self._session()
        session.stop()
        return True


class RefinitivConnector(BaseConnector):
    """config: client_id, client_secret (OAuth2 client-credentials grant),
    universe (list of RICs), fields (list of field names), base_url
    (default 'https://api.refinitiv.com'). Uses the Refinitiv Data Platform
    (RDP) REST API -- no Eikon/Workspace desktop app required, unlike the
    older 'eikon' package."""
    def _token(self, httpx):
        c = self.config
        url = f"{c.get('base_url', 'https://api.refinitiv.com')}/auth/oauth2/v1/token"
        data = {
            "grant_type": "client_credentials",
            "client_id": c["client_id"], "client_secret": c["client_secret"],
            "scope": c.get("scope", "trapi"),
        }
        r = httpx.post(url, data=data, timeout=30)
        r.raise_for_status()
        return r.json()["access_token"]

    def fetch(self) -> pd.DataFrame:
        import httpx
        c = self.config
        with httpx.Client(timeout=30) as client:
            token = self._token(client)
            url = f"{c.get('base_url', 'https://api.refinitiv.com')}/data/pricing/snapshots/v1/"
            r = client.get(url, headers={"Authorization": f"Bearer {token}"},
                           params={"universe": ",".join(c.get("universe", [])),
                                   "fields": ",".join(c.get("fields", []))})
            r.raise_for_status()
            payload = r.json()
            records = payload.get("data", payload) if isinstance(payload, dict) else payload
            return pd.json_normalize(records).astype(str)

    def test_connection(self) -> bool:
        import httpx
        with httpx.Client(timeout=15) as client:
            self._token(client)
        return True


class _TradingSystemRESTConnector(BaseConnector):
    """Shared fetch logic for trading/risk platforms that expose a
    resource-oriented REST API (bearer token or basic auth). config:
    base_url, resource, api_key (bearer) or username+password (basic),
    params (dict, optional)."""
    def _client_kwargs(self):
        c = self.config
        headers = {"Authorization": f"Bearer {c['api_key']}"} if c.get("api_key") else {}
        auth = (c["username"], c["password"]) if c.get("username") and not c.get("api_key") else None
        return {"headers": headers, "auth": auth}

    def fetch(self) -> pd.DataFrame:
        import httpx
        c = self.config
        with httpx.Client(timeout=30, **self._client_kwargs()) as client:
            r = client.get(f"{c['base_url'].rstrip('/')}/{c['resource'].lstrip('/')}",
                           params=c.get("params") or {})
            r.raise_for_status()
            payload = r.json()
            records = payload.get("data", payload) if isinstance(payload, dict) else payload
            return pd.json_normalize(records).astype(str)

    def test_connection(self) -> bool:
        import httpx
        c = self.config
        with httpx.Client(timeout=15, **self._client_kwargs()) as client:
            r = client.get(f"{c['base_url'].rstrip('/')}/{c.get('resource', '').lstrip('/')}",
                           params=c.get("params") or {})
            return r.status_code < 400


class MurexConnector(_TradingSystemRESTConnector):
    """config: base_url (Murex MX.3 REST Connectivity endpoint), resource
    (e.g. 'trades', 'positions'), api_key or username+password, params
    (dict, optional query filters). Murex MX.3 has no single universal API
    across deployments -- most banks expose it either via the MX.3 REST
    Connectivity layer (assumed here) or a Datamart reporting database (use
    the 'mssql'/'oracle' connector against the Datamart's tables instead, if
    that's how your deployment is configured)."""


class CalypsoConnector(_TradingSystemRESTConnector):
    """config: base_url (Calypso REST API endpoint), resource (e.g.
    'trades', 'positions', 'cashflows'), api_key or username+password,
    params (dict, optional query filters). Follows Calypso's documented
    REST Web Services resource model."""


# ==========================================================================
# REST API connector
# ==========================================================================
class APIConnector(BaseConnector):
    def fetch(self) -> pd.DataFrame:
        import httpx
        c = self.config
        method = (c.get("method") or "GET").upper()
        all_records = []
        url = c["url"]
        params = dict(c.get("params") or {})
        max_pages = int(c.get("max_pages", 50))
        page_key = c.get("pagination_key")
        data_path = c.get("data_path")

        with httpx.Client(timeout=30, follow_redirects=True) as client:
            for _ in range(max_pages):
                r = client.request(method, url, headers=c.get("headers") or {},
                                   params=params, json=c.get("body"))
                r.raise_for_status()
                fmt = self._detect_format(c, r)
                if fmt == "json":
                    payload = r.json()
                    records = self._extract(payload, data_path)
                    all_records.extend(records)
                    # cursor pagination
                    if page_key and isinstance(payload, dict) and payload.get(page_key):
                        params[page_key] = payload[page_key]
                        continue
                    break
                else:
                    return _parse_bytes(r.content, fmt_hint(fmt), c.get("delimiter"))
        return pd.json_normalize(all_records).astype(str)

    @staticmethod
    def _detect_format(c, r):
        if c.get("response_format") and c["response_format"] != "auto":
            return c["response_format"]
        ct = r.headers.get("content-type", "").lower()
        if "json" in ct:
            return "json"
        if "csv" in ct:
            return "csv"
        if "xml" in ct:
            return "xml"
        # url extension
        for ext in (".json", ".csv", ".xml", ".parquet"):
            if c["url"].lower().split("?")[0].endswith(ext):
                return ext.lstrip(".")
        # magic bytes
        if r.content[:1] in (b"{", b"["):
            return "json"
        return "csv"

    @staticmethod
    def _extract(payload, data_path):
        if data_path:
            node = payload
            for part in data_path.split("."):
                node = node.get(part, {}) if isinstance(node, dict) else node
            payload = node
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            for v in payload.values():
                if isinstance(v, list):
                    return v
            return [payload]
        return [{"value": payload}]

    def test_connection(self) -> bool:
        import httpx
        c = self.config
        with httpx.Client(timeout=15, follow_redirects=True) as client:
            r = client.request((c.get("method") or "GET").upper(), c["url"],
                               headers=c.get("headers") or {},
                               params=c.get("params") or {})
            return r.status_code < 400


def fmt_hint(fmt: str) -> str:
    return {"csv": "x.csv", "xml": "x.xml", "parquet": "x.parquet",
            "json": "x.json"}.get(fmt, "x.csv")
