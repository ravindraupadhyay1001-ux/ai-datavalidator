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
from abc import ABC, abstractmethod

import pandas as pd


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
        self.config = config or {}

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
