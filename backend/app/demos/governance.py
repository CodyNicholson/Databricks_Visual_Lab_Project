"""Unity Catalog: the three-level namespace, managed vs external tables, volumes and functions,
through both Spark SQL and the Unity Catalog REST API (the same /api/2.1/unity-catalog API)."""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter
from pyspark.sql import SparkSession

from .. import core
from ..lab import Demo

router = APIRouter(prefix="/api/demos/uc")
SCHEMA = "main.gov"


@router.post("/namespace")
def namespace(mistake: bool = False):
    s = core.spark()
    with Demo("uc-namespace") as d:
        core.ensure_schema(d, "gov")
        core.sql(d, f"CREATE TABLE IF NOT EXISTS {SCHEMA}.customers (id INT, email STRING, country STRING) USING DELTA COMMENT 'CRM export'")
        tree = []
        for cat in core.uc_json("GET", "/catalogs", d).get("catalogs", []):
            schemas = []
            for sch in core.uc_json("GET", "/schemas", d, params={"catalog_name": cat["name"]}).get("schemas", []):
                tables = core.uc_json("GET", "/tables", None, params={"catalog_name": cat["name"], "schema_name": sch["name"]}).get("tables", [])
                schemas.append({"name": sch["name"], "tables": [{"name": t["name"], "type": t["table_type"], "format": t.get("data_source_format")} for t in tables][:12]})
            tree.append({"name": cat["name"], "comment": cat.get("comment"), "schemas": schemas})
        info = core.uc_json("GET", f"/tables/{SCHEMA}.customers", d)
        current = core.sql(d, "SELECT current_catalog() AS catalog, current_schema() AS schema")[0]
        described = [r["col_name"] for r in core.sql(d, f"DESCRIBE TABLE {SCHEMA}.customers") if r["col_name"] and not r["col_name"].startswith("#")]
        qualified = core.scalar(d, "SELECT count(*) FROM samples.nyctaxi.trips")
        d.data.update(tree=tree, table={"full_name": info["catalog_name"] + "." + info["schema_name"] + "." + info["name"],
                                        "type": info["table_type"], "format": info["data_source_format"], "comment": info.get("comment"),
                                        "location": info.get("storage_location"), "columns": [c["name"] for c in info["columns"]]},
                      current=current)
        d.check("A table created with Spark SQL is visible through the UC REST API", info["name"] == "customers", f"{SCHEMA}.customers")
        d.check("UC stores the schema Spark declared", [c["name"] for c in info["columns"]] == described[:3], described[:3])
        d.check("The session works in catalog `main`, schema `default` (like a Databricks workspace)",
                current == {"catalog": "main", "schema": "default"}, current)
        d.check("A fully qualified catalog.schema.table name works from anywhere", qualified == 100_000, "samples.nyctaxi.trips")
        if mistake:
            _, error = d.attempt("SELECT count(*) FROM trips  # one-part name, but the current schema is main.default",
                                 lambda: s.sql("SELECT count(*) FROM trips").collect())
            d.check("An unqualified name resolves against the current catalog and schema, and fails", error and "TABLE_OR_VIEW_NOT_FOUND" in error, error)
            _, use_catalog = d.attempt("USE CATALOG samples  # Databricks SQL syntax", lambda: s.sql("USE CATALOG samples").collect())
            core.sql(d, "USE samples.nyctaxi")
            fixed = core.scalar(d, "SELECT count(*) FROM trips")
            core.sql(d, "USE main.default")
            d.check("USE catalog.schema (or a three-part name) fixes it", fixed == 100_000, f"{fixed:,} rows")
            d.check("`USE CATALOG x` is Databricks SQL; open-source Spark 4.2 spells it `USE x` / `SET CATALOG x`",
                    use_catalog and "PARSE_SYNTAX_ERROR" in use_catalog, use_catalog)
            d.data["mistake"] = {"unqualified": error, "useCatalog": use_catalog}
    return d.result()


@router.post("/managed-external")
def managed_vs_external(mistake: bool = False):
    s = core.spark()
    with Demo("uc-managed-external") as d:
        core.ensure_schema(d, "gov")
        managed, external = f"{SCHEMA}.orders_managed", f"{SCHEMA}.orders_external"
        ext_loc = core.ext_path("gov_orders_external")
        core.drop_table(d, managed)
        core.drop_table(d, external, str(ext_loc))
        core.sql(d, f"CREATE TABLE {managed} (id INT, total DOUBLE) USING DELTA")
        core.sql(d, f"CREATE TABLE {external} (id INT, total DOUBLE) USING DELTA LOCATION '{ext_loc}'")
        for t in (managed, external):
            core.sql(d, f"INSERT INTO {t} VALUES (1, 9.5), (2, 20.0), (3, 4.25)")
        details = {}
        for kind, t in (("managed", managed), ("external", external)):
            detail = core.sql(d, f"DESCRIBE DETAIL {t}")[0]
            info = core.uc_json("GET", f"/tables/{t}", d)
            details[kind] = {"name": t, "type": info["table_type"], "location": str(detail["location"]).replace("file:", ""),
                             "features": detail.get("tableFeatures") or [], "files": detail["numFiles"]}
        managed_dir = Path(details["managed"]["location"])
        core.sql(d, f"DROP TABLE {managed}")
        core.sql(d, f"DROP TABLE {external}")
        after_drop = {"managedRegistered": core.uc("GET", f"/tables/{managed}", d).status_code == 200,
                      "externalRegistered": core.uc("GET", f"/tables/{external}", d).status_code == 200,
                      "managedFilesOnDisk": len(core.data_files(managed_dir)),
                      "externalFilesOnDisk": len(core.data_files(ext_loc))}
        core.sql(d, f"CREATE TABLE {external} USING DELTA LOCATION '{ext_loc}'")
        reattached = core.scalar(d, f"SELECT count(*) FROM {external}")
        d.data.update(details=details, afterDrop=after_drop, reattachedRows=reattached)
        d.check("UC chose the managed table's location under the catalog's storage root",
                "__unitystorage" in details["managed"]["location"] and details["managed"]["type"] == "MANAGED", details["managed"]["location"])
        d.check("The external table lives exactly where we said", details["external"]["location"] == str(ext_loc) and details["external"]["type"] == "EXTERNAL",
                details["external"]["location"])
        d.check("Managed tables are catalog-managed: UC coordinates their commits",
                any("catalog" in f.lower() for f in details["managed"]["features"]), details["managed"]["features"])
        d.check("DROP unregisters both tables", not after_drop["managedRegistered"] and not after_drop["externalRegistered"], after_drop)
        d.check("DROP never deletes an external table's files: re-registering brings the data back",
                after_drop["externalFilesOnDisk"] > 0 and reattached == 3, f"{reattached} rows")
        if mistake:
            core.sql(d, f"CREATE TABLE {managed} (id INT, total DOUBLE) USING DELTA")
            _, optimize_error = d.attempt(f"OPTIMIZE {managed}", lambda: s.sql(f"OPTIMIZE {managed}").collect())
            _, parquet_error = d.attempt(f"CREATE TABLE {SCHEMA}.orders_parquet (id INT) USING PARQUET",
                                         lambda: s.sql(f"CREATE TABLE {SCHEMA}.orders_parquet (id INT) USING PARQUET").collect())
            d.check("OSS Unity Catalog blocks OPTIMIZE on catalog-managed tables", optimize_error and "CATALOG_MANAGED" in optimize_error, optimize_error)
            d.check("Managed tables must be Delta", parquet_error and "non-Delta managed table" in parquet_error, parquet_error)
            d.data["mistake"] = {"optimize": optimize_error, "parquet": parquet_error}
    return d.result()


@router.post("/volumes")
def volumes(mistake: bool = False):
    s = core.spark()
    with Demo("uc-volumes") as d:
        core.ensure_schema(d, "gov")
        name = f"{SCHEMA}.landing"
        if core.uc("GET", f"/volumes/{name}", d).status_code == 404:
            core.uc_json("POST", "/volumes", d, json={"catalog_name": "main", "schema_name": "gov", "name": "landing", "volume_type": "MANAGED",
                                                      "comment": "Raw files dropped by partners"})
        volume = core.uc_json("GET", f"/volumes/{name}", d)
        root = Path(volume["storage_location"].replace("file://", "").replace("file:", ""))
        folder = root / "exports"
        core.remove_dir(folder)
        folder.mkdir(parents=True, exist_ok=True)
        files = []
        for day, rows in (("2026-10-01", [("ana", 3), ("ben", 1)]), ("2026-10-02", [("cy", 7)]), ("2026-10-03", [("dee", 2), ("eli", 5), ("fay", 1)])):
            path = folder / f"signups_{day}.csv"
            path.write_text("name,visits\n" + "".join(f"{n},{v}\n" for n, v in rows))
            files.append({"name": path.name, "bytes": path.stat().st_size, "volumePath": f"/Volumes/main/gov/landing/exports/{path.name}"})
        d.command(f"write 3 CSV files to /Volumes/main/gov/landing/exports/  (storage: {folder})")
        df = s.read.option("header", "true").option("inferSchema", "true").csv(str(folder))
        rows = core.collect(d, f"spark.read.option('header', 'true').csv('{folder}')", df)
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SCHEMA}.signups")
        d.command(f"df.write.mode('overwrite').saveAsTable('{SCHEMA}.signups')")
        d.data.update(volume={"full_name": volume["full_name"] if "full_name" in volume else name, "type": volume["volume_type"],
                              "storage": volume["storage_location"], "path": "/Volumes/main/gov/landing"}, files=files, rows=rows)
        d.check("The volume is a governed folder registered in Unity Catalog", volume["volume_type"] == "MANAGED", volume["storage_location"])
        d.check("Spark read every file in the folder", len(rows) == 6, f"{len(rows)} rows from {len(files)} files")
        d.check("The files became a governed Delta table", core.scalar(d, f"SELECT sum(visits) FROM {SCHEMA}.signups") == 19, "19 visits")
        if mistake:
            _, error = d.attempt("spark.read.csv('/Volumes/main/gov/landing/exports')  # the Databricks path, outside Databricks",
                                 lambda: s.read.option("header", "true").csv("/Volumes/main/gov/landing/exports").collect())
            d.check("/Volumes/... exists only where Databricks mounts it; elsewhere use the volume's storage location",
                    error and "PATH_NOT_FOUND" in error, error)
            d.data["mistake"] = error
    return d.result()


@router.post("/functions")
def functions(mistake: bool = False):
    s = core.spark()
    with Demo("uc-functions") as d:
        core.ensure_schema(d, "gov")
        core.sql(d, f"CREATE TABLE IF NOT EXISTS {SCHEMA}.contacts (name STRING, email STRING) USING DELTA")
        if core.scalar(d, f"SELECT count(*) FROM {SCHEMA}.contacts") == 0:
            core.sql(d, f"INSERT INTO {SCHEMA}.contacts VALUES ('Ana','ana.silva@example.com'), ('Ben','ben@corp.io'), ('Cy','cy.long.name@mail.org')")
        fn = f"{SCHEMA}.mask_email"
        core.uc("DELETE", f"/functions/{fn}", d)
        spec = {"function_info": {
            "name": "mask_email", "catalog_name": "main", "schema_name": "gov", "data_type": "STRING", "full_data_type": "string",
            "input_params": {"parameters": [{"name": "email", "type_text": "string", "type_name": "STRING", "position": 0,
                                             "type_json": '{"name":"email","type":"string","nullable":true,"metadata":{}}'}]},
            "routine_body": "EXTERNAL", "external_language": "python", "parameter_style": "S", "is_deterministic": True,
            "sql_data_access": "NO_SQL", "is_null_call": False, "security_type": "DEFINER", "specific_name": "mask_email",
            "routine_definition": "return email[0] + '***@' + email.split('@')[1]",
            "comment": "Masks the local part of an email address"}}
        core.uc_json("POST", "/functions", d, json=spec)
        listed = [f["name"] for f in core.uc_json("GET", "/functions", d, params={"catalog_name": "main", "schema_name": "gov"}).get("functions", [])]
        _, call_error = d.attempt(f"SELECT {fn}(email) FROM {SCHEMA}.contacts  # call the UC function from Spark",
                                  lambda: s.sql(f"SELECT {fn}(email) FROM {SCHEMA}.contacts").collect())
        core.sql(d, "CREATE OR REPLACE TEMPORARY FUNCTION mask_email(email STRING) RETURNS STRING "
                    "RETURN concat(left(email, 1), '***@', split_part(email, '@', 2))")
        masked = core.sql(d, f"SELECT name, email, mask_email(email) AS masked FROM {SCHEMA}.contacts ORDER BY name")
        d.data.update(function={"name": fn, "language": "python", "body": spec["function_info"]["routine_definition"]},
                      listed=listed, callError=call_error, rows=masked)
        d.check("The function is registered (and governed) in Unity Catalog", "mask_email" in listed, listed)
        d.check("OSS Spark's UC connector cannot execute UC functions (Databricks can)", call_error and "MISSING_CATALOG_ABILITY" in call_error, call_error)
        d.check("A SQL function gives the same masking inside Spark", all(r["masked"].startswith(r["email"][0] + "***@") for r in masked),
                [r["masked"] for r in masked])
        if mistake:
            other = SparkSession.builder.remote(core.SPARK_REMOTE).create()
            d.command("other = SparkSession.builder.remote(...).create()  # e.g. a different job or notebook")
            try:
                _, error = d.attempt("other.sql(\"SELECT mask_email('x@y.z')\")", lambda: other.sql("SELECT mask_email('x@y.z')").collect())
            finally:
                other.stop()
            d.check("TEMPORARY functions exist only in the session that created them", error and ("UNRESOLVED_ROUTINE" in error or "MISSING_CATALOG_ABILITY" in error), error)
            d.data["mistake"] = error
    return d.result()
