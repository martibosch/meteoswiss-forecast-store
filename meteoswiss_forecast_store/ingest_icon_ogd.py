"""Ingest ICON-CH forecasts from MeteoSwiss Open Data into icechunk on S3.

Fetches the latest analysis snapshot (step=0) from the MeteoSwiss OGD STAC API
using meteodata-lab, then appends it to an icechunk zarr store.

Supports ICON-CH1-EPS (1 km, 3h cycle) and ICON-CH2-EPS (2.1 km, 6h cycle),
both single-level and multi-level variables.

Deployed as a Modal cron app with two scheduled functions:
  - ICON-CH1-EPS multi-level: every 3h
  - ICON-CH2-EPS multi-level: every 6h

Manual usage:
    modal run -m meteoswiss_forecast_store.ingest_icon_ogd \
        --model ch2 --level multi \
        --bucket my-bucket --prefix icon-ch2-ml

Deploy cron jobs:
    modal deploy -m meteoswiss_forecast_store.ingest_icon_ogd
"""

import logging
import os

import modal

log = logging.getLogger(__name__)

# Modal app setup

APP_NAME = "icon-ogd-ingest"

app = modal.App(APP_NAME)

aws_credentials_secret = modal.Secret.from_name("meteoswiss-forecast-store-tigris")

image = (
    modal.Image.micromamba(python_version="3.12")
    .micromamba_install("eccodes", channels=["conda-forge"])
    .uv_pip_install(
        "icechunk",
        "meteodata-lab",
        "xarray",
        "zarr",
    )
    .env({"EARTHKIT_DATA_CACHE_POLICY": "temporary"})
)

# model / variable configuration

COLLECTIONS = {
    "ch1": "ogd-forecasting-icon-ch1",
    "ch2": "ogd-forecasting-icon-ch2",
}

SINGLE_LEVEL_VARS = [
    "ALB_RAD",
    "ALHFL_S",
    "ASHFL_S",
    "ASOB_S",
    "ASWDIFD_S",
    "ASWDIFU_S",
    "ASWDIR_S",
    "ATHB_S",
    "CAPE_ML",
    "CAPE_MU",
    "CEILING",
    "CIN_ML",
    "CIN_MU",
    "CLAT",
    "CLCH",
    "CLCL",
    "CLCM",
    "CLCT",
    "CLON",
    "DBZ_850",
    "DBZ_CMAX",
    "DURSUN",
    "DURSUN_M",
    "FR_LAND",
    "GRAU_GSP",
    "HSURF",
    "HZEROCL",
    "H_SNOW",
    "LCL_ML",
    "LFC_ML",
    "PMSL",
    "PS",
    "RAIN_GSP",
    "SDI_2",
    "SI",
    "SLI",
    "SNOWLMT",
    "SNOW_GSP",
    "SOILTYP",
    "T_2M",
    "T_G",
    "T_SNOW",
    "TD_2M",
    "TMAX_2M",
    "TMIN_2M",
    "TOT_PR",
    "TOT_PREC",
    "TWATER",
    "U_10M",
    "V_10M",
    "VMAX_10M",
    "W_SNOW",
    "Z0",
]

MULTI_LEVEL_VARS = [
    "CLC",
    "HHL",
    "P",
    "QC",
    "QV",
    "T",
    "TKE",
    "T_SO",
    "U",
    "V",
    "W",
]

LEVEL_VARS = {
    "sfc": SINGLE_LEVEL_VARS,
    "ml": MULTI_LEVEL_VARS,
}

# default store config - override via env vars in the Modal secret.

DEFAULT_BUCKET = "meteoswiss-forecast-store"
STORE_PREFIXES = {
    ("ch1", "ml"): "icon-ch1-anl-ml",
    ("ch2", "ml"): "icon-ch2-anl-ml",
    ("ch1", "sfc"): "icon-ch1-anl-sfc",
    ("ch2", "sfc"): "icon-ch2-anl-sfc",
}


# ingestion logic (runs inside Modal container)


def _probe_ref_time(
    model: str,
    level: str,
    reference_datetime: str = "latest",
    horizon: str = "P0DT0H",
) -> str | None:
    """Fetch one variable to resolve the ref_time without pulling all data."""
    from meteodatalab import ogd_api

    req = ogd_api.Request(
        collection=COLLECTIONS[model],
        variable=LEVEL_VARS[level][0],
        reference_datetime=reference_datetime,
        perturbed=False,
        horizon=horizon,
    )
    try:
        da = ogd_api.get_from_ogd(req)
    except Exception as exc:
        log.warning("Could not probe ref_time: %s", exc)
        return None

    if "ref_time" in da.coords:
        return str(da.coords["ref_time"].values.ravel()[0])
    return None


def _fetch_snapshot(
    model: str,
    level: str,
    reference_datetime: str = "latest",
    horizon: str = "P0DT0H",
):
    """Fetch all variables for one snapshot, return an xr.Dataset or None."""
    import xarray as xr
    from meteodatalab import ogd_api

    collection = COLLECTIONS[model]
    variables = LEVEL_VARS[level]

    arrays: dict[str, xr.DataArray] = {}
    for var in variables:
        req = ogd_api.Request(
            collection=collection,
            variable=var,
            reference_datetime=reference_datetime,
            perturbed=False,
            horizon=horizon,
        )
        try:
            da = ogd_api.get_from_ogd(req)
            arrays[var] = da
            log.info("Fetched %s", var)
        except Exception as exc:
            log.warning("Could not fetch %s: %s", var, exc)

    if not arrays:
        return None

    ds = xr.Dataset(arrays)

    # Drop the size-1 eps dimension — implied by perturbed=False / the store prefix.
    if "eps" in ds.dims and ds.sizes["eps"] == 1:
        ds = ds.squeeze("eps", drop=True)

    # Drop valid_time — it's derivable as ref_time + lead_time.
    if "valid_time" in ds.coords:
        ds = ds.drop_vars("valid_time")

    # Drop size-1 lead_time=0 — implied by the anl (analysis) product type.
    if (
        "lead_time" in ds.dims
        and ds.sizes["lead_time"] == 1
        and ds.coords["lead_time"].values[0] == 0
    ):
        ds = ds.squeeze("lead_time", drop=True)

    # Ensure ref_time is a dimension (not scalar) for zarr append_dim.
    if "ref_time" in ds.coords and "ref_time" not in ds.dims:
        ds = ds.expand_dims("ref_time")

    # drop string/char variables — incompatible with Zarr V3 spec.
    str_vars = [v for v in ds.variables if ds[v].dtype.kind in ("S", "U", "O")]
    if str_vars:
        log.info("Dropping non-numeric variables: %s", str_vars)
        ds = ds.drop_vars(str_vars)

    return ds


def _ingest(
    model: str,
    level: str,
    bucket: str | None = None,
    prefix: str | None = None,
    reference_datetime: str = "latest",
    horizon: str = "P0DT0H",
) -> None:
    """Fetch one forecast snapshot and append to the icechunk store."""
    import icechunk
    import zarr

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    bucket = bucket or os.environ.get("ICON_STORE_BUCKET", DEFAULT_BUCKET)
    prefix = prefix or os.environ.get(
        f"ICON_{model.upper()}_{level.upper()}_PREFIX",
        STORE_PREFIXES[(model, level)],
    )

    # open store before any heavy API requests
    storage = icechunk.tigris_storage(
        bucket=bucket,
        prefix=prefix,
        region=os.environ["AWS_REGION"],
        from_env=True,
    )
    repo = icechunk.Repository.open_or_create(storage=storage)
    session = repo.writable_session("main")
    store = session.store

    try:
        existing = zarr.open_group(store, mode="r")
        empty = len(list(existing.arrays())) == 0
    except Exception:
        empty = True

    # probe ref_time with a single cheap request before fetching all variables
    log.info(
        "Probing ref_time for ICON-%s %s (ref=%s)",
        model.upper(),
        level,
        reference_datetime,
    )
    ref_time = _probe_ref_time(
        model, level, reference_datetime=reference_datetime, horizon=horizon
    )
    if ref_time is None:
        log.error("Could not resolve ref_time — aborting.")
        return

    if not empty:
        import numpy as np
        import xarray as xr

        existing_ds = xr.open_zarr(store, consolidated=False)
        if np.datetime64(ref_time, "ns") in existing_ds.ref_time.values:
            log.info("ref_time %s already in store — skipping.", ref_time)
            return

    log.info(
        "Fetching ICON-%s %s-level snapshot for %s", model.upper(), level, ref_time
    )
    ds = _fetch_snapshot(
        model, level, reference_datetime=reference_datetime, horizon=horizon
    )

    if ds is None:
        log.error("No data fetched — aborting.")
        return

    log.info("Fetched %d variables for %s", len(ds.data_vars), ref_time)

    # Strip non-JSON-serializable attributes (e.g. WrappedMetadata from meteodatalab)
    # before writing to zarr.
    def _sanitize_attrs(attrs: dict) -> dict:
        import json

        result = {}
        for k, v in attrs.items():
            try:
                json.dumps(v)
                result[k] = v
            except (TypeError, ValueError):
                pass
        return result

    ds = ds.assign_attrs(_sanitize_attrs(ds.attrs))
    for var in list(ds.data_vars) + list(ds.coords):
        ds[var].attrs = _sanitize_attrs(ds[var].attrs)

    if empty:
        ds.to_zarr(store, mode="w", consolidated=False)
    else:
        ds.to_zarr(store, append_dim="ref_time", consolidated=False)

    msg = f"Ingested ICON-{model.upper()} {level}-level {ref_time}"
    snapshot = session.commit(msg)
    log.info("%s [snapshot %s]", msg, snapshot[:12])


# scheduled functions


@app.function(
    image=image,
    secrets=[aws_credentials_secret],
    timeout=600,
    schedule=modal.Cron("30 0,3,6,9,12,15,18,21 * * *"),
)
def ingest_ch1_ml():
    """Ingest ICON-CH1-EPS multi-level (every 3h)."""
    _ingest("ch1", "ml")


@app.function(
    image=image,
    secrets=[aws_credentials_secret],
    timeout=600,
    schedule=modal.Cron("30 0,6,12,18 * * *"),
)
def ingest_ch2_ml():
    """Ingest ICON-CH2-EPS multi-level (every 6h)."""
    _ingest("ch2", "ml")


@app.function(
    image=image,
    secrets=[aws_credentials_secret],
    timeout=600,
    schedule=modal.Cron("30 0,3,6,9,12,15,18,21 * * *"),
)
def ingest_ch1_sfc():
    """Ingest ICON-CH1-EPS surface (every 3h)."""
    _ingest("ch1", "sfc")


@app.function(
    image=image,
    secrets=[aws_credentials_secret],
    timeout=600,
    schedule=modal.Cron("30 0,6,12,18 * * *"),
)
def ingest_ch2_sfc():
    """Ingest ICON-CH2-EPS surface (every 6h)."""
    _ingest("ch2", "sfc")


# manual trigger (e.g., to test)


@app.function(
    image=image,
    secrets=[aws_credentials_secret],
    timeout=600,
)
def ingest_once(
    model: str,
    level: str,
    bucket: str | None = None,
    prefix: str | None = None,
    reference_datetime: str = "latest",
    horizon: str = "P0DT0H",
):
    """One-shot ingestion, called from local_entrypoint or other Modal functions."""
    _ingest(model, level, bucket, prefix, reference_datetime, horizon)


@app.local_entrypoint()
def main(
    model: str = "ch2",
    level: str = "ml",
    bucket: str = "",
    prefix: str = "",
    ref_datetime: str = "latest",
    horizon: str = "P0DT0H",
):
    """Manual trigger.

    modal run -m meteoswiss_forecast_store.ingest_icon_ogd --model ch2 --level multi.
    """
    ingest_once.remote(
        model=model,
        level=level,
        bucket=bucket or None,
        prefix=prefix or None,
        reference_datetime=ref_datetime,
        horizon=horizon,
    )
