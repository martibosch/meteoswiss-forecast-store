app_name        := "icon-ogd-ingest"
ingest_module   := "meteoswiss_nwp_store.ingest_icon_ogd"
eps_module      := "meteoswiss_nwp_store.ingest_icon_ch_eps"
extract_module  := "meteoswiss_nwp_store.extract_stations"
ingest_bucket   := "meteoswiss-nwp-store"
ssh_host        := env("SSH_HOST")

prefix_ch1_anl_ml  := "icon-ch1-anl-ml"
prefix_ch2_anl_ml  := "icon-ch2-anl-ml"
prefix_ch1_anl_sfc := "icon-ch1-anl-sfc"
prefix_ch2_anl_sfc := "icon-ch2-anl-sfc"

# Canonical EPS control-member forecast store (extendable, not period-specific).
prefix_ch1_eps_fc := "icon-ch1-eps-fc"

arraylake_org            := "martibosch"
arraylake_bucket_nickname := "meteoswiss-forecast-store"

# deployment

deploy:
    modal deploy -m {{ingest_module}}

stop:
    modal app stop {{app_name}}

# arraylake repo creation

create-al-repo-ch1-anl-ml:
    arraylake repo create {{arraylake_org}}/meteoswiss-icon-ch1-anl-ml \
        --bucket-config-nickname {{arraylake_bucket_nickname}} \
        --prefix {{prefix_ch1_anl_ml}} --import-existing

create-al-repo-ch2-anl-ml:
    arraylake repo create {{arraylake_org}}/meteoswiss-icon-ch2-anl-ml \
        --bucket-config-nickname {{arraylake_bucket_nickname}} \
        --prefix {{prefix_ch2_anl_ml}} --import-existing

create-al-repo-ch1-anl-sfc:
    arraylake repo create {{arraylake_org}}/meteoswiss-icon-ch1-anl-sfc \
        --bucket-config-nickname {{arraylake_bucket_nickname}} \
        --prefix {{prefix_ch1_anl_sfc}} --import-existing

create-al-repo-ch2-anl-sfc:
    arraylake repo create {{arraylake_org}}/meteoswiss-icon-ch2-anl-sfc \
        --bucket-config-nickname {{arraylake_bucket_nickname}} \
        --prefix {{prefix_ch2_anl_sfc}} --import-existing

# ingestion

ingest-ch1-anl-ml:
    modal run -m {{ingest_module}} --model ch1 --level ml \
        --bucket {{ingest_bucket}} --prefix {{prefix_ch1_anl_ml}}

ingest-ch2-anl-ml:
    modal run -m {{ingest_module}} --model ch2 --level ml \
        --bucket {{ingest_bucket}} --prefix {{prefix_ch2_anl_ml}}

ingest-ch1-anl-sfc:
    modal run -m {{ingest_module}} --model ch1 --level sfc \
        --bucket {{ingest_bucket}} --prefix {{prefix_ch1_anl_sfc}}

ingest-ch2-anl-sfc:
    modal run -m {{ingest_module}} --model ch2 --level sfc \
        --bucket {{ingest_bucket}} --prefix {{prefix_ch2_anl_sfc}}

# EPS control-member forecast data lake (ingest_icon_ch_eps.py)
# Full forecasts (all lead times) are stored as the canonical archive.
# Analysis (lead_time=0) is derived on read: ds.isel(lead_time=0).
# The store (prefix_ch1_eps_fc) is period-agnostic and can be extended
# by running any ingest-ch1-eps-* recipe that covers new dates.

ingest-ch1-eps-jja-2025:
    python -m {{eps_module}} \
        --model ch1 \
        --ssh-host {{ssh_host}} \
        --bucket {{ingest_bucket}} \
        --prefix {{prefix_ch1_eps_fc}} \
        --start 2025-06-01T00 \
        --end 2025-08-31T21

extract-ch1-eps-jja-2025-stations stations output:
    python -m {{extract_module}} \
        --bucket {{ingest_bucket}} \
        --prefix {{prefix_ch1_eps_fc}} \
        --stations {{stations}} \
        --output {{output}} \
        --start 2025-06-01T00 \
        --end 2025-08-31T21
