app_name      := "icon-ogd-ingest"
ingest_module := "meteoswiss_forecast_store.ingest_icon_ogd"
ingest_bucket := "meteoswiss-forecast-store"

prefix_ch1_anl_ml  := "icon-ch1-anl-ml"
prefix_ch2_anl_ml  := "icon-ch2-anl-ml"
prefix_ch1_anl_sfc := "icon-ch1-anl-sfc"
prefix_ch2_anl_sfc := "icon-ch2-anl-sfc"

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
