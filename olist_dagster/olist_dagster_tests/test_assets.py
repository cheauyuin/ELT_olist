from olist_dagster.assets import pipeline


def test_asset_names():
    expected = {
        "meltano_extract_load",
        "ge_raw_validation",
        "dbt_staging",
        "dbt_snapshot",
        "dbt_marts",
        "generate_dashboard",
        "git_push_dashboard",
        "alert_declining_sellers",
    }
    actual = {a.key.to_python_identifier() for a in pipeline.__dict__.values()
              if hasattr(a, "key")}
    assert expected == actual
