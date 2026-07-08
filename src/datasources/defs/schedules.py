import dagster as dg

mdharura_sync_job = dg.define_asset_job(
    name="mdharura_sync_job",
    selection=dg.AssetSelection.groups("mdharura"),
    description="Loads m-Dharura signals for one partition (day) into MinIO",
)

# daily-partitioned job -> schedule fires at 06:00 UTC for the previous day
sync_mdharura_signals_daily = dg.build_schedule_from_partitioned_job(
    mdharura_sync_job,
    hour_of_day=6,
    name="sync_mdharura_signals_daily",
    description="Syncs the previous day's signals from m-Dharura every day at 06:00 UTC",
)

evd_screening_sync_job = dg.define_asset_job(
    name="evd_screening_sync_job",
    selection=dg.AssetSelection.groups("evd_screening"),
    description="Loads PoE health screenings for one partition (day) into MinIO",
)

# fires at 07:00 rather than 06:00 purely to stagger the two sources' load
sync_evd_screening_screenings_daily = dg.build_schedule_from_partitioned_job(
    evd_screening_sync_job,
    hour_of_day=7,
    name="sync_evd_screening_screenings_daily",
    description="Syncs the previous day's PoE health screenings every day at 07:00 UTC",
)
