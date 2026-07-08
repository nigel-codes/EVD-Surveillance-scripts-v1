import dagster as dg

sync_mdharura_signals_daily = dg.ScheduleDefinition(
    name="sync_mdharura_signals_daily",
    target=dg.AssetSelection.groups("mdharura"),
    cron_schedule="0 6 * * *",
    description="Syncs signals from m-Dharura every day at 06:00 UTC"
)