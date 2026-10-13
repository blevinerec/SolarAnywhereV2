# function_app.py (Python v2 + Durable)
from azure.functions import FunctionApp, TimerRequest
import azure.durable_functions as df
import logging
import os
import numpy as np
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import text
from datetime import datetime, timedelta, timezone

from pvlib import location, irradiance

from pvlib.iotools.solaranywhere import get_solaranywhere  # older pvlib
from pvlib.pvsystem import PVSystem
from pvlib.tracking import singleaxis

import sentry_sdk
from timezonefinder import TimezoneFinder
from dotenv import load_dotenv
load_dotenv(override=True)  # makes .env values authoritative locally
# ----------------------------
# ENV & constants
# ----------------------------

def _require_env(name: str) -> str:
    v = os.environ.get(name)
    if not v or not v.strip():
        raise RuntimeError(f"{name} is not defined")
    return v

# QUALIFIED_TARGET = PERF_DB_TABLE_NAME if '.' in PERF_DB_TABLE_NAME else f"dbo.{PERF_DB_TABLE_NAME}"
# SQL_SERVER = os.environ.get('DB_SERVER')
# SQL_DB     = os.environ.get('DB_NAME')
# SQL_USER   = os.environ.get('DB_USER')
# SQL_PWD    = os.environ.get('DB_PASSWORD')

# SA_API_KEY = os.environ.get('SOLAR_ANYWHERE_API_KEY')
# BACKFILL_DATE = os.environ.get('BACKFILL_DATE')  # optional, used by manual/HTTP starters

# # Adjust to match your deployed driver
# ODBC_DRIVER = os.environ.get('ODBC_DRIVER', 'ODBC Driver 18 for SQL Server')

# CONNECT_STR = (
#     f"mssql+pyodbc://{SQL_USER}:{SQL_PWD}@{SQL_SERVER}/{SQL_DB}"
#     f"?driver={ODBC_DRIVER}"
# )

def _qualify_table(name: str) -> str:
    return name if '.' in name else f"dbo.{name}"

def _build_connect_str() -> str:
    server = os.environ.get('DB_SERVER')
    db     = os.environ.get('DB_NAME')
    user   = os.environ.get('DB_USER')
    pwd    = os.environ.get('DB_PASSWORD')
    driver = os.environ.get('ODBC_DRIVER', 'ODBC Driver 17 for SQL Server')
    if not all([server, db, user, pwd]):
        raise RuntimeError("SQL env missing: DB_SERVER/DB_NAME/DB_USER/DB_PASSWORD")
    return f"mssql+pyodbc://{user}:{pwd}@{server}/{db}?driver={driver}"

# Batch controls
BATCH_SIZE = int(os.environ.get('BATCH_SIZE', '10'))
BATCH_DELAY_SEC = int(os.environ.get('BATCH_DELAY_SEC', '2'))

app = FunctionApp()

# =================================================================
#  A) TIMER STARTER — kicks off durable orchestration (daily runs)
# =================================================================
@app.schedule(schedule="0 20 12,20 * * *", run_on_startup=True, arg_name="timer")
@app.durable_client_input('client')                            # <-- pass the parameter name
async def SatelliteDataMorningRun_Start(timer: TimerRequest, client: df.DurableOrchestrationClient) -> None:

    logging.info("Timer starter fired.")
    utc_now = datetime.now(timezone.utc)
    base_utc_start = datetime(utc_now.year, utc_now.month, utc_now.day, tzinfo=timezone.utc) - timedelta(days=1)
    base_utc_end   = base_utc_start + timedelta(days=1)

    # Ensure scheduled runs always target the immediate prior UTC day (defensive against clock skew/backfill).
    expected_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
    if base_utc_start != expected_start:
        logging.warning("Base UTC start %s adjusted to expected prior UTC day %s", base_utc_start.isoformat(), expected_start.isoformat())
        base_utc_start = expected_start
        base_utc_end = base_utc_start + timedelta(days=1)

    payload = {
        "mode": "scheduled",
        "baseUtcStart": base_utc_start.isoformat(),
        "baseUtcEnd": base_utc_end.isoformat()
    }

    instance_id = f"sat-{base_utc_start.strftime('%Y%m%d')}"
    status = await client.get_status(instance_id, show_history=True)
    if status is None:
        logging.info("Orchestration %s: no existing instance found.", instance_id)
        status_name = None
    else:
        # runtime_status may be an enum (e.g., OrchestrationRuntimeStatus.Failed), None, or a string.
        rs = getattr(status, "runtime_status", status)
        # Normalize to a clean status_name; treat None or 'None' as no status.
        if rs is None:
            status_name = None
        else:
            status_name = getattr(rs, "name", None) or str(rs).split('.')[-1]
            if isinstance(status_name, str) and status_name.lower() == 'none':
                status_name = None
        hist_len = len(getattr(status, "history", []) or [])
        logging.info("Orch status: %s created=%s last_updated=%s runtime=%s events=%d",
                     getattr(status, "instance_id", instance_id),
                     getattr(status, "created_time", None),
                     getattr(status, "last_updated_time", None),
                     status_name,
                     hist_len)

    if status is None or status_name is None or status_name in {"Completed", "Failed", "Terminated"}:
        try:
            await client.start_new("SatelliteDataMorningRun_Orchestrator", instance_id=instance_id, client_input=payload)
            logging.info("Started orchestration %s for %s → %s", instance_id, payload["baseUtcStart"], payload["baseUtcEnd"])
        except Exception as e:
            logging.error("Failed to start orchestration %s: %s", instance_id, e)
            raise
    else:
        logging.info("Orchestration %s already %s", instance_id, status_name or "<unknown>")


# =================================================================
#  B) ORCHESTRATOR — fan-out/fan-in with batching + backpressure
# =================================================================


@app.orchestration_trigger("context")
def SatelliteDataMorningRun_Orchestrator(context: df.DurableOrchestrationContext):
    payload    = context.get_input() or {}
    base_start = _parse_utc(payload.get("baseUtcStart"))
    base_end   = _parse_utc(payload.get("baseUtcEnd"))

    table_name  = _require_env('PERF_DB_TABLE_NAME')
    perf_target = _qualify_table(table_name)

    sites = yield context.call_activity("GetSites", None)

    for i in range(0, len(sites), BATCH_SIZE):
        batch = sites[i:i+BATCH_SIZE]
        tasks = []
        for site in batch:
            ch_req = {"systemId": site["System_ID"]}
            channel_map = yield context.call_activity("GetChannelsForSystem", ch_req)

            proc_req = {
                "site": site,
                "baseUtcStart": base_start.isoformat(),
                "baseUtcEnd": base_end.isoformat(),
                "channelMap": channel_map,
                "perfTarget": perf_target
            }
            tasks.append(context.call_activity("ProcessSite", proc_req))

        yield context.task_all(tasks)
        yield context.create_timer(context.current_utc_datetime + timedelta(seconds=BATCH_DELAY_SEC))

    return {"ok": True, "sitesProcessed": len(sites)}

def _parse_utc(s: str) -> datetime:
    if not s:
        return datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    return datetime.fromisoformat(s.replace("Z", "+00:00"))

# =================================================================
#  C) ACTIVITIES — DB reads; per-site compute; SQL MERGE
# =================================================================

@app.activity_trigger(input_name="ignore")
def GetSites(ignore):
    """
    Returns list of site dicts:
    {
      'System_ID': int, 'Name': str, 'Latitude': float, 'Longitude': float,
      'Ins_Type': 'POA'|'GHI', 'Racking': 'Tracker'|...,
      'Tilt': float, 'Azimuth': float, 'Max_Angle': float, 'GCR_East': float, 'GCR_West': float,
      'Timezone': 'America/New_York' | ...
    }
    """
    logging.info("GetSites: querying system metadata.")
    engine = sa.create_engine(_build_connect_str())
    tf = TimezoneFinder()

    base_cols = [
        'System_ID', 'Name', 'Site_ID', 'Ins_Type', 'Racking', 'Tilt', 'Azimuth',
        'Latitude', 'Longitude', 'Max_Angle', 'GCR_East', 'GCR_West'
    ]
    q = f"SELECT {', '.join(base_cols)} FROM [dbo].[System]"
    df_sys = pd.read_sql(q, engine)

    # Compute tz and normalize Ins_Type
    df_sys = df_sys[~df_sys['Longitude'].isna()].copy()
    df_sys['Ins_Type'] = df_sys['Ins_Type'].apply(lambda x: "POA" if float(x) == 0.0 else "GHI")
    df_sys['Timezone'] = df_sys.apply(
        lambda x: tf.timezone_at(lng=x['Longitude'], lat=x['Latitude']) if x['Longitude'] and x['Latitude'] else np.nan,
        axis=1
    )

    # Keep only systems that have at least one satellite channel type
    engine.dispose()
    sites = df_sys.to_dict(orient="records")
    logging.info("GetSites: %d candidate systems.", len(sites))
    return sites

@app.activity_trigger(input_name="req")
def GetChannelsForSystem(req: dict):
    sys_id = req["systemId"]
    engine = sa.create_engine(_build_connect_str())    # <-- changed
    channel_types_sql = "'Ideal Tilt', 'Satellite Cell', 'Satellite POA', 'Satellite Ambient Temp', 'Satellite GHI'"
    q = f"""
    SELECT c.Channel_ID, d.System_ID, c.Channel_Type, c.DAS_Time_Period_Ind
    FROM [dbo].[Channel] c
    INNER JOIN [dbo].[Device] d ON c.[Device_ID] = d.[Device_ID]
    WHERE c.[Channel_Type] IN ({channel_types_sql}) AND d.[System_ID] = :sid
    """
    df_ch = pd.read_sql(text(q), engine, params={"sid": int(sys_id)})
    engine.dispose()

    ch_map = {
        row["Channel_Type"]: {
            "Channel_ID": int(row["Channel_ID"]),
            "DAS_Time_Period_Ind": int(row["DAS_Time_Period_Ind"]) if pd.notna(row["DAS_Time_Period_Ind"]) else None
        }
        for _, row in df_ch.iterrows()
    }

    return ch_map

@app.activity_trigger(input_name="req")
def ProcessSite(req: dict):
    """
    Per-site heavy lifting: compute window, fetch SolarAnywhere, compute POA/cell temps, then MERGE to SQL.
    """

    # --- Environment values (pulled at call time; avoids import-time failures) ---
    sa_api_key = os.environ.get('SOLAR_ANYWHERE_API_KEY')
    if not sa_api_key:
        raise RuntimeError("SOLAR_ANYWHERE_API_KEY is not defined")

    # Build SQL connect string locally (don’t rely on a module-level constant that may raise)
    db_server = os.environ.get('DB_SERVER')
    db_name   = os.environ.get('DB_NAME')
    db_user   = os.environ.get('DB_USER')
    db_pwd    = os.environ.get('DB_PASSWORD')
    odbc_drv  = os.environ.get('ODBC_DRIVER', 'ODBC Driver 17 for SQL Server')

    if not all([db_server, db_name, db_user, db_pwd]):
        raise RuntimeError("SQL connection env vars (DB_SERVER/DB_NAME/DB_USER/DB_PASSWORD) are not fully set")

    connect_str = (
        f"mssql+pyodbc://{db_user}:{db_pwd}@{db_server}/{db_name}"
        f"?driver={odbc_drv}"
    )

    # --- Inputs from orchestrator ---
    site        = req["site"]
    perf_target = req["perfTarget"]                # e.g., 'dbo.Perf_Interval'
    channel_map = req["channelMap"]                # { "Satellite GHI": id, ... }
    base_start  = _parse_utc(req["baseUtcStart"])
    base_end    = _parse_utc(req["baseUtcEnd"])

    # --- Optional: guard Sentry init so it’s not re-initialized every activity call ---
    # This is safe but you may prefer initializing once in a starter/HTTP function.
    if not getattr(ProcessSite, "_sentry_inited", False):
        try:
            # Avoid letting Sentry import/integration errors fail the activity. If you want integrations,
            # add them explicitly or ensure the sentry-sdk version includes the required integrations.
            sentry_sdk.init(
                dsn=os.environ.get('SENTRY_DSN'),
                send_default_pii=True,
                traces_sample_rate=0.2,
                environment="production",
                release="v2-durable",
                integrations=[]
            )
            ProcessSite._sentry_inited = True
        except Exception as e:
            logging.warning("Sentry init failed (continuing without Sentry): %s", e)

    # --- Unpack site data ---
    name           = site['Name']
    system_id      = int(site['System_ID'])
    irradiance_type = site['Ins_Type']            # 'GHI' or 'POA'
    config_type     = site['Racking']             # e.g., 'Tracker'
    tilt            = site['Tilt']
    azimuth         = site['Azimuth']
    lat             = site['Latitude']
    lon             = site['Longitude']
    tz_id           = site['Timezone']

    # --- Window (local yesterday) ---
    tz = tz_id
    # Compute the prior local day window relative to the provided base UTC *end*.
    # Using base_end ensures that the local date is the previous calendar day in the site's timezone
    # (e.g., base_end=2025-12-30T00:00Z → local end 2025-12-29 00:00:00-05:00 → start_local 2025-12-29).
    te = pd.Timestamp(base_end)
    if te.tzinfo is None:  # Naive (no timezone info)
        te = te.tz_localize("UTC")
    # Use one second before base_end to ensure we pick the correct prior local day.
    te_minus = te - pd.Timedelta(seconds=1)
    start_local = te_minus.tz_convert(tz).normalize()
    end_local = start_local + pd.Timedelta(days=1)

    logging.info(f"Site: {name} Timezone: {tz}")
    logging.info(f"[{name}:{system_id}] Window local({tz}): {start_local} → {end_local}; "
                 f"UTC: {start_local.tz_convert('UTC')} → {end_local.tz_convert('UTC')}; interval=15min; mode=scheduled")

    # --- DB engine ---
    engine = sa.create_engine(_build_connect_str())    # ✅ build at runtime

    # --- Solar position ---
    time_interval = 15
    times    = pd.date_range(start_local, end_local, freq='15min', tz=tz)
    site_loc = location.Location(lat, lon, tz=tz)
    solar_pos = site_loc.get_solarposition(times=times)

    # --- Tracker geometry (if applicable) ---
    ideal_angle_df = None
    if str(config_type).lower() == 'tracker':
        backtracking_angles = singleaxis(
            apparent_zenith=solar_pos['apparent_zenith'],
            apparent_azimuth=solar_pos['azimuth'],
            axis_tilt=0,
            axis_azimuth=azimuth,
            max_angle=site['Max_Angle'],
            backtrack=True,
            gcr=site['GCR_East']
        )
        ideal_angle_df = backtracking_angles[['surface_azimuth', 'surface_tilt']].copy()
        tilt_series    = ideal_angle_df['surface_tilt']
        azimuth_series = ideal_angle_df['surface_azimuth']
        tilt    = tilt_series
        azimuth = azimuth_series
        ideal_angle_df.reset_index(inplace=True)
        ideal_angle_df.rename(columns={"index": 'timestamp', 'surface_tilt': 'Ideal Tilt'}, inplace=True)
        ideal_angle_df['timestamp'] = ideal_angle_df['timestamp'].dt.tz_convert(tz)

    # --- SolarAnywhere fetch (via pvlib helper) ---
    sa_data, _meta = get_solaranywhere(
        latitude=lat,
        longitude=lon,
        api_key=sa_api_key,        # ✅ use local var
        time_resolution=time_interval,
        start=start_local,
        end=end_local
    )
    sa_data['StartTime'] = pd.to_datetime(sa_data['StartTime'])
    sa_df = sa_data.set_index('StartTime')

    # --- GHI-only path ---
    if irradiance_type == 'GHI':
        df_ghi = sa_df[['ghi', 'temp_air']].copy()
        df_ghi.rename(columns={'ghi': 'Satellite GHI', 'temp_air': 'Satellite Ambient Temp'}, inplace=True)
        df_ghi.index = df_ghi.index.tz_convert(tz=tz)
        df_ghi = df_ghi.reset_index()
        df_ghi['StartTime'] = df_ghi['StartTime'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df_ghi = df_ghi.rename(columns={'StartTime': 'Start_Dt'})
        df_ghi['System_ID'] = system_id

        melted = df_ghi.melt(id_vars=['Start_Dt', 'System_ID'],
                             var_name='Channel_Type', value_name='Qty')
    
        # Map Channel_Type -> Channel_ID

        # Build lookup from channel_map
        period_lookup = (
            pd.DataFrame.from_dict(channel_map, orient='index')
            .reset_index()
            .rename(columns={'index': 'Channel_Type'})
        )

        # Join to get Channel_ID and DAS_Time_Period_Ind per Channel_Type
        melted = melted.merge(period_lookup, on='Channel_Type', how='left')

        # Keep only rows that have a valid channel id
        melted = melted.dropna(subset=['Channel_ID'])
        melted['Channel_ID'] = melted['Channel_ID'].astype(int)

        # Use DB-mapped indicator; fallback to 15 if missing
        melted['Time_Period_Ind'] = melted['DAS_Time_Period_Ind'].fillna(15).astype(int)


        satellite_df = melted[['Start_Dt', 'Channel_ID', 'Time_Period_Ind', 'Qty']].copy()
        _bulk_upsert_with_merge(engine, perf_target, satellite_df, system_id, name)
        return {"systemId": system_id, "rows": int(len(satellite_df))}

    # --- POA path ---
    joined = pd.merge(solar_pos, sa_df, left_index=True, right_index=True, how='left')
    joined.index = joined.index.tz_convert(tz=tz)

    poa = irradiance.get_total_irradiance(
        surface_tilt=tilt,
        surface_azimuth=azimuth,
        dni=joined['dni'],
        ghi=joined['ghi'],
        dhi=joined['dhi'],
        solar_zenith=joined['apparent_zenith'],
        solar_azimuth=joined['azimuth']
    )
    poa.index = poa.index.tz_convert(tz=tz)
    joined = pd.merge(joined, poa, left_index=True, right_index=True, how='left')

    pv_system = PVSystem(surface_tilt=tilt, surface_azimuth=azimuth)
    df_cell_temp = pv_system.get_cell_temperature(
        poa['poa_global'], joined['temp_air'], joined['wind_speed'], model='pvsyst'
    ).to_frame()
    df_cell_temp.rename(columns={0: 'Satellite Cell'}, inplace=True)
    df_cell_temp.reset_index(inplace=True)
    df_cell_temp.rename(columns={"index": 'timestamp'}, inplace=True)
    df_cell_temp['timestamp'] = df_cell_temp['timestamp'].dt.tz_convert(tz)

    df_cell_temp_merged = pd.merge(df_cell_temp, joined, left_on='timestamp', right_index=True, how='left')
    df_cell_temp_merged.rename(columns={'ghi': 'Satellite GHI', 'temp_air': 'Satellite Ambient Temp', 'poa_global': 'Satellite POA'}, inplace=True)
    df_cell_temp_merged['Satellite Cell'] = df_cell_temp_merged['Satellite Cell'].fillna(df_cell_temp_merged['Satellite Ambient Temp'])

    if ideal_angle_df is not None:
        df_cell_temp_merged = pd.merge(df_cell_temp_merged, ideal_angle_df, on='timestamp', how='left')
        df_cell_temp_merged['Ideal Tilt'] = df_cell_temp_merged['Ideal Tilt'].where(
            df_cell_temp_merged['surface_azimuth'] >= 180, -df_cell_temp_merged['Ideal Tilt']
        )

    cols = ['Satellite GHI', 'Satellite Ambient Temp', 'Satellite POA', 'Satellite Cell']
    if ideal_angle_df is not None:
        cols.append('Ideal Tilt')

    df_satellite = df_cell_temp_merged[['timestamp'] + cols].copy()
    df_satellite['timestamp'] = df_satellite['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df_satellite = df_satellite.rename(columns={'timestamp': 'Start_Dt'})
    df_satellite['System_ID'] = system_id
    df_satellite.dropna(subset=['Satellite Ambient Temp'], inplace=True)
    df_satellite['Satellite POA'] = df_satellite['Satellite POA'].fillna(0)

    melted = df_satellite.melt(id_vars=['Start_Dt', 'System_ID'],
                               var_name='Channel_Type', value_name='Qty')
    

    period_lookup = (
        pd.DataFrame.from_dict(channel_map, orient='index')
        .reset_index()
        .rename(columns={'index': 'Channel_Type'})
    )

    melted = melted.merge(period_lookup, on='Channel_Type', how='left')
    melted = melted.dropna(subset=['Channel_ID'])
    melted['Channel_ID'] = melted['Channel_ID'].astype(int)

    melted['Time_Period_Ind'] = melted['DAS_Time_Period_Ind'].fillna(15).astype(int)


    satellite_df = melted[['Start_Dt', 'Channel_ID', 'Time_Period_Ind', 'Qty']].copy()
    _bulk_upsert_with_merge(engine, perf_target, satellite_df, system_id, name)
    return {"systemId": system_id, "rows": int(len(satellite_df))}

# =================================================================
#  D) SQL MERGE helper (your existing approach, slightly wrapped)
# =================================================================
def _bulk_upsert_with_merge(engine, target_table, df, system_id: int, site_name: str):
    if df.empty:
        logging.info("⚠ No data to upsert for system %s.", system_id)
        return

    df = df.replace({np.nan: None})
    required_cols = ['Start_Dt', 'Channel_ID', 'Time_Period_Ind', 'Qty']
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    rows = df[required_cols].values.tolist()
    staging_table = f"#staging_{target_table.replace('.', '_')}"

    raw_conn = engine.raw_connection()
    try:
        cursor = raw_conn.cursor()
        cursor.fast_executemany = True
        cursor.execute("SET NOCOUNT ON;")

        cursor.execute(f"""
        IF OBJECT_ID('tempdb..{staging_table}') IS NOT NULL
            DROP TABLE {staging_table};

        CREATE TABLE {staging_table} (
            Start_Dt DATETIME2(3),
            Channel_ID INT,
            Time_Period_Ind INT,
            Qty FLOAT
        );
        """)
        cursor.executemany(
            f"INSERT INTO {staging_table} (Start_Dt, Channel_ID, Time_Period_Ind, Qty) VALUES (?, ?, ?, ?)",
            rows
        )

        merge_sql = f"""
        ;WITH src AS (
          SELECT Start_Dt, Channel_ID, Time_Period_Ind, Qty
          FROM {staging_table}
        )
        MERGE {target_table} AS target
        USING src AS source
           ON target.Start_Dt        = source.Start_Dt
          AND target.Channel_ID      = source.Channel_ID
          AND target.Time_Period_Ind = source.Time_Period_Ind
        WHEN MATCHED THEN
          UPDATE SET target.Qty = source.Qty
        WHEN NOT MATCHED BY TARGET THEN
          INSERT (Start_Dt, Channel_ID, Time_Period_Ind, Qty)
          VALUES (source.Start_Dt, source.Channel_ID, source.Time_Period_Ind, source.Qty)
        OUTPUT $action, inserted.Start_Dt, inserted.Channel_ID, inserted.Time_Period_Ind;
        """
        cursor.execute(merge_sql)
        actions = cursor.fetchall()
        upd_count = sum(1 for r in actions if r[0] == 'UPDATE')
        ins_count = sum(1 for r in actions if r[0] == 'INSERT')
        raw_conn.commit()
        logging.info(f"✅ MERGE: site={site_name} ({system_id}) → {upd_count} updated, {ins_count} inserted, input={len(rows)}")
    except Exception as e:
        logging.error(f"❌ MERGE failed for system {system_id}: {e}")
        raw_conn.rollback()
        raise
    finally:
        raw_conn.close()