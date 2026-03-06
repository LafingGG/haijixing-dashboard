BEGIN;

CREATE TABLE IF NOT EXISTS app_settings (
  key   TEXT PRIMARY KEY,
  value TEXT
);

CREATE TABLE IF NOT EXISTS snapshots (
  snapshot_id TEXT PRIMARY KEY,
  created_at  TEXT,
  created_by  TEXT,
  status      TEXT,
  note        TEXT
);

CREATE TABLE IF NOT EXISTS etl_import_log (
  import_id     TEXT PRIMARY KEY,
  snapshot_id   TEXT,
  filename      TEXT,
  uploaded_at   TEXT,
  rows_upserted INTEGER,
  status        TEXT,
  error_message TEXT
);

-- 新表：增加 snapshot_id，并保留 source_sheet 字段
CREATE TABLE IF NOT EXISTS fact_daily_ops_v2 (
  snapshot_id TEXT NOT NULL,
  date TEXT NOT NULL,

  incoming_trips REAL,
  incoming_ton REAL,
  slag_trips REAL,
  slag_ton REAL,
  slag_total_ton REAL,
  slurry_m3 REAL,
  water_meter_m3 REAL,
  water_m3 REAL,
  elec_meter_x1e3kwh REAL,
  elec_meter_kwh REAL,
  proj_flow_m3 REAL,
  to_wwtp_m3 REAL,
  wwtp_flow_m3 REAL,
  arrive_wwtp_m3 REAL,
  source_sheet TEXT,

  PRIMARY KEY(snapshot_id, date)
);

-- 把旧数据搬进 legacy 快照
INSERT OR IGNORE INTO fact_daily_ops_v2 (
  snapshot_id, date,
  incoming_trips, incoming_ton,
  slag_trips, slag_ton, slag_total_ton,
  slurry_m3, water_meter_m3, water_m3,
  elec_meter_x1e3kwh, elec_meter_kwh,
  proj_flow_m3, to_wwtp_m3, wwtp_flow_m3, arrive_wwtp_m3,
  source_sheet
)
SELECT
  'legacy' AS snapshot_id,
  date,
  incoming_trips, incoming_ton,
  slag_trips, slag_ton, slag_total_ton,
  slurry_m3, water_meter_m3, water_m3,
  elec_meter_x1e3kwh, elec_meter_kwh,
  proj_flow_m3, to_wwtp_m3, wwtp_flow_m3, arrive_wwtp_m3,
  source_sheet
FROM fact_daily_ops;

-- 替换旧表（保留备份）
ALTER TABLE fact_daily_ops RENAME TO fact_daily_ops_old;
ALTER TABLE fact_daily_ops_v2 RENAME TO fact_daily_ops;

-- 设置 active/staging
INSERT OR REPLACE INTO app_settings(key, value) VALUES ('active_snapshot_id', 'legacy');
INSERT OR REPLACE INTO app_settings(key, value) VALUES ('staging_snapshot_id', 'legacy');

INSERT OR REPLACE INTO snapshots(snapshot_id, created_at, created_by, status, note)
VALUES ('legacy', datetime('now'), 'migration', 'published', 'migrated from old schema');

COMMIT;
