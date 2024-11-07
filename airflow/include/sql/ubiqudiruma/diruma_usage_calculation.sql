CREATE OR REPLACE TABLE stg_radacct AS
WITH
  main_df AS (
    SELECT *
    FROM
      read_parquet(
        '/opt/airflow/data/ubiqudiruma/{start_time}_{end_time}.parquet'
      )
  )

SELECT
  acctsessionid,
  acctuniqueid,
  concat(acctuniqueid, ';', username, ';', acctstarttime) AS adj_acctuniqueid,
  username,
  groupname,
  realm,
  acctstarttime,
  acctupdatetime,
  acctstoptime,
  if(
    cast(acctstarttime AS TIMESTAMP)
    >= coalesce(
      cast(acctstoptime AS TIMESTAMP), TIMESTAMP '9999-12-31 23:59:59'
    )
    AND acctsessiontime > 0,
    acctstarttime + (acctsessiontime * INTERVAL '1 second'),
    acctstoptime
  )                                                       AS adj_acctstoptime,
  if(
    cast(acctstarttime AS TIMESTAMP)
    >= coalesce(
      cast(acctstoptime AS TIMESTAMP), TIMESTAMP '9999-12-31 23:59:59'
    )
    AND acctsessiontime > 0,
    1,
    0
  )                                                       AS is_time_adjusted,
  acctinterval,
  acctsessiontime,
  if(
    cast(acctstarttime AS TIMESTAMP)
    >= coalesce(
      cast(acctstoptime AS TIMESTAMP), TIMESTAMP '9999-12-31 23:59:59'
    )
    AND acctsessiontime = 0,
    abs(extract(EPOCH FROM age(acctstarttime, acctstoptime))),
    acctsessiontime
  )                                                       AS adj_acctsessiontime,
  if(
    cast(acctstarttime AS TIMESTAMP)
    >= coalesce(
      cast(acctstoptime AS TIMESTAMP), TIMESTAMP '9999-12-31 23:59:59'
    )
    AND acctsessiontime = 0,
    1,
    0
  )                                                       AS is_sessiontime_adjusted,
  acctinputoctets,
  acctoutputoctets,
  calledstationid,
  acctstartdelay,
  acctstopdelay,
  extract_date,
  md5(
    concat(
      coalesce(
        cast(acctstarttime AS TIMESTAMP),
        TIMESTAMP '9999-12-31 23:59:59'
      ),
      ';',
      coalesce(acctsessiontime, 0), ';',
      coalesce(acctinputoctets, 0), ';',
      coalesce(acctoutputoctets, 0), ';',
      username, ';',
      acctuniqueid
    )
  )                                                       AS stg_hashdiff
FROM main_df;
CREATE OR REPLACE TABLE joined AS
WITH
  b AS (
    SELECT *
    FROM read_parquet('/opt/airflow/data/ubiqudiruma/ubiqudiruma.parquet')
  )

SELECT
  a.acctuniqueid,
  a.username,
  a.acctinputoctets,
  a.acctoutputoctets,
  a.extract_date
FROM stg_radacct AS a
INNER JOIN
  b
  ON
  a.acctuniqueid = b.acctuniqueid
  AND a.username = b.username
  AND a.acctinputoctets = b.acctinputoctets
  AND a.acctoutputoctets = b.acctoutputoctets
QUALIFY row_number()
  OVER
  (
    PARTITION BY a.username, a.acctstarttime, a.acctuniqueid
    ORDER BY
      coalesce(
        cast(b.extract_date AS TIMESTAMP),
        TIMESTAMP '9999-12-31 23:59:59'
      ) DESC
  )
= 1;
CREATE OR REPLACE TABLE cleaned AS
SELECT a.*
FROM stg_radacct AS a
WHERE NOT EXISTS (
  SELECT 1
  FROM joined AS b
  WHERE a.acctuniqueid = b.acctuniqueid
);
CREATE OR REPLACE TABLE track_radacct AS
WITH
  c AS (
    SELECT *
    FROM read_parquet('/opt/airflow/data/ubiqudiruma/ubiqudiruma.parquet')
  )

SELECT
  s.*,
  CASE WHEN c.acctuniqueid IS null THEN 1 ELSE 0 END AS new_ind,
  CASE
    WHEN
      c.acctuniqueid IS NOT null
      AND s.stg_hashdiff != c.hashdiff
    THEN 1
    ELSE 0
  END                                                AS track_ind
FROM
  cleaned AS s
LEFT JOIN
  c
  ON
  s.username = c.username
  AND s.acctstarttime = c.acctstarttime
  AND s.acctuniqueid = c.acctuniqueid
QUALIFY
  row_number()
    OVER (
      PARTITION BY s.username, s.acctstarttime, s.acctuniqueid
      ORDER BY
        coalesce(cast(c.extract_date AS TIMESTAMP), TIMESTAMP '9999-12-31 23:59:59') DESC
    )
  = 1;
CREATE OR REPLACE TABLE newid AS
WITH
  tmp AS (
    SELECT
      acctsessionid,
      acctuniqueid,
      adj_acctuniqueid,
      username,
      groupname,
      realm,
      acctstarttime,
      lag(acctstarttime)
        OVER (
          PARTITION BY username
          ORDER BY acctstarttime ASC, extract_date ASC
        )
        AS prev_acctstarttime,
      acctstoptime,
      adj_acctstoptime,
      lag(adj_acctstoptime)
        OVER (
          PARTITION BY username
          ORDER BY acctstarttime ASC, extract_date ASC
        )
        AS prev_adj_acctstoptime,
      is_time_adjusted,
      acctupdatetime,
      acctsessiontime,
      adj_acctsessiontime,
      is_sessiontime_adjusted,
      acctinputoctets,
      acctoutputoctets,
      calledstationid,
      stg_hashdiff                                                           AS hashdiff,
      lag(acctinputoctets)
        OVER (PARTITION BY username, adj_acctuniqueid ORDER BY extract_date)
        AS prev_acctinputoctets,
      lag(acctoutputoctets)
        OVER (PARTITION BY username, adj_acctuniqueid ORDER BY extract_date)
        AS prev_acctoutputoctets,
      extract_date,
      coalesce(
        if(
          acctinputoctets - prev_acctinputoctets >= 0,
          acctinputoctets - prev_acctinputoctets,
          acctinputoctets
        ),
        acctinputoctets
      )                                                                      AS diff_acctinputoctets,
      coalesce(
        if(
          acctoutputoctets - prev_acctoutputoctets >= 0,
          acctoutputoctets - prev_acctoutputoctets,
          acctoutputoctets
        ),
        acctoutputoctets
      )                                                                      AS diff_acctoutputoctets
    FROM track_radacct
    WHERE new_ind = 1
    ORDER BY username, acctstarttime, extract_date
  )

SELECT *
FROM tmp
ORDER BY username, acctstarttime, extract_date;
CREATE OR REPLACE TABLE trackedid AS
WITH
  tmp AS (
    SELECT
      t.acctsessionid,
      t.acctuniqueid,
      t.adj_acctuniqueid,
      t.username,
      t.groupname,
      t.realm,
      t.acctstarttime,
      f.acctstarttime    AS prev_acctstarttime,
      t.acctstoptime,
      t.adj_acctstoptime,
      f.adj_acctstoptime AS prev_adj_acctstoptime,
      t.is_time_adjusted,
      t.acctupdatetime,
      t.acctsessiontime,
      t.adj_acctsessiontime,
      t.is_sessiontime_adjusted,
      t.acctinputoctets,
      t.acctoutputoctets,
      t.calledstationid,
      t.stg_hashdiff     AS hashdiff,
      f.acctinputoctets  AS prev_acctinputoctets,
      f.acctoutputoctets AS prev_acctoutputoctets,
      t.extract_date,
      coalesce(
        if(
          t.acctinputoctets - f.acctinputoctets >= 0,
          t.acctinputoctets - f.acctinputoctets,
          t.acctinputoctets
        ),
        t.acctinputoctets
      )                  AS diff_acctinputoctets,
      coalesce(
        if(
          t.acctoutputoctets - f.acctoutputoctets >= 0,
          t.acctoutputoctets - f.acctoutputoctets,
          t.acctoutputoctets
        ),
        t.acctoutputoctets
      )                  AS diff_acctoutputoctets
    FROM track_radacct AS t
    LEFT JOIN
      f
      ON
      t.username = f.username
      AND t.acctstarttime = f.acctstarttime
      AND t.acctuniqueid = f.acctuniqueid
    WHERE t.track_ind = 1
    QUALIFY
      row_number()
        OVER (
          PARTITION BY t.username, t.acctstarttime, t.acctuniqueid
          ORDER BY
            coalesce(
              cast(f.extract_date AS TIMESTAMP),
              TIMESTAMP '9999-12-31 23:59:59'
            ) DESC
        )
      = 1
    ORDER BY t.username, t.acctstarttime, t.extract_date
  ),

  f AS (
    SELECT *
    FROM read_parquet('/opt/airflow/data/ubiqudiruma/ubiqudiruma.parquet')
  )

SELECT *
FROM tmp
ORDER BY username, acctstarttime, extract_date;

CREATE TABLE diruma_usage AS
SELECT * FROM read_parquet('/opt/airflow/data/ubiqudiruma/ubiqudiruma.parquet');

INSERT INTO diruma_usage SELECT * FROM newid;

INSERT INTO diruma_usage SELECT * FROM trackedid;

COPY (SELECT * FROM diruma_usage)
TO '/opt/airflow/data/ubiqudiruma/ubiqudiruma.parquet'
(FORMAT 'parquet', COMPRESSION 'zstd', ROW_GROUP_SIZE 100000);

COPY (SELECT acctsessionid,
    acctuniqueid,
    adj_acctuniqueid,
    username,
    realm,
    acctstarttime,
    acctinputoctets,
    acctoutputoctets,
    diff_acctinputoctets,
    diff_acctoutputoctets,
    extract_date
    FROM
    (SELECT * FROM newid
    UNION ALL SELECT * FROM trackedid))
    TO '/opt/airflow/data/ubiqudiruma/ubiqudiruma_analysis.parquet'
    (FORMAT 'parquet', COMPRESSION 'zstd', ROW_GROUP_SIZE 100000);
