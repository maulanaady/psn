SELECT * FROM public.radacct
WHERE
  etl_date > TIMESTAMP '{start_time}'
  AND etl_date <= TIMESTAMP '{end_time}';
