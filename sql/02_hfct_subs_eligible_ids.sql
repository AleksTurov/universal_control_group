WITH
    toDate(:DATA_START) AS data_start,
    addDays(addMonths(data_start, 1), -1) AS data_end
SELECT DISTINCT hs.subscription_id
FROM DWH.hfct_subs_short hs
WHERE 1 = 1
  AND hs.eff_dt = data_end -- конец месяца
  AND toDate(hs.subs_termination_time) > data_start -- подписка не завершена на начало месяца
  AND dictGetOrDefault(dict.dict_msisdn_operator, 'operator', hs.subs_msisdn, 'beeline') <> 'IZI' -- не IZI
  AND hs.subs_category NOT ILIKE '%employee%' -- не сотрудник
  AND hs.subs_category NOT ILIKE '%test%'  -- не тестовый
  AND hs.subscription_id GLOBAL NOT IN ( -- исключаем M2M
      SELECT DISTINCT hsp.subscription_id
      FROM DWH.hfct_subs_packs hsp
      WHERE hsp.package_code = 'M2M'
        AND (hsp.subspack_deactivated_at IS NULL -- пакет не деактивирован
          OR hsp.subspack_deactivated_at > data_end) -- или деактивирован после начала месяца
  )
