select *
from DWH.hfct_subs_short hs
where 1=1
  --and contract_status IN (1,4) -- active and suspended
  and eff_dt = today() -1 -- yesterday's data
  and toDate(subs_termination_time) > now() -- active, contract time > now
  and dictGetOrDefault(dict.dict_msisdn_operator, 'operator', subs_msisdn, 'beeline') <> 'IZI' -- not izi
  and subs_category not ilike '%employee%' -- not employee
  and subs_category not ilike '%test%'  -- not test
  and subscription_id global not in (select distinct subscription_id
                                      from DWH.hfct_subs_packs hsp
                                      WHERE package_code IN ('M2M')  -- not M2M
                                       and (subspack_deactivated_at is null or subspack_deactivated_at > now()))
--