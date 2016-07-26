with txns as
(select *, date_part('hour', event_time) as hod, extract(dow from event_time) as dow
, case when next_event_time is null or datediff('day', event_time, next_event_time)>=7 then 1 else 0 end as lapse7
, case when next_event_time is null or datediff('day', event_time, next_event_time)>=14 then 1 else 0 end as lapse14
, case when next_event_time is null or datediff('day', event_time, next_event_time)>=30 then 1 else 0 end as lapse30
from
        (select
        idfa, event_time, event_time::date as date, e_sum::float as rev, e_purchaseamount::float, e_purchaseprice, u_networkid, e_viptier
        , e_purchaseamount::float/e_purchaseprice::float as xrate
        , e_source, e_vip_boost, e_vip_points
        , e_creditsbeforepurchase
        , e_level::bigint
        , e_machine
        , u_playertenure::bigint
        , u_fbstatus
        , u_totalcredits
        , u_totalcredits::float - e_purchaseamount::float as credits
        , row_number() over (partition by idfa order by event_time) as rn
        , dense_rank() over (partition by  idfa order by event_time::date) as rank
        , count(user_id) over (partition by idfa) as txns
        , count(user_id) over (partition by idfa, event_time::date) as txns_on_day
        , dense_rank() over (partition by idfa order by event_time::date desc) rank_desc
        , lead(event_time,1) over (partition by idfa order by event_time) as next_event_time
        , lag(event_time,1) over (partition by idfa order by event_time) as previous_event_time
        from app132763.purchase_verified
        where u_custom_platform = 'iOS' and idfa is not null) x
where event_time between '2016-02-29' and '2016-04-02'
order by idfa, event_time)

, friends as
(select idfa, max(regexp_count(e_friendids, '[0-9]+')) as friends
from app132763.acceptgiftscollectall
where u_custom_platform = 'iOS' and idfa is not null and event_time between '2016-01-01' and '2016-04-01' 
group by 1)

, events as
(select event_type, idfa, date_trunc('minute', event_time) as event_time, u_totalcredits, u_level::bigint, 0 as qws, 0 as spins from app132763.outofcredits  
where u_custom_platform = 'iOS' and idfa is not null and event_time between '2016-01-01' and '2016-04-01' 
group by 1,2,3,4,5,6
union all
select event_type, idfa, event_time, u_totalcredits, e_level::bigint, 0 as qws, 0 as spins from app132763.levelup 
where u_custom_platform = 'iOS' and idfa is not null and event_time between '2016-01-01' and '2016-04-01' 
union all
select event_type, idfa, event_time, u_totalcredits, coalesce(u_level,u_level_1)::bigint as u_level
, (nvl(e_gameplay_bigwin::float,0.0) + nvl(e_gameplay_megawin::float,0.0) + nvl(e_gameplay_epicwin::float,0.0)) as qws 
, regexp_count(event_properties	,'MachineSpinNumber') as spins
from app132763.heartbeat
where u_custom_platform = 'iOS' and idfa is not null and event_time between '2016-01-01' and '2016-04-01' 
union all
select event_type, idfa, event_time, u_totalcredits, u_level::bigint, 0 as qws, 0 as spins from app132763.startsessionplayerinfo
where u_custom_platform = 'iOS' and idfa is not null and event_time between '2016-01-01' and '2016-04-01' 
union all
select event_type, idfa, event_time, u_totalcredits, u_level::bigint, e_sum::float as qws, 0 as spins from app132763.purchase_verified
where u_custom_platform = 'iOS' and idfa is not null and event_time between '2016-01-01' and '2016-04-01'
union all
select event_type, idfa, event_time, u_totalcredits, u_level::bigint, 0 as qws, 0 as spins from app132763.collectdailybonus
where u_custom_platform = 'iOS' and idfa is not null and event_time between '2016-01-01' and '2016-04-01'  )

select t.idfa, t.rn, t.rev, case when t.u_networkid is null then False else True end as "hasEmail", coalesce(f.friends,0) as "fb_friends", t.e_viptier, t.event_time, t.e_purchaseamount, t.credits, t.e_level, datediff('hour', t.event_time, t.next_event_time) as hours_until, datediff('hour', t.event_time,t.previous_event_time) as hours_prior, t.lapse7, t.lapse14, t.lapse30

, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time + interval '5 minutes' and t.event_time + interval '7 days' then e.qws end),0) revNext7Days
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time + interval '5 minutes' and t.event_time + interval '14 days' then e.qws end),0) revNext14Days
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time + interval '5 minutes' and t.event_time + interval '30 days' then e.qws end),0) revNext30Days
, count(case when e.event_type = 'outOfCredits' and e.event_time between t.event_time - interval '56 days' and t.event_time - interval '49 days' then e.idfa end) ooc_56_49d
, count(case when e.event_type = 'outOfCredits' and e.event_time between t.event_time - interval '49 days' and t.event_time - interval '42 days' then e.idfa end) ooc_49_42d
, count(case when e.event_type = 'outOfCredits' and e.event_time between t.event_time - interval '42 days' and t.event_time - interval '35 days' then e.idfa end) ooc_42_35d
, count(case when e.event_type = 'outOfCredits' and e.event_time between t.event_time - interval '35 days' and t.event_time - interval '28 days' then e.idfa end) ooc_35_28d
, count(case when e.event_type = 'outOfCredits' and e.event_time between t.event_time - interval '28 days' and t.event_time - interval '21 days' then e.idfa end) ooc_28_21d
, count(case when e.event_type = 'outOfCredits' and e.event_time between t.event_time - interval '21 days' and t.event_time - interval '14 days' then e.idfa end) ooc_21_14d
, count(case when e.event_type = 'outOfCredits' and e.event_time between t.event_time - interval '14 days' and t.event_time - interval '7 days' then e.idfa end) ooc_14_7d
, count(case when e.event_type = 'outOfCredits' and e.event_time between t.event_time - interval '7 days' and t.event_time - interval '6 days' then e.idfa end) ooc_7_6d
, count(case when e.event_type = 'outOfCredits' and e.event_time between t.event_time - interval '6 days' and t.event_time - interval '5 days' then e.idfa end) ooc_6_5d
, count(case when e.event_type = 'outOfCredits' and e.event_time between t.event_time - interval '5 days' and t.event_time - interval '4 days' then e.idfa end) ooc_5_4d
, count(case when e.event_type = 'outOfCredits' and e.event_time between t.event_time - interval '4 days' and t.event_time - interval '3 days' then e.idfa end) ooc_4_3d
, count(case when e.event_type = 'outOfCredits' and e.event_time between t.event_time - interval '3 days' and t.event_time - interval '2 days' then e.idfa end) ooc_3_2d
, count(case when e.event_type = 'outOfCredits' and e.event_time between t.event_time - interval '2 days' and t.event_time - interval '1 days' then e.idfa end) ooc_2_1d
, count(case when e.event_type = 'outOfCredits' and e.event_time between t.event_time - interval '1 days' and t.event_time - interval '0 days' then e.idfa end) ooc_1_0d

, count(case when e.event_type = 'startSessionPlayerInfo' and e.event_time between t.event_time - interval '56 days' and t.event_time - interval '49 days' then e.idfa end) ss_56_49d
, count(case when e.event_type = 'startSessionPlayerInfo' and e.event_time between t.event_time - interval '49 days' and t.event_time - interval '42 days' then e.idfa end) ss_49_42d
, count(case when e.event_type = 'startSessionPlayerInfo' and e.event_time between t.event_time - interval '42 days' and t.event_time - interval '35 days' then e.idfa end) ss_42_35d
, count(case when e.event_type = 'startSessionPlayerInfo' and e.event_time between t.event_time - interval '35 days' and t.event_time - interval '28 days' then e.idfa end) ss_35_28d
, count(case when e.event_type = 'startSessionPlayerInfo' and e.event_time between t.event_time - interval '28 days' and t.event_time - interval '21 days' then e.idfa end) ss_28_21d
, count(case when e.event_type = 'startSessionPlayerInfo' and e.event_time between t.event_time - interval '21 days' and t.event_time - interval '14 days' then e.idfa end) ss_21_14d
, count(case when e.event_type = 'startSessionPlayerInfo' and e.event_time between t.event_time - interval '14 days' and t.event_time - interval '7 days' then e.idfa end) ss_14_7d
, count(case when e.event_type = 'startSessionPlayerInfo' and e.event_time between t.event_time - interval '7 days' and t.event_time - interval '6 days' then e.idfa end) ss_7_6d
, count(case when e.event_type = 'startSessionPlayerInfo' and e.event_time between t.event_time - interval '6 days' and t.event_time - interval '5 days' then e.idfa end) ss_6_5d
, count(case when e.event_type = 'startSessionPlayerInfo' and e.event_time between t.event_time - interval '5 days' and t.event_time - interval '4 days' then e.idfa end) ss_5_4d
, count(case when e.event_type = 'startSessionPlayerInfo' and e.event_time between t.event_time - interval '4 days' and t.event_time - interval '3 days' then e.idfa end) ss_4_3d
, count(case when e.event_type = 'startSessionPlayerInfo' and e.event_time between t.event_time - interval '3 days' and t.event_time - interval '2 days' then e.idfa end) ss_3_2d
, count(case when e.event_type = 'startSessionPlayerInfo' and e.event_time between t.event_time - interval '2 days' and t.event_time - interval '1 days' then e.idfa end) ss_2_1d
, count(case when e.event_type = 'startSessionPlayerInfo' and e.event_time between t.event_time - interval '1 days' and t.event_time - interval '0 days' then e.idfa end) ss_1_0d

, count(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '56 days' and t.event_time - interval '49 days' then e.idfa end) hb_56_49d
, count(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '49 days' and t.event_time - interval '42 days' then e.idfa end) hb_49_42d
, count(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '42 days' and t.event_time - interval '35 days' then e.idfa end) hb_42_35d
, count(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '35 days' and t.event_time - interval '28 days' then e.idfa end) hb_35_28d
, count(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '28 days' and t.event_time - interval '21 days' then e.idfa end) hb_28_21d
, count(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '21 days' and t.event_time - interval '14 days' then e.idfa end) hb_21_14d
, count(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '14 days' and t.event_time - interval '7 days' then e.idfa end) hb_14_7d
, count(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '7 days' and t.event_time - interval '6 days' then e.idfa end) hb_7_6d
, count(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '6 days' and t.event_time - interval '5 days' then e.idfa end) hb_6_5d
, count(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '5 days' and t.event_time - interval '4 days' then e.idfa end) hb_5_4d
, count(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '4 days' and t.event_time - interval '3 days' then e.idfa end) hb_4_3d
, count(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '3 days' and t.event_time - interval '2 days' then e.idfa end) hb_3_2d
, count(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '2 days' and t.event_time - interval '1 days' then e.idfa end) hb_2_1d
, count(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '1 days' and t.event_time - interval '0 days' then e.idfa end) hb_1_0d

, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '56 days' and t.event_time - interval '49 days' then e.qws end), 0) qw_56_49d
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '49 days' and t.event_time - interval '42 days' then e.qws end), 0) qw_49_42d
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '42 days' and t.event_time - interval '35 days' then e.qws end), 0) qw_42_35d
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '35 days' and t.event_time - interval '28 days' then e.qws end), 0) qw_35_28d
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '28 days' and t.event_time - interval '21 days' then e.qws end), 0) qw_28_21d
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '21 days' and t.event_time - interval '14 days' then e.qws end), 0) qw_21_14d
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '14 days' and t.event_time - interval '7 days' then e.qws end), 0) qw_14_7d
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '7 days' and t.event_time - interval '6 days' then e.qws end), 0) qw_7_6d
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '6 days' and t.event_time - interval '5 days' then e.qws end), 0) qw_6_5d
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '5 days' and t.event_time - interval '4 days' then e.qws end), 0) qw_5_4d
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '4 days' and t.event_time - interval '3 days' then e.qws end), 0) qw_4_3d
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '3 days' and t.event_time - interval '2 days' then e.qws end), 0) qw_3_2d
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '2 days' and t.event_time - interval '1 days' then e.qws end), 0) qw_2_1d
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '1 days' and t.event_time - interval '0 days' then e.qws end), 0) qw_1_0d

, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '56 days' and t.event_time - interval '49 days' then e.spins end), 0) sp_56_49d
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '49 days' and t.event_time - interval '42 days' then e.spins end), 0) sp_49_42d
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '42 days' and t.event_time - interval '35 days' then e.spins end), 0) sp_42_35d
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '35 days' and t.event_time - interval '28 days' then e.spins end), 0) sp_35_28d
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '28 days' and t.event_time - interval '21 days' then e.spins end), 0) sp_28_21d
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '21 days' and t.event_time - interval '14 days' then e.spins end), 0) sp_21_14d
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '14 days' and t.event_time - interval '7 days' then e.spins end), 0) sp_14_7d
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '7 days' and t.event_time - interval '6 days' then e.spins end), 0) sp_7_6d
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '6 days' and t.event_time - interval '5 days' then e.spins end), 0) sp_6_5d
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '5 days' and t.event_time - interval '4 days' then e.spins end), 0) sp_5_4d
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '4 days' and t.event_time - interval '3 days' then e.spins end), 0) sp_4_3d
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '3 days' and t.event_time - interval '2 days' then e.spins end), 0) sp_3_2d
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '2 days' and t.event_time - interval '1 days' then e.spins end), 0) sp_2_1d
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '1 days' and t.event_time - interval '0 days' then e.spins end), 0) sp_1_0d

, count(case when e.event_type = 'levelUp' and e.event_time between t.event_time - interval '56 days' and t.event_time - interval '49 days' then e.idfa end) lu_56_49d
, count(case when e.event_type = 'levelUp' and e.event_time between t.event_time - interval '49 days' and t.event_time - interval '42 days' then e.idfa end) lu_49_42d
, count(case when e.event_type = 'levelUp' and e.event_time between t.event_time - interval '42 days' and t.event_time - interval '35 days' then e.idfa end) lu_42_35d
, count(case when e.event_type = 'levelUp' and e.event_time between t.event_time - interval '35 days' and t.event_time - interval '28 days' then e.idfa end) lu_35_28d
, count(case when e.event_type = 'levelUp' and e.event_time between t.event_time - interval '28 days' and t.event_time - interval '21 days' then e.idfa end) lu_28_21d
, count(case when e.event_type = 'levelUp' and e.event_time between t.event_time - interval '21 days' and t.event_time - interval '14 days' then e.idfa end) lu_21_14d
, count(case when e.event_type = 'levelUp' and e.event_time between t.event_time - interval '14 days' and t.event_time - interval '7 days' then e.idfa end) lu_14_7d
, count(case when e.event_type = 'levelUp' and e.event_time between t.event_time - interval '7 days' and t.event_time - interval '6 days' then e.idfa end) lu_7_6d
, count(case when e.event_type = 'levelUp' and e.event_time between t.event_time - interval '6 days' and t.event_time - interval '5 days' then e.idfa end) lu_6_5d
, count(case when e.event_type = 'levelUp' and e.event_time between t.event_time - interval '5 days' and t.event_time - interval '4 days' then e.idfa end) lu_5_4d
, count(case when e.event_type = 'levelUp' and e.event_time between t.event_time - interval '4 days' and t.event_time - interval '3 days' then e.idfa end) lu_4_3d
, count(case when e.event_type = 'levelUp' and e.event_time between t.event_time - interval '3 days' and t.event_time - interval '2 days' then e.idfa end) lu_3_2d
, count(case when e.event_type = 'levelUp' and e.event_time between t.event_time - interval '2 days' and t.event_time - interval '1 days' then e.idfa end) lu_2_1d
, count(case when e.event_type = 'levelUp' and e.event_time between t.event_time - interval '1 days' and t.event_time - interval '0 days' then e.idfa end) lu_1_0d

, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '56 days' and t.event_time - interval '49 days' then e.idfa end) pv_56_49d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '49 days' and t.event_time - interval '42 days' then e.idfa end) pv_49_42d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '42 days' and t.event_time - interval '35 days' then e.idfa end) pv_42_35d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '35 days' and t.event_time - interval '28 days' then e.idfa end) pv_35_28d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '28 days' and t.event_time - interval '21 days' then e.idfa end) pv_28_21d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '21 days' and t.event_time - interval '14 days' then e.idfa end) pv_21_14d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '14 days' and t.event_time - interval '7 days' then e.idfa end) pv_14_7d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '7 days' and t.event_time - interval '6 days' then e.idfa end) pv_7_6d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '6 days' and t.event_time - interval '5 days' then e.idfa end) pv_6_5d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '5 days' and t.event_time - interval '4 days' then e.idfa end) pv_5_4d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '4 days' and t.event_time - interval '3 days' then e.idfa end) pv_4_3d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '3 days' and t.event_time - interval '2 days' then e.idfa end) pv_3_2d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '2 days' and t.event_time - interval '1 days' then e.idfa end) pv_2_1d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '1 days' and t.event_time - interval '0 days' then e.idfa end) pv_1_0d

, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '8 days' and t.event_time - interval '7 days' then e.idfa end) pv_8_7d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '9 days' and t.event_time - interval '8 days' then e.idfa end) pv_9_8d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '10 days' and t.event_time - interval '9 days' then e.idfa end) pv_10_9d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '11 days' and t.event_time - interval '10 days' then e.idfa end) pv_11_10d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '12 days' and t.event_time - interval '11 days' then e.idfa end) pv_12_11d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '13 days' and t.event_time - interval '12 days' then e.idfa end) pv_13_12d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '14 days' and t.event_time - interval '13 days' then e.idfa end) pv_14_13d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '15 days' and t.event_time - interval '14 days' then e.idfa end) pv_15_14d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '16 days' and t.event_time - interval '15 days' then e.idfa end) pv_16_15d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '17 days' and t.event_time - interval '16 days' then e.idfa end) pv_17_16d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '18 days' and t.event_time - interval '17 days' then e.idfa end) pv_18_17d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '19 days' and t.event_time - interval '18 days' then e.idfa end) pv_19_18d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '20 days' and t.event_time - interval '19 days' then e.idfa end) pv_20_19d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '21 days' and t.event_time - interval '20 days' then e.idfa end) pv_21_20d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '22 days' and t.event_time - interval '21 days' then e.idfa end) pv_22_21d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '23 days' and t.event_time - interval '22 days' then e.idfa end) pv_23_22d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '24 days' and t.event_time - interval '23 days' then e.idfa end) pv_24_23d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '25 days' and t.event_time - interval '24 days' then e.idfa end) pv_25_24d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '26 days' and t.event_time - interval '25 days' then e.idfa end) pv_26_25d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '27 days' and t.event_time - interval '26 days' then e.idfa end) pv_27_26d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '28 days' and t.event_time - interval '27 days' then e.idfa end) pv_28_27d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '29 days' and t.event_time - interval '28 days' then e.idfa end) pv_29_28d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '30 days' and t.event_time - interval '29 days' then e.idfa end) pv_30_29d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '31 days' and t.event_time - interval '30 days' then e.idfa end) pv_31_30d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '32 days' and t.event_time - interval '31 days' then e.idfa end) pv_32_31d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '33 days' and t.event_time - interval '32 days' then e.idfa end) pv_33_32d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '34 days' and t.event_time - interval '33 days' then e.idfa end) pv_34_33d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '35 days' and t.event_time - interval '34 days' then e.idfa end) pv_35_34d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '36 days' and t.event_time - interval '35 days' then e.idfa end) pv_36_35d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '37 days' and t.event_time - interval '36 days' then e.idfa end) pv_37_36d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '38 days' and t.event_time - interval '37 days' then e.idfa end) pv_38_37d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '39 days' and t.event_time - interval '38 days' then e.idfa end) pv_39_38d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '40 days' and t.event_time - interval '39 days' then e.idfa end) pv_40_39d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '41 days' and t.event_time - interval '40 days' then e.idfa end) pv_41_40d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '42 days' and t.event_time - interval '41 days' then e.idfa end) pv_42_41d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '43 days' and t.event_time - interval '42 days' then e.idfa end) pv_43_42d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '44 days' and t.event_time - interval '43 days' then e.idfa end) pv_44_43d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '45 days' and t.event_time - interval '44 days' then e.idfa end) pv_45_44d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '46 days' and t.event_time - interval '45 days' then e.idfa end) pv_46_45d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '47 days' and t.event_time - interval '46 days' then e.idfa end) pv_47_46d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '48 days' and t.event_time - interval '47 days' then e.idfa end) pv_48_47d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '49 days' and t.event_time - interval '48 days' then e.idfa end) pv_49_48d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '50 days' and t.event_time - interval '49 days' then e.idfa end) pv_50_49d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '51 days' and t.event_time - interval '50 days' then e.idfa end) pv_51_50d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '52 days' and t.event_time - interval '51 days' then e.idfa end) pv_52_51d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '53 days' and t.event_time - interval '52 days' then e.idfa end) pv_53_52d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '54 days' and t.event_time - interval '53 days' then e.idfa end) pv_54_53d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '55 days' and t.event_time - interval '54 days' then e.idfa end) pv_55_54d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '56 days' and t.event_time - interval '55 days' then e.idfa end) pv_56_55d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '57 days' and t.event_time - interval '56 days' then e.idfa end) pv_57_56d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '58 days' and t.event_time - interval '57 days' then e.idfa end) pv_58_57d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '59 days' and t.event_time - interval '58 days' then e.idfa end) pv_59_58d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '60 days' and t.event_time - interval '59 days' then e.idfa end) pv_60_59d

, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '56 days' and t.event_time - interval '49 days' then e.qws end),0) rev_56_49d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '49 days' and t.event_time - interval '42 days' then e.qws end),0) rev_49_42d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '42 days' and t.event_time - interval '35 days' then e.qws end),0) rev_42_35d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '35 days' and t.event_time - interval '28 days' then e.qws end),0) rev_35_28d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '28 days' and t.event_time - interval '21 days' then e.qws end),0) rev_28_21d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '21 days' and t.event_time - interval '14 days' then e.qws end),0) rev_21_14d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '14 days' and t.event_time - interval '7 days' then e.qws end),0) rev_14_7d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '7 days' and t.event_time - interval '6 days' then e.qws end),0) rev_7_6d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '6 days' and t.event_time - interval '5 days' then e.qws end),0) rev_6_5d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '5 days' and t.event_time - interval '4 days' then e.qws end),0) rev_5_4d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '4 days' and t.event_time - interval '3 days' then e.qws end),0) rev_4_3d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '3 days' and t.event_time - interval '2 days' then e.qws end),0) rev_3_2d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '2 days' and t.event_time - interval '1 days' then e.qws end),0) rev_2_1d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '1 days' and t.event_time - interval '0 days' then e.qws end),0) rev_1_0d

, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '8 days' and t.event_time - interval '7 days' then e.qws end),0) rev_8_7d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '9 days' and t.event_time - interval '8 days' then e.qws end),0) rev_9_8d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '10 days' and t.event_time - interval '9 days' then e.qws end),0) rev_10_9d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '11 days' and t.event_time - interval '10 days' then e.qws end),0) rev_11_10d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '12 days' and t.event_time - interval '11 days' then e.qws end),0) rev_12_11d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '13 days' and t.event_time - interval '12 days' then e.qws end),0) rev_13_12d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '14 days' and t.event_time - interval '13 days' then e.qws end),0) rev_14_13d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '15 days' and t.event_time - interval '14 days' then e.qws end),0) rev_15_14d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '16 days' and t.event_time - interval '15 days' then e.qws end),0) rev_16_15d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '17 days' and t.event_time - interval '16 days' then e.qws end),0) rev_17_16d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '18 days' and t.event_time - interval '17 days' then e.qws end),0) rev_18_17d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '19 days' and t.event_time - interval '18 days' then e.qws end),0) rev_19_18d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '20 days' and t.event_time - interval '19 days' then e.qws end),0) rev_20_19d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '21 days' and t.event_time - interval '20 days' then e.qws end),0) rev_21_20d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '22 days' and t.event_time - interval '21 days' then e.qws end),0) rev_22_21d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '23 days' and t.event_time - interval '22 days' then e.qws end),0) rev_23_22d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '24 days' and t.event_time - interval '23 days' then e.qws end),0) rev_24_23d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '25 days' and t.event_time - interval '24 days' then e.qws end),0) rev_25_24d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '26 days' and t.event_time - interval '25 days' then e.qws end),0) rev_26_25d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '27 days' and t.event_time - interval '26 days' then e.qws end),0) rev_27_26d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '28 days' and t.event_time - interval '27 days' then e.qws end),0) rev_28_27d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '29 days' and t.event_time - interval '28 days' then e.qws end),0) rev_29_28d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '30 days' and t.event_time - interval '29 days' then e.qws end),0) rev_30_29d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '31 days' and t.event_time - interval '30 days' then e.qws end),0) rev_31_30d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '32 days' and t.event_time - interval '31 days' then e.qws end),0) rev_32_31d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '33 days' and t.event_time - interval '32 days' then e.qws end),0) rev_33_32d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '34 days' and t.event_time - interval '33 days' then e.qws end),0) rev_34_33d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '35 days' and t.event_time - interval '34 days' then e.qws end),0) rev_35_34d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '36 days' and t.event_time - interval '35 days' then e.qws end),0) rev_36_35d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '37 days' and t.event_time - interval '36 days' then e.qws end),0) rev_37_36d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '38 days' and t.event_time - interval '37 days' then e.qws end),0) rev_38_37d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '39 days' and t.event_time - interval '38 days' then e.qws end),0) rev_39_38d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '40 days' and t.event_time - interval '39 days' then e.qws end),0) rev_40_39d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '41 days' and t.event_time - interval '40 days' then e.qws end),0) rev_41_40d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '42 days' and t.event_time - interval '41 days' then e.qws end),0) rev_42_41d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '43 days' and t.event_time - interval '42 days' then e.qws end),0) rev_43_42d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '44 days' and t.event_time - interval '43 days' then e.qws end),0) rev_44_43d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '45 days' and t.event_time - interval '44 days' then e.qws end),0) rev_45_44d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '46 days' and t.event_time - interval '45 days' then e.qws end),0) rev_46_45d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '47 days' and t.event_time - interval '46 days' then e.qws end),0) rev_47_46d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '48 days' and t.event_time - interval '47 days' then e.qws end),0) rev_48_47d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '49 days' and t.event_time - interval '48 days' then e.qws end),0) rev_49_48d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '50 days' and t.event_time - interval '49 days' then e.qws end),0) rev_50_49d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '51 days' and t.event_time - interval '50 days' then e.qws end),0) rev_51_50d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '52 days' and t.event_time - interval '51 days' then e.qws end),0) rev_52_51d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '53 days' and t.event_time - interval '52 days' then e.qws end),0) rev_53_52d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '54 days' and t.event_time - interval '53 days' then e.qws end),0) rev_54_53d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '55 days' and t.event_time - interval '54 days' then e.qws end),0) rev_55_54d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '56 days' and t.event_time - interval '55 days' then e.qws end),0) rev_56_55d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '57 days' and t.event_time - interval '56 days' then e.qws end),0) rev_57_56d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '58 days' and t.event_time - interval '57 days' then e.qws end),0) rev_58_57d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '59 days' and t.event_time - interval '58 days' then e.qws end),0) rev_59_58d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '60 days' and t.event_time - interval '59 days' then e.qws end),0) rev_60_59d

, count(case when e.event_type = 'collectDailyBonus' and e.event_time between t.event_time - interval '56 days' and t.event_time - interval '49 days' then e.idfa end) chb_56_49d
, count(case when e.event_type = 'collectDailyBonus' and e.event_time between t.event_time - interval '49 days' and t.event_time - interval '42 days' then e.idfa end) chb_49_42d
, count(case when e.event_type = 'collectDailyBonus' and e.event_time between t.event_time - interval '42 days' and t.event_time - interval '35 days' then e.idfa end) chb_42_35d
, count(case when e.event_type = 'collectDailyBonus' and e.event_time between t.event_time - interval '35 days' and t.event_time - interval '28 days' then e.idfa end) chb_35_28d
, count(case when e.event_type = 'collectDailyBonus' and e.event_time between t.event_time - interval '28 days' and t.event_time - interval '21 days' then e.idfa end) chb_28_21d
, count(case when e.event_type = 'collectDailyBonus' and e.event_time between t.event_time - interval '21 days' and t.event_time - interval '14 days' then e.idfa end) chb_21_14d
, count(case when e.event_type = 'collectDailyBonus' and e.event_time between t.event_time - interval '14 days' and t.event_time - interval '7 days' then e.idfa end) chb_14_7d
, count(case when e.event_type = 'collectDailyBonus' and e.event_time between t.event_time - interval '7 days' and t.event_time - interval '6 days' then e.idfa end) chb_7_6d
, count(case when e.event_type = 'collectDailyBonus' and e.event_time between t.event_time - interval '6 days' and t.event_time - interval '5 days' then e.idfa end) chb_6_5d
, count(case when e.event_type = 'collectDailyBonus' and e.event_time between t.event_time - interval '5 days' and t.event_time - interval '4 days' then e.idfa end) chb_5_4d
, count(case when e.event_type = 'collectDailyBonus' and e.event_time between t.event_time - interval '4 days' and t.event_time - interval '3 days' then e.idfa end) chb_4_3d
, count(case when e.event_type = 'collectDailyBonus' and e.event_time between t.event_time - interval '3 days' and t.event_time - interval '2 days' then e.idfa end) chb_3_2d
, count(case when e.event_type = 'collectDailyBonus' and e.event_time between t.event_time - interval '2 days' and t.event_time - interval '1 days' then e.idfa end) chb_2_1d
, count(case when e.event_type = 'collectDailyBonus' and e.event_time between t.event_time - interval '1 days' and t.event_time - interval '0 days' then e.idfa end) chb_1_0d


from txns t
join events e on t.idfa = e.idfa
left join friends f on t.idfa=f.idfa
where t.event_time between '2016-03-01' and '2016-04-01'
group by t.idfa, t.rn, t.rev, "hasEmail", "fb_friends", t.u_networkid, t.e_viptier, t.event_time,t.idfa, t.rn, t.u_networkid, t.e_viptier, t.event_time,  t.e_purchaseamount, t.credits, t.e_level, datediff('hour', t.event_time, t.next_event_time), datediff('hour', t.event_time,t.previous_event_time), t.lapse7, t.lapse14, t.lapse30
order by random()
