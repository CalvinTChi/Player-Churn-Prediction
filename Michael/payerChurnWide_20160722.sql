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
