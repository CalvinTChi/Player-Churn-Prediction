with txns as
(select *, date_part('hour', event_time) as hod, extract(dow from event_time) as dow, case when next_event_time is null or datediff('day', event_time, next_event_time)>=7 then 0 else 1 end as lapse
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
where u_custom_platform = 'iOS' and idfa is not null and event_time between '2016-03-01' and '2016-04-01' 
group by 1)

, events as
(select event_type, idfa, date_trunc('minute', event_time) as event_time, u_totalcredits, u_level::bigint, 0 as qws, 0 as spins from app132763.outofcredits  
where u_custom_platform = 'iOS' and idfa is not null and event_time between '2016-02-01' and '2016-04-01' 
group by 1,2,3,4,5,6
union all
select event_type, idfa, event_time, u_totalcredits, e_level::bigint, 0 as qws, 0 as spins from app132763.levelup 
where u_custom_platform = 'iOS' and idfa is not null and event_time between '2016-02-01' and '2016-04-01' 
union all
select event_type, idfa, event_time, u_totalcredits, coalesce(u_level,u_level_1)::bigint as u_level
, (nvl(e_gameplay_bigwin::float,0.0) + nvl(e_gameplay_megawin::float,0.0) + nvl(e_gameplay_epicwin::float,0.0)) as qws 
, regexp_count(event_properties	,'MachineSpinNumber') as spins
from app132763.heartbeat
where u_custom_platform = 'iOS' and idfa is not null and event_time between '2016-02-01' and '2016-04-01' 
union all
select event_type, idfa, event_time, u_totalcredits, u_level::bigint, 0 as qws, 0 as spins from app132763.startsessionplayerinfo
where u_custom_platform = 'iOS' and idfa is not null and event_time between '2016-02-01' and '2016-04-01' 
union all
select event_type, idfa, event_time, u_totalcredits, u_level::bigint, e_sum::float as qws, 0 as spins from app132763.purchase_verified
where u_custom_platform = 'iOS' and idfa is not null and event_time between '2016-02-01' and '2016-04-01'
union all
select event_type, idfa, event_time, u_totalcredits, u_level::bigint, 0 as qws, 0 as spins from app132763.collectdailybonus
where u_custom_platform = 'iOS' and idfa is not null and event_time between '2016-02-01' and '2016-04-01'  )

select t.idfa, t.rn, t.rev, case when t.u_networkid is null then False else True end as "hasEmail", coalesce(f.friends,0) as "fb_friends", t.e_viptier, t.event_time, t.e_purchaseamount, t.credits, t.e_level, datediff('hour', t.event_time, t.next_event_time) as hours_until, datediff('hour', t.event_time,t.previous_event_time) as hours_prior, t.lapse

, count(case when e.event_type = 'outOfCredits' and e.event_time between t.event_time - interval '21 days' and t.event_time - interval '14 days' then e.idfa end) ooc_21_14d
, count(case when e.event_type = 'outOfCredits' and e.event_time between t.event_time - interval '14 days' and t.event_time - interval '7 days' then e.idfa end) ooc_14_7d
, count(case when e.event_type = 'outOfCredits' and e.event_time between t.event_time - interval '7 days' and t.event_time - interval '6 days' then e.idfa end) ooc_7_6d
, count(case when e.event_type = 'outOfCredits' and e.event_time between t.event_time - interval '6 days' and t.event_time - interval '5 days' then e.idfa end) ooc_6_5d
, count(case when e.event_type = 'outOfCredits' and e.event_time between t.event_time - interval '5 days' and t.event_time - interval '4 days' then e.idfa end) ooc_5_4d
, count(case when e.event_type = 'outOfCredits' and e.event_time between t.event_time - interval '4 days' and t.event_time - interval '3 days' then e.idfa end) ooc_4_3d
, count(case when e.event_type = 'outOfCredits' and e.event_time between t.event_time - interval '3 days' and t.event_time - interval '2 days' then e.idfa end) ooc_3_2d
, count(case when e.event_type = 'outOfCredits' and e.event_time between t.event_time - interval '2 days' and t.event_time - interval '1 days' then e.idfa end) ooc_2_1d
, count(case when e.event_type = 'outOfCredits' and e.event_time between t.event_time - interval '24 hours' and t.event_time - interval '12 hours' then e.idfa end) ooc_24_12
, count(case when e.event_type = 'outOfCredits' and e.event_time between t.event_time - interval '12 hours' and t.event_time - interval '10 hours' then e.idfa end) ooc_12_10
, count(case when e.event_type = 'outOfCredits' and e.event_time between t.event_time - interval '10 hours' and t.event_time - interval '08 hours' then e.idfa end) ooc_10_08
, count(case when e.event_type = 'outOfCredits' and e.event_time between t.event_time - interval '08 hours' and t.event_time - interval '06 hours' then e.idfa end) ooc_08_06
, count(case when e.event_type = 'outOfCredits' and e.event_time between t.event_time - interval '06 hours' and t.event_time - interval '04 hours' then e.idfa end) ooc_06_04
, count(case when e.event_type = 'outOfCredits' and e.event_time between t.event_time - interval '04 hours' and t.event_time - interval '02 hours' then e.idfa end) ooc_04_02
, count(case when e.event_type = 'outOfCredits' and e.event_time between t.event_time - interval '02 hours' and t.event_time - interval '00 hours' then e.idfa end) ooc_02_00
, count(case when e.event_type = 'outOfCredits' and e.event_time between t.event_time + interval '00 hours' and t.event_time + interval '02 hours' then e.idfa end) ooc_00_02
, count(case when e.event_type = 'outOfCredits' and e.event_time between t.event_time + interval '02 hours' and t.event_time + interval '04 hours' then e.idfa end) ooc_02_04
, count(case when e.event_type = 'outOfCredits' and e.event_time between t.event_time + interval '04 hours' and t.event_time + interval '06 hours' then e.idfa end) ooc_04_06
, count(case when e.event_type = 'outOfCredits' and e.event_time between t.event_time + interval '06 hours' and t.event_time + interval '08 hours' then e.idfa end) ooc_06_08
, count(case when e.event_type = 'outOfCredits' and e.event_time between t.event_time + interval '08 hours' and t.event_time + interval '10 hours' then e.idfa end) ooc_08_10
, count(case when e.event_type = 'outOfCredits' and e.event_time between t.event_time + interval '10 hours' and t.event_time + interval '12 hours' then e.idfa end) ooc_10_12
, count(case when e.event_type = 'outOfCredits' and e.event_time between t.event_time + interval '12 hours' and t.event_time + interval '24 hours' then e.idfa end) ooc_12_24
, count(case when e.event_type = 'outOfCredits' and e.event_time between t.event_time + interval '24 hours' and t.event_time + interval '48 hours' then e.idfa end) ooc_24_48


, count(case when e.event_type = 'startSessionPlayerInfo' and e.event_time between t.event_time - interval '21 days' and t.event_time - interval '14 days' then e.idfa end) ss_21_14d
, count(case when e.event_type = 'startSessionPlayerInfo' and e.event_time between t.event_time - interval '14 days' and t.event_time - interval '7 days' then e.idfa end) ss_14_7d
, count(case when e.event_type = 'startSessionPlayerInfo' and e.event_time between t.event_time - interval '7 days' and t.event_time - interval '6 days' then e.idfa end) ss_7_6d
, count(case when e.event_type = 'startSessionPlayerInfo' and e.event_time between t.event_time - interval '6 days' and t.event_time - interval '5 days' then e.idfa end) ss_6_5d
, count(case when e.event_type = 'startSessionPlayerInfo' and e.event_time between t.event_time - interval '5 days' and t.event_time - interval '4 days' then e.idfa end) ss_5_4d
, count(case when e.event_type = 'startSessionPlayerInfo' and e.event_time between t.event_time - interval '4 days' and t.event_time - interval '3 days' then e.idfa end) ss_4_3d
, count(case when e.event_type = 'startSessionPlayerInfo' and e.event_time between t.event_time - interval '3 days' and t.event_time - interval '2 days' then e.idfa end) ss_3_2d
, count(case when e.event_type = 'startSessionPlayerInfo' and e.event_time between t.event_time - interval '2 days' and t.event_time - interval '1 days' then e.idfa end) ss_2_1d
, count(case when e.event_type = 'startSessionPlayerInfo' and e.event_time between t.event_time - interval '24 hours' and t.event_time - interval '12 hours' then e.idfa end) ss_24_12
, count(case when e.event_type = 'startSessionPlayerInfo' and e.event_time between t.event_time - interval '12 hours' and t.event_time - interval '10 hours' then e.idfa end) ss_12_10
, count(case when e.event_type = 'startSessionPlayerInfo' and e.event_time between t.event_time - interval '10 hours' and t.event_time - interval '08 hours' then e.idfa end) ss_10_08
, count(case when e.event_type = 'startSessionPlayerInfo' and e.event_time between t.event_time - interval '08 hours' and t.event_time - interval '06 hours' then e.idfa end) ss_08_06
, count(case when e.event_type = 'startSessionPlayerInfo' and e.event_time between t.event_time - interval '06 hours' and t.event_time - interval '04 hours' then e.idfa end) ss_06_04
, count(case when e.event_type = 'startSessionPlayerInfo' and e.event_time between t.event_time - interval '04 hours' and t.event_time - interval '02 hours' then e.idfa end) ss_04_02
, count(case when e.event_type = 'startSessionPlayerInfo' and e.event_time between t.event_time - interval '02 hours' and t.event_time - interval '00 hours' then e.idfa end) ss_02_00
, count(case when e.event_type = 'startSessionPlayerInfo' and e.event_time between t.event_time + interval '00 hours' and t.event_time + interval '02 hours' then e.idfa end) ss_00_02
, count(case when e.event_type = 'startSessionPlayerInfo' and e.event_time between t.event_time + interval '02 hours' and t.event_time + interval '04 hours' then e.idfa end) ss_02_04
, count(case when e.event_type = 'startSessionPlayerInfo' and e.event_time between t.event_time + interval '04 hours' and t.event_time + interval '06 hours' then e.idfa end) ss_04_06
, count(case when e.event_type = 'startSessionPlayerInfo' and e.event_time between t.event_time + interval '06 hours' and t.event_time + interval '08 hours' then e.idfa end) ss_06_08
, count(case when e.event_type = 'startSessionPlayerInfo' and e.event_time between t.event_time + interval '08 hours' and t.event_time + interval '10 hours' then e.idfa end) ss_08_10
, count(case when e.event_type = 'startSessionPlayerInfo' and e.event_time between t.event_time + interval '10 hours' and t.event_time + interval '12 hours' then e.idfa end) ss_10_12
, count(case when e.event_type = 'startSessionPlayerInfo' and e.event_time between t.event_time + interval '12 hours' and t.event_time + interval '24 hours' then e.idfa end) ss_12_24
, count(case when e.event_type = 'startSessionPlayerInfo' and e.event_time between t.event_time + interval '24 hours' and t.event_time + interval '48 hours' then e.idfa end) ss_24_48

, count(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '21 days' and t.event_time - interval '14 days' then e.idfa end) hb_21_14d
, count(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '14 days' and t.event_time - interval '7 days' then e.idfa end) hb_14_7d
, count(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '7 days' and t.event_time - interval '6 days' then e.idfa end) hb_7_6d
, count(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '6 days' and t.event_time - interval '5 days' then e.idfa end) hb_6_5d
, count(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '5 days' and t.event_time - interval '4 days' then e.idfa end) hb_5_4d
, count(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '4 days' and t.event_time - interval '3 days' then e.idfa end) hb_4_3d
, count(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '3 days' and t.event_time - interval '2 days' then e.idfa end) hb_3_2d
, count(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '2 days' and t.event_time - interval '1 days' then e.idfa end) hb_2_1d
, count(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '24 hours' and t.event_time - interval '12 hours' then e.idfa end) hb_24_12
, count(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '12 hours' and t.event_time - interval '10 hours' then e.idfa end) hb_12_10
, count(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '10 hours' and t.event_time - interval '08 hours' then e.idfa end) hb_10_08
, count(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '08 hours' and t.event_time - interval '06 hours' then e.idfa end) hb_08_06
, count(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '06 hours' and t.event_time - interval '04 hours' then e.idfa end) hb_06_04
, count(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '04 hours' and t.event_time - interval '02 hours' then e.idfa end) hb_04_02
, count(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '02 hours' and t.event_time - interval '00 hours' then e.idfa end) hb_02_00
, count(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time + interval '00 hours' and t.event_time + interval '02 hours' then e.idfa end) hb_00_02
, count(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time + interval '02 hours' and t.event_time + interval '04 hours' then e.idfa end) hb_02_04
, count(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time + interval '04 hours' and t.event_time + interval '06 hours' then e.idfa end) hb_04_06
, count(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time + interval '06 hours' and t.event_time + interval '08 hours' then e.idfa end) hb_06_08
, count(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time + interval '08 hours' and t.event_time + interval '10 hours' then e.idfa end) hb_08_10
, count(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time + interval '10 hours' and t.event_time + interval '12 hours' then e.idfa end) hb_10_12
, count(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time + interval '12 hours' and t.event_time + interval '24 hours' then e.idfa end) hb_12_24
, count(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time + interval '24 hours' and t.event_time + interval '48 hours' then e.idfa end) hb_24_48

, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '21 days' and t.event_time - interval '14 days' then e.qws end),0) qw_21_14d
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '14 days' and t.event_time - interval '7 days' then e.qws end),0) qw_14_7d
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '7 days' and t.event_time - interval '6 days' then e.qws end),0) qw_7_6d
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '6 days' and t.event_time - interval '5 days' then e.qws end),0) qw_6_5d
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '5 days' and t.event_time - interval '4 days' then e.qws end),0) qw_5_4d
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '4 days' and t.event_time - interval '3 days' then e.qws end),0) qw_4_3d
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '3 days' and t.event_time - interval '2 days' then e.qws end),0) qw_3_2d
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '2 days' and t.event_time - interval '1 days' then e.qws end),0) qw_2_1d
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '24 hours' and t.event_time - interval '12 hours' then e.qws end),0) qw_24_12
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '12 hours' and t.event_time - interval '10 hours' then e.qws end),0) qw_12_10
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '10 hours' and t.event_time - interval '08 hours' then e.qws end),0) qw_10_08
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '08 hours' and t.event_time - interval '06 hours' then e.qws end),0) qw_08_06
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '06 hours' and t.event_time - interval '04 hours' then e.qws end),0) qw_06_04
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '04 hours' and t.event_time - interval '02 hours' then e.qws end),0) qw_04_02
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '02 hours' and t.event_time - interval '00 hours' then e.qws end),0) qw_02_00
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time + interval '00 hours' and t.event_time + interval '02 hours' then e.qws end),0) qw_00_02
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time + interval '02 hours' and t.event_time + interval '04 hours' then e.qws end),0) qw_02_04
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time + interval '04 hours' and t.event_time + interval '06 hours' then e.qws end),0) qw_04_06
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time + interval '06 hours' and t.event_time + interval '08 hours' then e.qws end),0) qw_06_08
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time + interval '08 hours' and t.event_time + interval '10 hours' then e.qws end),0) qw_08_10
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time + interval '10 hours' and t.event_time + interval '12 hours' then e.qws end),0) qw_10_12
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time + interval '12 hours' and t.event_time + interval '24 hours' then e.qws end),0) qw_12_24
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time + interval '24 hours' and t.event_time + interval '48 hours' then e.qws end),0) qw_24_48

, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '21 days' and t.event_time - interval '14 days' then e.spins end),0) sp_21_14d
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '14 days' and t.event_time - interval '7 days' then e.spins end),0) sp_14_7d
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '7 days' and t.event_time - interval '6 days' then e.spins end),0) sp_7_6d
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '6 days' and t.event_time - interval '5 days' then e.spins end),0) sp_6_5d
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '5 days' and t.event_time - interval '4 days' then e.spins end),0) sp_5_4d
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '4 days' and t.event_time - interval '3 days' then e.spins end),0) sp_4_3d
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '3 days' and t.event_time - interval '2 days' then e.spins end),0) sp_3_2d
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '2 days' and t.event_time - interval '1 days' then e.spins end),0) sp_2_1d
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '24 hours' and t.event_time - interval '12 hours' then e.spins end),0) sp_24_12
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '12 hours' and t.event_time - interval '10 hours' then e.spins end),0) sp_12_10
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '10 hours' and t.event_time - interval '08 hours' then e.spins end),0) sp_10_08
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '08 hours' and t.event_time - interval '06 hours' then e.spins end),0) sp_08_06
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '06 hours' and t.event_time - interval '04 hours' then e.spins end),0) sp_06_04
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '04 hours' and t.event_time - interval '02 hours' then e.spins end),0) sp_04_02
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time - interval '02 hours' and t.event_time - interval '00 hours' then e.spins end),0) sp_02_00
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time + interval '00 hours' and t.event_time + interval '02 hours' then e.spins end),0) sp_00_02
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time + interval '02 hours' and t.event_time + interval '04 hours' then e.spins end),0) sp_02_04
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time + interval '04 hours' and t.event_time + interval '06 hours' then e.spins end),0) sp_04_06
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time + interval '06 hours' and t.event_time + interval '08 hours' then e.spins end),0) sp_06_08
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time + interval '08 hours' and t.event_time + interval '10 hours' then e.spins end),0) sp_08_10
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time + interval '10 hours' and t.event_time + interval '12 hours' then e.spins end),0) sp_10_12
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time + interval '12 hours' and t.event_time + interval '24 hours' then e.spins end),0) sp_12_24
, coalesce(sum(case when e.event_type = 'HeartBeat' and e.event_time between t.event_time + interval '24 hours' and t.event_time + interval '48 hours' then e.spins end),0) sp_24_48

, count(case when e.event_type = 'levelUp' and e.event_time between t.event_time - interval '21 days' and t.event_time - interval '14 days' then e.idfa end) lu_21_14d
, count(case when e.event_type = 'levelUp' and e.event_time between t.event_time - interval '14 days' and t.event_time - interval '7 days' then e.idfa end) lu_14_7d
, count(case when e.event_type = 'levelUp' and e.event_time between t.event_time - interval '7 days' and t.event_time - interval '6 days' then e.idfa end) lu_7_6d
, count(case when e.event_type = 'levelUp' and e.event_time between t.event_time - interval '6 days' and t.event_time - interval '5 days' then e.idfa end) lu_6_5d
, count(case when e.event_type = 'levelUp' and e.event_time between t.event_time - interval '5 days' and t.event_time - interval '4 days' then e.idfa end) lu_5_4d
, count(case when e.event_type = 'levelUp' and e.event_time between t.event_time - interval '4 days' and t.event_time - interval '3 days' then e.idfa end) lu_4_3d
, count(case when e.event_type = 'levelUp' and e.event_time between t.event_time - interval '3 days' and t.event_time - interval '2 days' then e.idfa end) lu_3_2d
, count(case when e.event_type = 'levelUp' and e.event_time between t.event_time - interval '2 days' and t.event_time - interval '1 days' then e.idfa end) lu_2_1d
, count(case when e.event_type = 'levelUp' and e.event_time between t.event_time - interval '24 hours' and t.event_time - interval '12 hours' then e.idfa end) lu_24_12
, count(case when e.event_type = 'levelUp' and e.event_time between t.event_time - interval '12 hours' and t.event_time - interval '10 hours' then e.idfa end) lu_12_10
, count(case when e.event_type = 'levelUp' and e.event_time between t.event_time - interval '10 hours' and t.event_time - interval '08 hours' then e.idfa end) lu_10_08
, count(case when e.event_type = 'levelUp' and e.event_time between t.event_time - interval '08 hours' and t.event_time - interval '06 hours' then e.idfa end) lu_08_06
, count(case when e.event_type = 'levelUp' and e.event_time between t.event_time - interval '06 hours' and t.event_time - interval '04 hours' then e.idfa end) lu_06_04
, count(case when e.event_type = 'levelUp' and e.event_time between t.event_time - interval '04 hours' and t.event_time - interval '02 hours' then e.idfa end) lu_04_02
, count(case when e.event_type = 'levelUp' and e.event_time between t.event_time - interval '02 hours' and t.event_time - interval '00 hours' then e.idfa end) lu_02_00
, count(case when e.event_type = 'levelUp' and e.event_time between t.event_time + interval '00 hours' and t.event_time + interval '02 hours' then e.idfa end) lu_00_02
, count(case when e.event_type = 'levelUp' and e.event_time between t.event_time + interval '02 hours' and t.event_time + interval '04 hours' then e.idfa end) lu_02_04
, count(case when e.event_type = 'levelUp' and e.event_time between t.event_time + interval '04 hours' and t.event_time + interval '06 hours' then e.idfa end) lu_04_06
, count(case when e.event_type = 'levelUp' and e.event_time between t.event_time + interval '06 hours' and t.event_time + interval '08 hours' then e.idfa end) lu_06_08
, count(case when e.event_type = 'levelUp' and e.event_time between t.event_time + interval '08 hours' and t.event_time + interval '10 hours' then e.idfa end) lu_08_10
, count(case when e.event_type = 'levelUp' and e.event_time between t.event_time + interval '10 hours' and t.event_time + interval '12 hours' then e.idfa end) lu_10_12
, count(case when e.event_type = 'levelUp' and e.event_time between t.event_time + interval '12 hours' and t.event_time + interval '24 hours' then e.idfa end) lu_12_24
, count(case when e.event_type = 'levelUp' and e.event_time between t.event_time + interval '24 hours' and t.event_time + interval '48 hours' then e.idfa end) lu_24_48

, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '21 days' and t.event_time - interval '14 days' then e.idfa end) pv_21_14d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '14 days' and t.event_time - interval '7 days' then e.idfa end) pv_14_7d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '7 days' and t.event_time - interval '6 days' then e.idfa end) pv_7_6d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '6 days' and t.event_time - interval '5 days' then e.idfa end) pv_6_5d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '5 days' and t.event_time - interval '4 days' then e.idfa end) pv_5_4d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '4 days' and t.event_time - interval '3 days' then e.idfa end) pv_4_3d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '3 days' and t.event_time - interval '2 days' then e.idfa end) pv_3_2d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '2 days' and t.event_time - interval '1 days' then e.idfa end) pv_2_1d
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '24 hours' and t.event_time - interval '12 hours' then e.idfa end) pv_24_12
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '12 hours' and t.event_time - interval '10 hours' then e.idfa end) pv_12_10
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '10 hours' and t.event_time - interval '08 hours' then e.idfa end) pv_10_08
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '08 hours' and t.event_time - interval '06 hours' then e.idfa end) pv_08_06
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '06 hours' and t.event_time - interval '04 hours' then e.idfa end) pv_06_04
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '04 hours' and t.event_time - interval '02 hours' then e.idfa end) pv_04_02
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '02 hours' and t.event_time - interval '00 hours' then e.idfa end) pv_02_00
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time + interval '00 hours' and t.event_time + interval '02 hours' then e.idfa end) pv_00_02
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time + interval '02 hours' and t.event_time + interval '04 hours' then e.idfa end) pv_02_04
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time + interval '04 hours' and t.event_time + interval '06 hours' then e.idfa end) pv_04_06
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time + interval '06 hours' and t.event_time + interval '08 hours' then e.idfa end) pv_06_08
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time + interval '08 hours' and t.event_time + interval '10 hours' then e.idfa end) pv_08_10
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time + interval '10 hours' and t.event_time + interval '12 hours' then e.idfa end) pv_10_12
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time + interval '12 hours' and t.event_time + interval '24 hours' then e.idfa end) pv_12_24
, count(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time + interval '24 hours' and t.event_time + interval '48 hours' then e.idfa end) pv_24_48

, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '21 days' and t.event_time - interval '14 days' then e.qws end),0) rev_21_14d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '14 days' and t.event_time - interval '7 days' then e.qws end),0) rev_14_7d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '7 days' and t.event_time - interval '6 days' then e.qws end),0) rev_7_6d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '6 days' and t.event_time - interval '5 days' then e.qws end),0) rev_6_5d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '5 days' and t.event_time - interval '4 days' then e.qws end),0) rev_5_4d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '4 days' and t.event_time - interval '3 days' then e.qws end),0) rev_4_3d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '3 days' and t.event_time - interval '2 days' then e.qws end),0) rev_3_2d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '2 days' and t.event_time - interval '1 days' then e.qws end),0) rev_2_1d
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '24 hours' and t.event_time - interval '12 hours' then e.qws end),0) rev_24_12
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '12 hours' and t.event_time - interval '10 hours' then e.qws end),0) rev_12_10
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '10 hours' and t.event_time - interval '08 hours' then e.qws end),0) rev_10_08
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '08 hours' and t.event_time - interval '06 hours' then e.qws end),0) rev_08_06
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '06 hours' and t.event_time - interval '04 hours' then e.qws end),0) rev_06_04
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '04 hours' and t.event_time - interval '02 hours' then e.qws end),0) rev_04_02
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time - interval '02 hours' and t.event_time - interval '00 hours' then e.qws end),0) rev_02_00
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time + interval '1 minute' and t.event_time + interval '02 hours' then e.qws end),0) rev_00_02
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time + interval '02 hours' and t.event_time + interval '04 hours' then e.qws end),0) rev_02_04
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time + interval '04 hours' and t.event_time + interval '06 hours' then e.qws end),0) rev_04_06
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time + interval '06 hours' and t.event_time + interval '08 hours' then e.qws end),0) rev_06_08
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time + interval '08 hours' and t.event_time + interval '10 hours' then e.qws end),0) rev_08_10
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time + interval '10 hours' and t.event_time + interval '12 hours' then e.qws end),0) rev_10_12
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time + interval '12 hours' and t.event_time + interval '24 hours' then e.qws end),0) rev_12_24
, coalesce(sum(case when e.event_type = 'purchase_verified' and e.event_time between t.event_time + interval '24 hours' and t.event_time + interval '48 hours' then e.qws end),0) rev_24_48

, count(case when e.event_type = 'collectDailyBonus' and e.event_time between t.event_time - interval '21 days' and t.event_time - interval '14 days' then e.idfa end) chb_21_14d
, count(case when e.event_type = 'collectDailyBonus' and e.event_time between t.event_time - interval '14 days' and t.event_time - interval '7 days' then e.idfa end) chb_14_7d
, count(case when e.event_type = 'collectDailyBonus' and e.event_time between t.event_time - interval '7 days' and t.event_time - interval '6 days' then e.idfa end) chb_7_6d
, count(case when e.event_type = 'collectDailyBonus' and e.event_time between t.event_time - interval '6 days' and t.event_time - interval '5 days' then e.idfa end) chb_6_5d
, count(case when e.event_type = 'collectDailyBonus' and e.event_time between t.event_time - interval '5 days' and t.event_time - interval '4 days' then e.idfa end) chb_5_4d
, count(case when e.event_type = 'collectDailyBonus' and e.event_time between t.event_time - interval '4 days' and t.event_time - interval '3 days' then e.idfa end) chb_4_3d
, count(case when e.event_type = 'collectDailyBonus' and e.event_time between t.event_time - interval '3 days' and t.event_time - interval '2 days' then e.idfa end) chb_3_2d
, count(case when e.event_type = 'collectDailyBonus' and e.event_time between t.event_time - interval '2 days' and t.event_time - interval '1 days' then e.idfa end) chb_2_1d
, count(case when e.event_type = 'collectDailyBonus' and e.event_time between t.event_time - interval '24 hours' and t.event_time - interval '12 hours' then e.idfa end) chb_24_12
, count(case when e.event_type = 'collectDailyBonus' and e.event_time between t.event_time - interval '12 hours' and t.event_time - interval '10 hours' then e.idfa end) chb_12_10
, count(case when e.event_type = 'collectDailyBonus' and e.event_time between t.event_time - interval '10 hours' and t.event_time - interval '08 hours' then e.idfa end) chb_10_08
, count(case when e.event_type = 'collectDailyBonus' and e.event_time between t.event_time - interval '08 hours' and t.event_time - interval '06 hours' then e.idfa end) chb_08_06
, count(case when e.event_type = 'collectDailyBonus' and e.event_time between t.event_time - interval '06 hours' and t.event_time - interval '04 hours' then e.idfa end) chb_06_04
, count(case when e.event_type = 'collectDailyBonus' and e.event_time between t.event_time - interval '04 hours' and t.event_time - interval '02 hours' then e.idfa end) chb_04_02
, count(case when e.event_type = 'collectDailyBonus' and e.event_time between t.event_time - interval '02 hours' and t.event_time - interval '00 hours' then e.idfa end) chb_02_00
, count(case when e.event_type = 'collectDailyBonus' and e.event_time between t.event_time + interval '00 hours' and t.event_time + interval '02 hours' then e.idfa end) chb_00_02
, count(case when e.event_type = 'collectDailyBonus' and e.event_time between t.event_time + interval '02 hours' and t.event_time + interval '04 hours' then e.idfa end) chb_02_04
, count(case when e.event_type = 'collectDailyBonus' and e.event_time between t.event_time + interval '04 hours' and t.event_time + interval '06 hours' then e.idfa end) chb_04_06
, count(case when e.event_type = 'collectDailyBonus' and e.event_time between t.event_time + interval '06 hours' and t.event_time + interval '08 hours' then e.idfa end) chb_06_08
, count(case when e.event_type = 'collectDailyBonus' and e.event_time between t.event_time + interval '08 hours' and t.event_time + interval '10 hours' then e.idfa end) chb_08_10
, count(case when e.event_type = 'collectDailyBonus' and e.event_time between t.event_time + interval '10 hours' and t.event_time + interval '12 hours' then e.idfa end) chb_10_12
, count(case when e.event_type = 'collectDailyBonus' and e.event_time between t.event_time + interval '12 hours' and t.event_time + interval '24 hours' then e.idfa end) chb_12_24
, count(case when e.event_type = 'collectDailyBonus' and e.event_time between t.event_time + interval '24 hours' and t.event_time + interval '48 hours' then e.idfa end) chb_24_48


from txns t
join events e on t.idfa = e.idfa
left join friends f on t.idfa=f.idfa
where t.event_time between '2016-03-01' and '2016-04-01'
group by t.idfa, t.rn, t.rev, "hasEmail", "fb_friends", t.u_networkid, t.e_viptier, t.event_time,t.idfa, t.rn, t.u_networkid, t.e_viptier, t.event_time,  t.e_purchaseamount, t.credits, t.e_level, datediff('hour', t.event_time, t.next_event_time), datediff('hour', t.event_time,t.previous_event_time), t.lapse
order by random()
