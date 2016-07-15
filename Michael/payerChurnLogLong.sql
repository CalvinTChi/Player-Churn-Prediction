select * from
(select
        user_id, event_time, event_time::date as date, e_sum::float as rev, e_purchaseamount::float, e_purchaseprice, case when u_networkid is null then False else True end hasEmail, e_viptier
        , e_purchaseamount::float/e_purchaseprice::float as xrate
        , e_source, e_vip_boost, e_vip_points
        , e_creditsbeforepurchase
        , e_level::bigint
        , e_machine
        , u_playertenure::bigint
        , u_fbstatus
        , u_totalcredits
        , u_totalcredits::float - e_purchaseamount::float as credits
        , row_number() over (partition by user_id order by event_time) as rn
        , dense_rank() over (partition by  user_id order by event_time::date) as rank
        , count(user_id) over (partition by user_id) as txns
        , count(user_id) over (partition by user_id, event_time::date) as txns_on_day
        , dense_rank() over (partition by user_id order by event_time::date desc) rank_desc
        , lead(event_time,1) over (partition by user_id order by event_time) as next_event_time
        , lag(event_time,1) over (partition by user_id order by event_time) as previous_event_time
        from app132763.purchase_verified
        where u_custom_platform = 'iOS' and idfa is not null) x
where event_time between '2015-09-01' and '2016-05-01'
order by user_id, event_time
