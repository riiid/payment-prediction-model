with orders as( --b2b 제외
        select paid_at, count(paid_at) as paid_cnt, sum(total_charge_amount) as revenue
        from
            (select o.user_id, e.id, e.order_id as order_id, o.product_days,
                date(e.event_time, 'Asia/Seoul') as paid_at, total_charge_amount,
                row_number() over (partition by e.order_id order by e.id desc) as rn
            from toeic_db.order o
            inner join toeic_db.order_amount_event e on e.order_id = o.id
            inner join toeic_db.product p on p.id = o.product_id
            inner join toeic_db.payment_provider pp on pp.id = o.payment_provider_id
            inner join toeic_db.fulfillment f on f.order_id = o.id
            inner join toeic_db.goods g on g.id = f.goods_id and g.currency = 'KRW'
            where e.event_type = 'PAID' and total_charge_amount > 0
              and display_config_name not like '%에듀%' and display_config_name not like '%한동대%'
              and display_config_name not like '%B2B%' and display_config_name not like '%GE%'
              and display_config_name not like '%순천향대%' and display_config_name not like '%안산대%'
              and display_config_name not like '%인하공업%' and display_config_name not like '%전문대학생%'
              and display_config_name not like '%GS에듀%' and display_config_name not like '%중원대%'
              and display_config_name not like '%동양미래%' and display_config_name not like '%한양여대%'
              and display_config_name not like '%경복대%' and display_config_name not like '%산업은행%'
              and display_config_name not like '%에듀테크%' and display_config_name not like '%TTC%')
        where rn = 1
        group by 1
    ),
    arppu as(
        select paid_at, round(safe_divide(revenue, paid_cnt)) as arppu
        from orders
        order by 1 desc
    )

select * from arppu
