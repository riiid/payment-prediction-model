create temp function ndy(dt date)
    as (date_add(dt, interval 0 day));

--가입 후 d+x일까지 activity
with users_ as(
        select * except(rn),
            case when extract(month from registered_at) = 12 or extract(month from registered_at) = 01 or extract(month from registered_at) = 02
                   or extract(month from registered_at) = 06 or extract(month from registered_at) = 07 or extract(month from registered_at) = 08
                   then 1 else 0 end as season
        from
            (select date(u.created_at, 'Asia/Seoul') as installed_at, u.auth_id, u.first_country,
                date(e.datetime) as exam_date, es.lc_score, es.rc_score, es.lc_score + es.rc_score as exam_score,
                row_number() over(partition by u.auth_id order by date(e.datetime) desc) as rn,
                case when pi.contact_email is null then u.contact_email else pi.contact_email end as contact_email,
                case when p.target_score is null then u.target_score else p.target_score end as target_score,
                case when p.diagnosis_progress is null then u.diagnosis_progress else p.diagnosis_progress end as diagnosis_progress,
                date(registered_at, 'Asia/Seoul') as registered_at,
            from toeic_db.user u
            left join toeic_db.user_profile p on u.auth_id = p.auth_id
            left join toeic_db.personal_identity_info pi on u.auth_id = pi.auth_id
            left join toeic_db.toeic_exam_score es on es.user_auth_id = u.auth_id
            left join toeic_db.toeic_exam e on es.exam_id = e.id
            where u.first_country = 'KR'
              and u.registered_at is not null)
        where (contact_email not like '%riiid%'
              or contact_email is null)
          and rn = 1
    ),
    users as( --환불 제외 첫 구매
        select u.*, o.* except(user_id)
        from users_ u
        left join
            (select * except(rn)
            from
                (select o.user_id, date(e.event_time, 'Asia/Seoul') as paid_at, p.display_config_name as product_name,
                  p.display_config_description as product_description, p.days as product_days, total_charge_amount,
                  datetime(e.event_time, 'Asia/Seoul') as valid_from,
                  date_add(datetime(e.event_time, 'Asia/Seoul'), interval cast(product_days as int) day) as valid_until,
                  row_number() over(partition by user_id order by o.id) as rn
                from toeic_db.order o
                inner join toeic_db.order_amount_event e on e.order_id = o.id
                inner join toeic_db.product p on p.id = o.product_id
                inner join toeic_db.payment_provider pp on pp.id = o.payment_provider_id
                where e.event_type = 'PAID'
                  and total_charge_amount > 0
                group by 1,2,3,4,5,6,7,8, o.id)
            where rn = 1) o on u.auth_id = o.user_id and o.paid_at <= ndy(registered_at)
        where o.paid_at is null or o.paid_at = ndy(registered_at)
    ),
    access as (
        select u.auth_id, count(distinct(date(l.timestamp_server, 'Asia/Seoul'))) as access_cnt
        from users u
        inner join toeic_db.user_device_access_log l on l.user_auth_id = u.auth_id
        where (datetime(l.timestamp_server, 'Asia/Seoul') < u.valid_from
                or u.valid_from is null)
          and date(l.timestamp_server, 'Asia/Seoul') <= ndy(registered_at)
        group by 1
    ),
    complete_cell as(
        select id
        from
            (select s.id, sum(case when cis.completed_at is null then 1 else 0 end) as null_cnt
            from toeic_db.learning_session s
            inner join toeic_db.content_interaction_state cis on cis.learning_session_id = s.id
            group by 1
            order by 2 desc)
        where null_cnt = 0
    ),
    cell_ as(
        select * except(rn), row_number() over(partition by parent_id order by completed_at desc) as rn
        from
            (select c.auth_id, c.session_id, c.parent_id, c.type, c.parent_type,
                datetime(cis.completed_at, 'Asia/Seoul') as completed_at,
                row_number() over(partition by c.auth_id, c.session_id order by cis.completed_at desc) as rn
            from toeic_db.learning_cell c
            inner join complete_cell s on s.id = c.session_id
            inner join toeic_db.content_interaction_state cis on cis.learning_session_id = c.session_id
            group by 1,2,3,4,5, cis.completed_at)
        where rn = 1
    ),
    cell_scores_ as(
        select u.auth_id, c.* except(auth_id),
            row_number() over(partition by u.auth_id order by c.completed_at) as rn
        from users u
        inner join
            (--추천학습
            (select c.auth_id, c.completed_at, c.session_id as id, c.type, es.lc + es.rc as score
            from cell_ c
            inner join
                (select *, row_number() over(partition by learning_cycle_id order by skill_analytics_id desc) as rn
                from toeic_db.learning_cycle_progression_snapshot) l on l.rn = c.rn and l.learning_cycle_id = c.parent_id
            inner join toeic_db.skill_analytics a on a.id = l.skill_analytics_id
            inner join toeic_db.estimated_score es on es.id = a.estimated_score_id
            where c.parent_type = 'CYCLE'
            group by 1,2,3,4,5
            order by 2 desc)
            union all
            --선택학습
            (select c.auth_id, c.completed_at, c.session_id as id, c.type, es.lc + es.rc as score
            from cell_ c
            inner join toeic_db.self_learning_record_unit r on r.auth_id = c.auth_id and r.session_id = c.session_id
            inner join toeic_db.skill_analytics a on a.id = r.skill_analytics_id
            inner join toeic_db.estimated_score es on es.id = a.estimated_score_id
            where c.parent_type = 'SELF_CARD'
            group by 1,2,3,4,5
            order by 2 desc)) c on u.auth_id = c.auth_id and c.completed_at <= ndy(registered_at)
    ),
    diagnoses as( --진단고사
        select s.auth_id, s.score as diag_score, s.rn as diag_order
        from users u
        inner join cell_scores_ s on s.auth_id = u.auth_id
        where (s.completed_at < u.valid_from
              or u.valid_from is null)
          and s.type = 'DIAGNOSIS'
    ),
    first_cell as( --구매 전 처음으로 푼 셀
        select s.*
        from users u
        inner join cell_scores_ s on s.auth_id = u.auth_id
        where (s.completed_at < u.valid_from
              or u.valid_from is null)
          and rn = 1
    ),
    last_cell as( --구매 전 마지막 점수
        select * except(rn_rev)
        from
            (select s.*, row_number() over(partition by s.auth_id order by completed_at desc) as rn_rev
            from users u
            inner join cell_scores_ s on s.auth_id = u.auth_id
            where (s.completed_at < u.valid_from
                  or u.valid_from is null))
        where rn_rev = 1
    ),
    cells as( --진단고사 제외 구매전까지 푼 셀 개수
        select s.auth_id, count(id) as cell_cnt
        from users u
        inner join cell_scores_ s on s.auth_id = u.auth_id
        where (s.completed_at < u.valid_from
              or u.valid_from is null)
          and s.type <> 'DIAGNOSIS'
        group by 1
    ),
    cycles as( --구매전까지 푼 싸이클 개수
        select s.auth_id, count(id) as cycle_cnt
        from users u
        inner join cell_scores_ s on s.auth_id = u.auth_id
        where (s.completed_at < u.valid_from
              or u.valid_from is null)
          and s.type in ('REVIEW', 'MOCK_EXAM')
        group by 1
    ),
    contents as( --구매전 콘텐츠별 학습 횟수
        select s.auth_id,
            count(case when type = 'BASIC' then s.auth_id end) as basics,
            --count(case when type = 'DIAGNOSIS'then auth_id end) as diagnoses,
            count(case when type = 'LESSON' then s.auth_id end) as lessons,
            count(case when type = 'MOCK_EXAM' then s.auth_id end) as mock_exams,
            count(case when type = 'MY_NOTE_QUIZ' then s.auth_id end) as my_note_quizzes,
            count(case when type = 'QUESTION' then s.auth_id end) as questions,
            count(case when type = 'REVIEW' then s.auth_id end) as reviews,
            count(case when type = 'VOCABULARY' then s.auth_id end) as vocab,
            count(case when type = 'SELF_CARD_LESSON' then s.auth_id end) as self_lessons,
            count(case when type = 'SELF_CARD_QUESTION' then s.auth_id end) as self_questions,
            count(case when type = 'SELF_CARD_VIRTUAL_EXAM' then s.auth_id end) as self_virt_exams,
            count(case when type = 'SELF_CARD_VOCABULARY' then s.auth_id end) as self_vocab
        from users u
        inner join cell_scores_ s on s.auth_id = u.auth_id
        where s.completed_at < u.valid_from
           or u.valid_from is null
        group by 1
    ),
    features as(
        select u.auth_id, u.installed_at, date(u.registered_at) as registered_at, u.season, u.target_score, u.exam_date, u.exam_score,
            a.access_cnt, d.diag_score, d.diag_order, f.type as first_cell_type, f.score as first_cell_score, l.score as last_cell_score,
            c.cell_cnt, cy.cycle_cnt, con.* except(auth_id),
            u.paid_at, u.product_name, u.product_days, u.product_description, u.total_charge_amount
        from users u
        left join access a on u.auth_id = a.auth_id
        left join diagnoses d on u.auth_id = d.auth_id
        left join first_cell f on u.auth_id = f.auth_id
        left join last_cell l on u.auth_id = l.auth_id
        left join cells c on u.auth_id = c.auth_id
        left join cycles cy on u.auth_id = cy.auth_id
        left join contents con on u.auth_id = con.auth_id
        order by 1
    )

select * from features
