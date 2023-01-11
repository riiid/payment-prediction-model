select u.auth_id, agree_marketing, agree_age14, agree_privacy, agree_service_terms, agree_push, latest_score, target_score, diagnosis_consumed_count, examination_date, date(last_activity_at, 'Asia/Seoul') as last_activity_at, date(created_at, 'Asia/Seoul') as created_at, date(registered_at, 'Asia/Seoul') as registered_at
from toeic_db.user u
left join toeic_db.personal_identity_info pii on u.auth_id = pii.auth_id
where country = 'KR' and (pii.contact_email not like '%riiid%'
               or pii.contact_email is null)
