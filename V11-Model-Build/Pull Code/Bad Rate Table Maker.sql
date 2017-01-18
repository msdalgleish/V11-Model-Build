-- Payday --

drop table if exists mdalgleish.payday_bad_rates;
create table mdalgleish.payday_bad_rates as (
select
l.id as loan_id,
l.due_date_adjusted::date as due_date_adjusted,
(case when extract(month from l.due_date_adjusted) >= 10 then extract(year from l.due_date_adjusted::date)::text || extract(month from l.due_date_adjusted)::text else extract(year from l.due_date_adjusted::date)::text || '0' || extract(month from l.due_date_adjusted)::text end ) as month_year,
l.gov_law_state_cd as state,
case when cs.id is null then 'organic' else cs.source_type_cd end as lead_provider,
case when rv.score >= 600.0 then 1
     when rv.score >= 578.0 then 2
     when rv.score >= 556.0 then 3
     else 4 end as rv_score_tier,
case when (select count(*) from loans where customer_id = l.customer_id and status_cd not in ('declined','withdrawn','on_hold','approved','applied') and funding_date_actual < l.funding_date_actual) = 0 then 'f' else 't' end as existing_flg,
case when ls.id is not null then 1 else 0 end as initial_default_flg,
case when (select count(*) from loans where base_loan_id = l.id and status_cd not in ('declined','withdrawn','on_hold','approved','applied')) > 0 then 1 else 0 end as extended_flg
from loans l
left join loan_statuses ls on ls.id =
(select id from loan_statuses where loan_id = l.id and status_cd in ('in_default','in_default_pmt_proc') limit 1)
left join customer_sources cs on cs.id =
(select id from customer_sources where customer_id = l.customer_id and created_on between l.requested_time - interval '3 days' and l.requested_time + interval '30 minutes' and type_cd in ('import','lead_reject_import','pass_active_customer') order by id asc limit 1)
left join approvals a on a.id =
(select id from approvals where customer_id = l.customer_id and processed_on <= l.requested_time order by id desc limit 1)
left join lexis_nexis.risk_view_reports rv ON rv.id = a.risk_view_report_id
where l.loan_type_cd = 'payday'
and l.base_loan_id is null
and l.status_cd not in ('declined','withdrawn','on_hold','approved','applied')
and l.due_date_adjusted between '2014-09-01' and '2016-12-31'
--limit 1000
);

create index payday_bad_idx1 on mdalgleish.payday_bad_rates(due_date_adjusted);
create index payday_bad_idx2 on mdalgleish.payday_bad_rates(loan_id);
create index payday_bad_idx3 on mdalgleish.payday_bad_rates(state);
create index payday_bad_idx4 on mdalgleish.payday_bad_rates(lead_provider);


---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


-- Installments --

drop table if exists mdalgleish.installment_bad_rates;
create table mdalgleish.installment_bad_rates as (
select
l.id as loan_id,
i.id as first_installment_id,
i.due_date::date as first_installment_due_date,
(case when extract(month from i.due_date) >= 10 then extract(year from i.due_date::date)::text || extract(month from i.due_date)::text else extract(year from i.due_date::date)::text || '0' || extract(month from i.due_date)::text end ) as month_year,
l.gov_law_state_cd as state,
case when cs.id is null then 'organic' else cs.source_type_cd end as lead_provider,
case when rv.score >= 600.0 then 1
     when rv.score >= 578.0 then 2
     when rv.score >= 556.0 then 3
     else 4 end as rv_score_tier,
case when (select count(*) from loans where customer_id = l.customer_id and status_cd not in ('declined','withdrawn','on_hold','approved','applied') and funding_date_actual < l.funding_date_actual) = 0 then 'f' else 't' end as existing_flg,
case when ish.id is not null then 1 else 0 end as first_installment_initial_default_flg
--case when (select count(*) from installments where loan_id = l.id) > bl.number_paid_installments and l.status_cd = 'paid_off' then 1 else 0 end as early_payoff_flg

from loans l

left join installments i on i.id =
(select id from installments where loan_id = l.id and installment_number = 1 limit 1)

left join installment_status_history ish on ish.id =
(select id from installment_status_history where installments_static_id = i.id and installment_status_id in (3,4) limit 1)

left join customer_sources cs on cs.id =
(select id from customer_sources where customer_id = l.customer_id and created_on between l.requested_time - interval '3 days' and l.requested_time + interval '30 minutes' and type_cd in ('import','lead_reject_import','pass_active_customer') order by id asc limit 1)

left join approvals a on a.id =
(select id from approvals where customer_id = l.customer_id and processed_on <= l.requested_time order by id desc limit 1)

left join lexis_nexis.risk_view_reports rv ON rv.id = a.risk_view_report_id

left join bus_analytics.installment_loans_performance bl on bl.base_loan_id = l.id

where l.loan_type_cd = 'installment'
and l.base_loan_id is null
and l.status_cd not in ('declined','withdrawn','on_hold','approved','applied')
and i.due_date between '2014-09-01' and '2016-12-31'
--limit 1000
);

create index installment_bad_idx1 on mdalgleish.installment_bad_rates(first_installment_due_date);
create index installment_bad_idx2 on mdalgleish.installment_bad_rates(loan_id);
create index installment_bad_idx3 on mdalgleish.installment_bad_rates(state);
create index installment_bad_idx4 on mdalgleish.installment_bad_rates(lead_provider);
create index installment_bad_idx5 on mdalgleish.installment_bad_rates(first_installment_id);




---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


-- LOC --

drop table if exists mdalgleish.loc_bad_rates;
create table mdalgleish.loc_bad_rates as (
select
l.id as loan_id,
os.id as first_statement_id,
os.due_date::date as first_statement_due_date,

case when extract(month from os.due_date) >= 10 then extract(year from os.due_date::date)::text || extract(month from os.due_date)::text else extract(year from os.due_date::date)::text || '0' || extract(month from os.due_date)::text end as month_year,

l.gov_law_state_cd as state,
case when cs.id is null then 'organic' else cs.source_type_cd end as lead_provider,

case when rv.score >= 600.0 then 1
     when rv.score >= 578.0 then 2
     when rv.score >= 556.0 then 3
     else 4 end as rv_score_tier,

case when (select count(*) from loans where customer_id = l.customer_id and status_cd not in ('declined','withdrawn','on_hold','approved','applied') and funding_date_actual < l.funding_date_actual) = 0 then 'f' else 't' end as existing_flg,

case when exists (select ptc.id from payment_transactions_committed ptc
    where ptc.loan_id=l.id
    and ptc.eff_date BETWEEN COALESCE(os.due_date_adjusted, os.due_date)
    AND add_business_days(COALESCE(os.due_date_adjusted, os.due_date),5)
    AND ptc.debit_account_cd LIKE ('uncollected_%')
    AND (ptc.name ilike '%fees_due%' OR ptc.name ilike '%principal_due%' OR ptc.name ilike '%interest_due%')
    ) then 1 else 0 end as first_statement_initial_default_flag

from loans l

left join oec.oec_statements os on os.id =
(select min(id) from oec.oec_statements where loan_id = l.id)

left join customer_sources cs on cs.id =
(select id from customer_sources where customer_id = l.customer_id and created_on between l.requested_time - interval '3 days' and l.requested_time + interval '30 minutes' and type_cd in ('import','lead_reject_import','pass_active_customer') order by id asc limit 1)

left join approvals a on a.id =
(select id from approvals where customer_id = l.customer_id and processed_on <= l.requested_time order by id desc limit 1)

left join lexis_nexis.risk_view_reports rv ON rv.id = a.risk_view_report_id

where l.loan_type_cd = 'oec'
and l.base_loan_id is null
and l.status_cd not in ('declined','withdrawn','on_hold','approved','applied')
and os.due_date between '2014-09-01' and '2016-12-31'
--limit 1000
);

create index loc_bad_idx1 on mdalgleish.loc_bad_rates(first_statement_due_date);
create index loc_bad_idx2 on mdalgleish.loc_bad_rates(loan_id);
create index loc_bad_idx3 on mdalgleish.loc_bad_rates(state);
create index loc_bad_idx4 on mdalgleish.loc_bad_rates(lead_provider);
create index loc_bad_idx5 on mdalgleish.loc_bad_rates(first_statement_id);
