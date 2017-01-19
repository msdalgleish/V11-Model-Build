
create or replace function pg_temp.temp_genser() returns setof int as $$
select * from generate_series(1,30,1) as g(x);
$$ Language sql rows 30
;

commit;


-- drop table if exists mdalgleish.customer_net_fees;
-- create table mdalgleish.customer_net_fees as

-- drop table if exists mdalgleish.net_fees_prestatus;
-- create table mdalgleish.net_fees_prestatus as

-- drop table if exists first_funded;
-- create temp table first_funded as
with first_funded as
(
select l.*
from loans l

left join loans l2
    on l2.customer_id = l.customer_id
    and l2.id < l.id
    and l2.status_cd in ('issued','issued_pmt_proc','in_default','in_default_pmt_proc','paid_off')

where 1=1
    and l2.id is null
    and l.status_cd in ('issued','issued_pmt_proc','in_default','in_default_pmt_proc','paid_off')

--THIS CONTROLS HOW OLD YOUR CUSTOMERS ARE
    and l.funding_date_actual >= current_date - interval'36 month'


-- and l.customer_id = (21663720)
-- and l.customer_id = (11971531)
and l.customer_id in (21663720,21700534)
-- order by l.customer_id
)



-- drop table if exists mdalgleish.customer_net_fees;
-- create table mdalgleish.customer_net_fees as
-- drop table if exists net_fees_prestatus;
-- create temp table net_fees_prestatus as
,net_fees_prestatus as
(
select t.customer_id
    ,t.loan_id as first_funded_loan_id
    ,t.funding_date_actual as first_funding_date_actual
    ,months as month_calculated
    ,((tm.months * length_of_month) * interval'1 day' + t.funding_date_actual)::date as calc_date

--  ,ls.status_cd
--
--     ,sum(coalesce(
--         case when ls.status_cd in ('in_default','in_default_pmt_proc','paid_off')
--             and t.sub_type_cd not in ('customer_balance_reconciliation')
--             then (total_amount - coalesce(misc_fees,0))
--         when t.sub_type_cd <> 'principal_out'
--             and t.sub_type_cd in ('customer_balance_reconciliation','payment')
--             then (interest /*+ misc_fees*/)
--         end
--         ,0)
--         ) as net_fees
--     ,sum(coalesce(
--         case when cust_status in ('in_default','in_default_pmt_proc','paid_off')
--             and t.sub_type_cd not in ('customer_balance_reconciliation')
--             then (total_amount - coalesce(misc_fees,0))
--         when t.sub_type_cd <> 'principal_out'
--             and t.sub_type_cd in ('customer_balance_reconciliation','payment')
--             then (interest /*+ misc_fees*/)
--         end
--         ,0)
--         ) as net_fees
    ,sum(coalesce(
        case when t.sub_type_cd not in ('customer_balance_reconciliation')
            then (total_amount - coalesce(misc_fees,0))
        end
        ,0)
        ) as net_fees_final_status
    ,sum(coalesce(
    Case when t.sub_type_cd <> 'principal_out'
            and t.sub_type_cd in ('customer_balance_reconciliation','payment')
            then (interest /*+ misc_fees*/)
        End
        ,0)
        ) as net_fees_in_flight


from

(
select l.customer_id
    ,l.id as loan_id
    ,l.funding_date_actual
    ,apcf.loan_task_committed_id
    ,apcf.base_loan_id
    ,apcf.loan_id as tallying_loan_id
    ,apcf.total_amount
    ,apcf.principal
    ,apcf.interest
    ,apcf.misc_fees
    ,apcf.sub_type_cd
    ,apcf.eff_date
--  ,max(apcf.eff_date) over (PARTITION BY apcf.base_loan_id) as max_eff_date
--  ,lc.loan_chain_end
--  ,lc.loan_type_cd
    ,apcf.eff_date-l.funding_date_actual as days_since_funding
    ,current_date-l.funding_date_actual as days_to_today
--     ,ls.status_cd as cust_status

/*
  Change these variables to alter the NPV calculation
  NOTE: I (Jon F) tried using a temp table to store these values however
  it negatively affect performance.
  NOTE: If you change these numbers you must also change them in the update scripts
-- INPUT_VARIABLES below
*/
--  ,0.12::numeric as wacc
    ,30.4375::numeric as length_of_month
    ,365.25::numeric as length_of_year


-- from loans l
from first_funded l

-- left join loans l2
--     on l2.customer_id = l.customer_id
--     and l2.id < l.id
--     and l2.status_cd in ('issued','issued_pmt_proc','in_default','in_default_pmt_proc','paid_off')

left join bus_analytics.all_products_cash_flow apcf
    on apcf.customer_id = l.customer_id
    and apcf.status_cd in ('created','ach')
    and apcf.sub_type_cd in ('principal_out','payment','customer_balance_reconciliation')




-- where 1=1
--     and l2.id is null
--     and l.status_cd in ('issued','issued_pmt_proc','in_default','in_default_pmt_proc','paid_off')

--THIS CONTROLS HOW OLD YOUR CUSTOMERS ARE
--     and l.funding_date_actual >= current_date - interval'36 month'

--  and l.customer_id = 17459357
-- and l.customer_id = (21586961)
)t

/*
  Note: I (Jon F) found that when using generate_series() by itself the query planner
  would inaccurately estimate the number of rows hence the temp function that is just
  the generate series function rewraped.
*/
inner join (    (select b as months
        from pg_temp.temp_genser() b
--      union select -1
            )
        ) tm
--      The -1 is added to calculate the NPV to date
--     on tm.months =-1
    on (days_since_funding/length_of_month <= tm.months
    and days_to_today/length_of_month >=tm.months)

-- left join loan_statuses ls
--  on ls.id = (
--      select id
--      from loan_statuses ls9
--      where ls9.loan_id = t.tallying_loan_id
--          and ls9.eff_start_time::date <= (tm.months * length_of_month) * interval'1 day' + t.funding_date_actual
--      order by id desc
--      limit 1
--      )
-- left join loan_statuses ls
--     on ls.loan_id = t.tallying_loan_id
--     and ls.eff_start_time::Date <= (tm.months * length_of_month) * interval'1 day' + t.funding_date_actual
-- --     and ls.eff_start_time::Date >= current_date - interval'37 month'
-- left join loan_statuses ls2
--     on ls2.loan_id = t.tallying_loan_id
--     and ls2.eff_start_time::date <= (tm.months * length_of_month) * interval'1 day' + t.funding_date_actual
-- --     and ls2.eff_start_time::date >= current_date - interval'37 month'
--     and ls2.id > ls.id
-- where ls2.id is null

group by customer_id,t.loan_id,funding_date_actual,months
    ,(tm.months * length_of_month) * interval'1 day' + t.funding_date_actual

-- limit 1000
order by customer_id,month_calculated -- Useful for testing, otherwise your months get all out of order but slow for full build
)
-- ;


-- drop table if exists mdalgleish.customer_net_fees;
-- create table mdalgleish.customer_net_fees as
select n.customer_id
    ,n.first_funded_loan_id
    ,n.first_funding_date_actual
    ,n.month_calculated
    ,calc_date
    ,l.id
    ,l2.id
     ,case when ls.status_cd in ('in_default','in_default_pmt_proc','paid_off')
         then net_fees_final_status
         else net_fees_in_flight
         end as net_fees
from net_fees_prestatus n
left join loans l
    on l.customer_id = n.customer_id
    and l.funding_date_actual <= n.calc_date
    and ma.issued(l.status_cd)
left join loans l2
    on l2.customer_id = n.customer_id
    and l2.funding_date_actual <= n.calc_date
    and ma.issued(l2.status_cd)
    and l2.id > l.id
 left join loan_statuses ls
     on ls.loan_id = l.id
     and ls.eff_start_time::Date <= calc_date
-- --     and ls.eff_start_time::Date >= current_date - interval'37 month'
 left join loan_statuses ls2
     on ls2.loan_id = l.id
     and ls2.eff_start_time::date <= calc_date
-- --     and ls2.eff_start_time::date >= current_date - interval'37 month'
     and ls2.id > ls.id
where 1=1
     and ls2.id is null
    and l2.id is null
-- limit 100
order by customer_id,month_calculated
;



-------------------------------------------------------------------
-------------------------------------------------------------------
-------------------------------------------------------------------
-------------------------------------------------------------------
-------------------------------------------------------------------
-------------------------------------------------------------------
-------------------------------------------------------------------
-------------------------------------------------------------------
-------------------------------------------------------------------
-------------------------------------------------------------------
-------------------------------------------------------------------





drop table if exists ohtx_net_fees;
create temp table ohtx_net_fees as
 SELECT * FROM dblink('dbname=staging host=eis-reporting-masterdb.enova.com user=analyticsnhu password=fj024r0fmmcv4',
    $token2$




with txoh_cust as (

select *    --Nested query in case we want to limit by funding date or other customer/loan criteria
from(

select distinct on (l.account_id)
    l.loan_id as first_funded_loan_id
    ,l.account_id
    ,ac.public_identifier::int as cnu_customer_id
    ,l.disbursement_date as first_funding_date
    ,current_date - least(cl.funding_date_actual,l.disbursement_date) as days_since_first_funding
    ,l.region_id
    ,case when l.region_id = 43 then 'TX'
        when l.region_id = 35 then 'OH'
        else 'You done gone goofed'
        end as state    --State w/ error flag if other states start showing up
    ,ls.loan_status
    ,cl.id as cnu_loan_id
    ,cl.funding_date_actual as cnu_funding_date_actual
from nbox_portfolio_portfolio.loans l
inner join nbox_portfolio_portfolio.loan_applications la
    using (loan_agreement_id)
inner join nbox_portfolio_portfolio.loan_statuses ls
    using (loan_status_id)
inner join nbox_identity_identity.accounts ac
    on ac.account_id = l.account_id
-- inner join preapp_cust_list pcl
--  on pcl.account_id = ac.account_id
left join cnuus.loans cl
    on cl.customer_id = ac.public_identifier::int
    and cl.status_cd in ('issued','issued_pmt_proc','in_default','in_default_pmt_proc','paid_off')

where 1=1   --If you want to put any criteria on customers/loans please put it in the where clause of the seemingly pointless outer query
    and ls.loan_status in (
        'current',
        'paid_off',
        'called_due',
--      'issued', --Haven't actually lent the money yet
        'past_due',
        'discharged'
         )
    and la.brand_id = 4 --Only want CNU loans
--  and l.account_id in ( 7729029 , 7706656 )
--  and cl.customer_id = 17459357

order by    --This is important b/c we are selecting distinct on. I am hoping I dont forget again and spend another hour debugging
    l.account_id
    ,first_funding_date asc
    ,l.loan_id asc
    ,days_since_first_funding desc
    ,cl.id asc
)foo
-- where days_since_first_funding < 180 --If you only want recent customers or any other critera on customers/loans
where 1=1
--  and days_since_first_funding < current_date - '2015-10-01'
--  and not exists(
--      select 1
--      from cnuus.loans cl
--      where cl.customer_id = foo.cnu_customer_id
--          and cl.status_cd in ('issued','issued_pmt_proc','in_default','in_default_pmt_proc','paid_off')
--      )
)

--What does our data look like now?
-- select * from txoh_cust
-- where loan_status = 'current'
 -- limit 1000;

 -- select state,count(1),count(account_id),count(distinct account_id) from txoh_cust group by 1;


,ohtx_cashout as (

select ot.*
    ,m.mon
    ,least(ot.first_funding_date,ot.cnu_funding_date_actual) + (m.mon * interval'1 month') as max_date_of_interest
    ,sum(l.amount) as total_cash_out
    ,sum(case when l.loan_status_id in (
                7
                ,11
                ,12
                )
            AND lsh.created_at <= least(ot.first_funding_date,ot.cnu_funding_date_actual) + (m.mon * interval'1 month')
        then l.amount / (1+0.12/365.25)^(l.disbursement_date-least(ot.first_funding_date,ot.cnu_funding_date_actual)) else 0 end) as defaulted_principal    --Track defaulted principal, ignore if loan in good standing (I hope this logic is correct, can you be in good standing without paying all your installments? If so this might need to change)
from txoh_cust ot
inner join (    --We are looking for a MoM view of the data
    select generate_series(1,48,1) as mon   --Can adjust for more or fewer months
    ) m
    on least(ot.first_funding_date,ot.cnu_funding_date_actual) + (m.mon * interval'1 month') <= current_date
left join nbox_portfolio_portfolio.loans l  --Get all those loans
    on l.account_id = ot.account_id
    and l.disbursement_date <= least(ot.first_funding_date,ot.cnu_funding_date_actual) + (m.mon * interval'1 month')    --But only the ones in the time range
    and l.loan_status_id in (
        3
        ,6
        ,7
        ,11
        ,12
        )
-- left join nbox_portfolio_portfolio.loan_statuses ls
--  on ls.loan_status_id = l.loan_status_id
--  and ls.loan_status in ( --Only want loans in a funded status
--      'current',
--      'paid_off',
--      'called_due',
-- --       'issued',
--      'past_due',
--      'discharged'
--       )
left join nbox_portfolio_portfolio.loan_status_history lsh  --Need to determine when the loan went into default
    on lsh.id = (
        select lsh9.id
        from nbox_portfolio_portfolio.loan_status_history lsh9
        where lsh9.loan_id = l.loan_id
            and l.loan_status_id in (
                    7
                    ,11
                    ,12
                    )
            and lsh9.to_status in (
                'called_due',
                'past_due',
                'discharged'
                )
        order by lsh9.id asc
        limit 1
        )
where 1=1

group by
    ot.first_funded_loan_id
    ,ot.account_id
    ,ot.first_funding_date
    ,ot.days_since_first_funding
    ,ot.region_id
    ,ot.state
    ,ot.loan_status
    ,m.mon
    ,ot.first_funding_date + (m.mon * interval'1 month')
    ,ot.cnu_customer_id
    ,ot.cnu_loan_id
    ,ot.cnu_funding_date_actual
-- order by 2,m.mon
)

--Whats our data look like now?
-- select * from ohtx_cashout where account_id in(8369245,8380897,8386116,8392511) order by 2,mon limit 1000;
-- select * from ohtx_cashout where account_id = 8959353 limit 1000;
-- select * from ohtx_cashout where account_id = 8549008 limit 1000;
-- select * from ohtx_cashout limit 1000;



, ohtx_net_cash as (

select
    ot.*
    ,round(coalesce(sum(case when (acc.account ~ 'interest'
        or acc.account ~ 'cso_fee'
        or acc.account ~ 'principal') and acc2.account = 'cso_cash'
        then e.amount else 0 end), 0), 2) as total_cash_in
    ,round(coalesce(sum(case when (acc.account ~ 'interest'
        or acc.account ~ 'cso_fee'
        or acc.account ~ 'principal') and acc2.account = 'cso_cash'
        then e.amount else 0 end) - ot.total_cash_out, -1 * total_cash_out), 2) as net_cash
    ,case when ot.defaulted_principal <> 0
        then sum(case when (acc.account ~ 'interest'
        or acc.account ~ 'cso_fee'
        or acc.account ~ 'principal') and acc2.account = 'cso_cash'
        then e.amount / (1+0.12/365.25)^(l.disbursement_date-least(ot.first_funding_date,ot.cnu_funding_date_actual)) else 0 end) - ot.defaulted_principal
        else sum(
            case
            when (acc.account ~* 'interest' or acc.account ~* 'cso_fee')
                and acc2.account = 'cso_cash'
            then e.amount / (1+0.12/365.25)^(l.disbursement_date-least(ot.first_funding_date,ot.cnu_funding_date_actual))
        end)
        end as net_fees
    ,case when sum((('interest' ~* acc.account or 'fee' ~* acc.account) or 'principal' ~* acc.account)::int) > 0    --This should probably be an xor since we dont account for an account named with both interest/fee and principal
        then 'Unaccounted for accounts in the data... Where did they come from?'
        else 'Everything little thing is gonna be alright, singing dont worry about thing...'
        end as is_broken    --A flag to determine if there are unexpected accounts, I guess you could change the text but who doesnt like Bob Marley
                    --You should probably check the results of this flag every once and awhile
    ,count(1)
    ,sum(e.amount)
from ohtx_cashout ot
inner join nbox_portfolio_portfolio.loans l
    using (account_id)
inner join nbox_portfolio_portfolio.loan_statuses ls
    using (loan_status_id)
left join nbox_portfolio_portfolio.payments p   --Get payments
    on p.loan_id = l.loan_id
    and p.effective_date <= ot.max_date_of_interest --Within date limit
    and p.payment_status_id in (3,4)    --Paid
    and p.payment_purpose_id = 2    --For loan
    and p.repayment_method_id not in (9,10)
left join nbox_portfolio_accounting.activities ac
    using(payment_id)
left join nbox_portfolio_accounting.entries e
    on e.activity_id = ac.activity_id
    and e.ledger_id = 2 --Look at operational transactions b/c financial ones suck, or not apparently operational accounts are different than financial...
left join nbox_portfolio_accounting.entry_types et
    using(entry_type_id)
left join nbox_portfolio_accounting.accounts acc    --Get the split between principal and intrest
    on acc.account_id = et.credit_account_id
left join nbox_portfolio_accounting.accounts acc2   --Get the split between principal and intrest
    on acc2.account_id = et.debit_account_id
where 1=1
    and ls.loan_status in (
        'current',
        'paid_off',
        'called_due',
--      'issued',
        'past_due',
        'discharged'
        )
group by
    ot.first_funded_loan_id
    ,ot.account_id
    ,ot.first_funding_date
    ,ot.days_since_first_funding
    ,ot.region_id
    ,ot.state
    ,ot.loan_status
    ,ot.mon
    ,ot.max_date_of_interest
    ,ot.total_cash_out
    ,ot.defaulted_principal
    ,ot.cnu_customer_id
    ,ot.cnu_loan_id
    ,ot.cnu_funding_date_actual
-- order by 2,ot.mon
)

--Whats our data look like now?
-- select * from ohtx_net_cash where account_id in(8369245,8380897,8386116,8392511) order by 2,mon limit 1000;
-- select * from ohtx_net_cash where account_id in(8959353) order by 2,mon limit 1000;
-- select * from ohtx_net_cash where cnu_loan_id is not null order by 2,mon limit 1000;
-- select * from ohtx_net_cash  order by 2,mon limit 1000;

    SELECT n.first_funded_loan_id as cab_first_funded_loan_id
        ,n.account_id
        ,n.cnu_customer_id
        ,n.first_funding_date as cab_first_funding
        ,least(n.first_funding_date, n.cnu_funding_date_actual) as true_first_funding
        ,n.state
        ,n.cnu_loan_id
        ,n.mon
        ,n.net_fees


        FROM ohtx_net_cash n
        $token2$) As
            p(
        cab_first_funded_loan_id int,
        account_id int,
        cnu_customer_id INT,
        cab_first_funding timestamp,
        true_first_funding timestamp,
        state text,
        cnu_loan_id int,
        mon int,
        net_fees numeric)
    ;
create index tmp_cabnf_custid_idx on ohtx_net_fees(cnu_customer_id);



-----------------------------------------------------------
-----------------------------------------------------------
-----------------------------------------------------------
-----------------------------------------------------------
-----------------------------------------------------------
-----------------------------------------------------------
-----------------------------------------------------------
-----------------------------------------------------------





drop table if exists mdalgleish.all_customer_net_fees;
create table mdalgleish.all_customer_net_fees as
(select n.customer_id as cnu_customer_id
    ,o.account_id
    ,n.first_funded_loan_id
    ,n.first_funding_date_actual
    ,o.cab_first_funded_loan_id
    ,o.cab_first_funding as cab_first_funding
    ,o.true_first_funding
    ,o.state
    ,n.month_calculated
    ,case when n.net_fees is null
            and o.net_fees is null
        then null
        else
            coalesce(n.net_fees,0)
            + coalesce(o.net_fees,0)
        end as net_fees

-- from mdalgleish.customer_nfpv n
from mdalgleish.customer_net_fees n
left join ohtx_net_fees o
    on o.cnu_customer_id = n.customer_id
    and o.mon = n.month_calculated
-- where n.customer_id = 17459357
--  order by month_calculated
-- limit 1000
)union(
select o.cnu_customer_id as cnu_customer_id
    ,o.account_id
    ,n.first_funded_loan_id
    ,n.first_funding_date_actual
    ,o.cab_first_funded_loan_id
    ,o.cab_first_funding as cab_first_funding
    ,o.true_first_funding
    ,o.state
    ,o.mon as month_calculated
    ,case when n.net_fees is null
            and o.net_fees is null
        then null
        else
            coalesce(n.net_fees,0)
            + coalesce(o.net_fees,0)
        end as net_fees

from ohtx_net_fees o
left join mdalgleish.customer_net_fees n
    on o.cnu_customer_id = n.customer_id
    and o.mon = n.month_calculated
where o.true_first_funding::date >= '2014-01-01'::date
-- where n.customer_id = 17459357
--  order by month_calculated
-- limit 1000
)
;
create index tmp_allnfpv_custid_idx on all_customer_net_fees(cnu_customer_id);





-- select * from ohtx_net_fees
-- order by mon
-- ;

select * from mdalgleish.all_customer_net_fees
order by month_calculated
;




    --End goal success! Boom ShAUK-a-lAUKa
--          .-"-.
--         /  ,~a\_
--         \  \__))>
--         ,) ." \
--        /  (    \
--       /   )    ;
--      /   /     /
--    ,/_."`  _.-`
--     /_/`"\\___
--          `~~~`
-- AUK! AUK! AUK!
-- If you did the Knowledge Master trivia competitions in high school you know whats up!
