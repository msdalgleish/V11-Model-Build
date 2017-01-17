-- Installment --
drop table if exists temp_loans_deduped;
select
l.id as loan_id,
l.customer_id
into temp temp_loans_deduped
from loans l
where l.loan_type_cd = 'installment'
--and exists (select id from payment_transactions_committed where loan_id = l.id and status_cd not in ('returned','cancelled','canceled') and credit_account_cd = 'disbursement_account' and debit_account_cd = 'principal' and payment_method_cd <> 'internal')
and l.base_loan_id is null
and l.funding_date_actual >= '2015-01-01'
and (select due_date from installments where loan_id = l.id and installment_number = 4)::date <= current_date - interval '90 days'
and l.status_cd not in ('declined','withdrawn','on_hold','approved','applied')
and (select count(*) from loans where customer_id = l.customer_id and id < l.id and base_loan_id is null and status_cd in ('paid_off','in_default')) = 0
limit 100
;

commit;

--select * from temp_loans_deduped limit 10;


---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


drop table if exists temp_data;
select
l.id as loan_id
,l.customer_id
,a.id as approval_id
,l.requested_time
,l.funding_date_actual
,l.due_date
,l.due_date_adjusted

,(select max(due_date) from installments where loan_id = l.id)::date as due_date_expected

,(select max(due_date) from installments where loan_id = l.id)::date - l.funding_date_actual as loan_duration_days_expected

,l.amount as loan_amount
,l.status_cd as loan_status
,a.processed_on as approval_processed
,a.amount as approval_amount
,a.default_rate as app_default_rate
,a.credit_score as app_credit_score
,a.profitability_rate as app_profitability_rate
,ba.application_id
,c.created_on as customer_created
,l.gov_law_state_cd as state

,(select count(*) from approvals where customer_id = a.customer_id and processed_on < a.processed_on - interval '3 days') as previous_application_count

,(select count(distinct processed_on::date) from approvals where customer_id = a.customer_id and processed_on < a.processed_on - interval '3 days') as previous_application_count_dist_day

,(select count(*) from customer_sources where customer_id = a.customer_id and created_on::date < a.processed_on - interval '3 days') as previous_customer_sources_count

,(select count(distinct created_on::date) from customer_sources where customer_id = a.customer_id and created_on::date < a.processed_on - interval '3 days') as previous_customer_sources_count_dist_day

,(select count(*) from customer_sources where customer_id = a.customer_id and created_on::date < a.processed_on - interval '3 days' and type_cd in ('import','lead_reject_import','pass_active_customer')) as previously_purchased_count

,case when cs2.id is not null then 1 else 0 end as appeared_in_leads_flg
,case when cs3.id is not null then 1 else 0 end as purchased_leads_flg
,case when cs4.id is not null then 1 else 0 end as rejected_leads_flg
,case when cs3.id is not null then cs3.source_type_cd else 'organic' end as lead_provider

,case when a.profitability_rate = 1 or a.reason_cd = 'pre-approved' or a.pre_approved_flg = 't' /*or a.created_by = 'auto_approve_all_pending'*/ then 1 else 0 end as pre_approved_flg

,a.risk_view_report_id
,a.subprime_id_fraud_report_id
,a.idscore_report_id
,a.clearinquiry_report_id
,a.targus_report_home_phone_id
,a.targus_report_work_phone_id
,a.flex_id_report_id
,a.clear_bank_behavior_report_id

into temp temp_data
from temp_loans_deduped tld

inner join loans l on l.id = tld.loan_id

inner join customers c on c.id = l.customer_id

left join bus_analytics.applications ba on ba.application_id =
(select application_id from bus_analytics.applications where loan_id = l.id order by application_id desc limit 1)

left join approvals a on a.id =
(select id from approvals where customer_id = l.customer_id and processed_on < l.requested_time order by id desc limit 1)

left join customer_sources cs2 on cs2.id =
(select id from customer_sources where customer_id = l.customer_id and created_on between a.processed_on - interval '3 days' and a.processed_on + interval '30 minutes' order by id asc limit 1)

left join customer_sources cs3 on cs3.id = /*must be imported*/
(select id from customer_sources where customer_id = l.customer_id and created_on between a.processed_on - interval '3 days' and a.processed_on + interval '30 minutes' and type_cd in ('import','lead_reject_import','pass_active_customer') order by id asc limit 1)

left join customer_sources cs4 on cs4.id = /*not imported*/
(select id from customer_sources where customer_id = l.customer_id and created_on between a.processed_on - interval '3 days' and a.processed_on + interval '30 minutes' and type_cd not in ('import','lead_reject_import','pass_active_customer') order by id asc limit 1)

where a.existing_flg = 'f'
;

commit;

--select * from temp_data limit 10;


---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


drop table if exists temp_reports;
select
td.*

,rv.id as rv_id
,rv.credit_report_id as rv_credit_report_id
,rv.created_on as rv_created_on
,rv.score as rv_score
,rv.addr_stability as rv_addr_stability
,rv.bankruptcy_count_03 as rv_bankruptcy_count_03
,rv.curr_addr_applicant_owned as rv_curr_addr_applicant_owned
,rv.curr_addr_last_sales_price as rv_curr_addr_last_sales_price
,rv.eviction_count as rv_eviction_count
,rv.input_addr_applicant_owned as rv_input_addr_applicant_owned
,rv.invalid_dl as rv_invalid_dl
,rv.prof_lic_count as rv_prof_lic_count
,rv.prop_purchased_count_24 as rv_prop_purchased_count_24
,rv.recent_update as rv_recent_update
,rv.srcs_confirm_id_addr_count as rv_srcs_confirm_id_addr_count
,rv.eviction_age as rv_eviction_age
,rv.age_oldest_record as rv_age_oldest_record
,rv.bankruptcy_count_60 as rv_bankruptcy_count_60
,rv.addr_recent_econ_trajectory_index as rv_addr_recent_econ_trajectory_index
,rv.prop_purchased_count_60 as rv_prop_purchased_count_60
,rv.non_derog_count as rv_non_derog_count
,rv.estimated_annual_income as rv_estimated_annual_income
,rv.wealth_index as rv_wealth_index
,rv.addr_change_count_12 as rv_addr_change_count_12
,rv.subject_ssn_count as rv_subject_ssn_count
,rv.voter_registration_record as rv_voter_registration_record
,rv.prev_addr_tax_yr as rv_prev_addr_tax_yr
,rv.derog_severity_index as rv_derog_severity_index
,rv.addr_change_count_03 as rv_addr_change_count_03
,rv.addr_change_count_24 as rv_addr_change_count_24
,rv.age_newest_record as rv_age_newest_record
,rv.best_reported_age as rv_best_reported_age
,rv.curr_addr_family_owned as rv_curr_addr_family_owned
,rv.curr_addr_len_of_res as rv_curr_addr_len_of_res
,rv.curr_addr_mortgage_type as rv_curr_addr_mortgage_type
,rv.derog_count as rv_derog_count
,rv.eviction_count_60 as rv_eviction_count_60
,rv.felony_age as rv_felony_age
,rv.felony_count as rv_felony_count
,rv.felony_count_12 as rv_felony_count_12
,rv.inferred_minimum_age as rv_inferred_minimum_age
,rv.input_addr_age_newest_record as rv_input_addr_age_newest_record
,rv.input_addr_age_oldest_record as rv_input_addr_age_oldest_record
,rv.input_addr_historical_match as rv_input_addr_historical_match
,rv.lien_federal_tax_released_total as rv_lien_federal_tax_released_total
,rv.lien_filed_count_03 as rv_lien_filed_count_03
,rv.lien_filed_count_06 as rv_lien_filed_count_06
,rv.lien_filed_count_24 as rv_lien_filed_count_24
,rv.lien_filed_count_60 as rv_lien_filed_count_60
,rv.lien_filed_total as rv_lien_filed_total
,rv.lien_released_count as rv_lien_released_count
,rv.lien_released_count_24 as rv_lien_released_count_24
,rv.lien_small_claims_filed_total as rv_lien_small_claims_filed_total
,rv.lien_tax_other_filed_total as rv_lien_tax_other_filed_total
,rv.lien_tax_other_released_total as rv_lien_tax_other_released_total
,rv.non_derog_count_60 as rv_non_derog_count_60
,rv.phone_identities_recent_count as rv_phone_identities_recent_count
,rv.prev_addr_age_last_sale as rv_prev_addr_age_last_sale
,rv.prev_addr_age_oldest_record as rv_prev_addr_age_oldest_record
,rv.prev_addr_applicant_owned as rv_prev_addr_applicant_owned
,rv.prev_addr_family_owned as rv_prev_addr_family_owned
,rv.prev_addr_tax_value as rv_prev_addr_tax_value
,rv.prop_age_newest_purchase as rv_prop_age_newest_purchase
,rv.prop_age_oldest_purchase as rv_prop_age_oldest_purchase
,rv.property_owner as rv_property_owner
,rv.prop_owned_historical_count as rv_prop_owned_historical_count
,rv.ssn_addr_recent_count as rv_ssn_addr_recent_count
,rv.sub_prime_offer_request_count_12 as rv_sub_prime_offer_request_count_12
,rv.verified_address as rv_verified_address
,rv.verified_dob as rv_verified_dob
,rv.verified_phone as rv_verified_phone
,rv.curr_addr_avm_value_12 as rv_curr_addr_avm_value_12
,rv.high_risk_credit_activity as rv_high_risk_credit_activity
,rv.eviction_count_24 as rv_eviction_count_24
,rv.sub_prime_offer_request_count as rv_sub_prime_offer_request_count
,rv.bankruptcy_count as rv_bankruptcy_count
,rv.bankruptcy_age as rv_bankruptcy_age
,rv.bankruptcy_type as rv_bankruptcy_type
,rv.bankruptcy_status as rv_bankruptcy_status
,rv.bankruptcy_count_01 as rv_bankruptcy_count_01
,rv.bankruptcy_count_06 as rv_bankruptcy_count_06
,rv.bankruptcy_count_12 as rv_bankruptcy_count_12

,csidf.id as csidf_id
,csidf.credit_report_id as csidf_credit_report_id
,csidf.created_on as csidf_created_on
,csidf.csidf_score as csidf_score
,csidf.csidf_reason_codes as csidf_reason_codes
,csidf.csidf_wph_prev_listed_cell as csidf_wph_prev_listed_cell
,csidf.csidf_wph_prev_listed_hmph as csidf_wph_prev_listed_hmph
,csidf.csidf_name_ssn_match as csidf_name_ssn_match
,csidf.csidf_valid_address as csidf_valid_address
,csidf.csidf_high_risk_address as csidf_high_risk_address
,csidf.csidf_business_address as csidf_business_address
,csidf.csidf_name_address_match as csidf_name_address_match
,csidf.csidf_ssn_birth_date_con as csidf_ssn_birth_date_con
,csidf.csidf_idfraud_indicators as csidf_idfraud_indicators
,csidf.csidf_possible_fraud_type as csidf_possible_fraud_type
,csidf.csidf_alternate_transposed as csidf_alternate_transposed
,csidf.csidf_miskeyed_same_identity as csidf_miskeyed_same_identity
,csidf.csidf_alt_ssn_possible_fraud as csidf_alt_ssn_possible_fraud
,csidf.csidf_num_inq_24_hours as csidf_num_inq_24_hours
,csidf.csidf_num_inq_7_days as csidf_num_inq_7_days
,csidf.csidf_num_inq_30_days as csidf_num_inq_30_days
,csidf.csidf_num_inq_90_days as csidf_num_inq_90_days
,csidf.csidf_num_inq_1_year as csidf_num_inq_1_year
,csidf.csidf_bank_1_min as csidf_bank_1_min
,csidf.csidf_bank_10_min as csidf_bank_10_min
,csidf.csidf_bank_1_hour as csidf_bank_1_hour
,csidf.csidf_bank_24_hours as csidf_bank_24_hours
,csidf.csidf_bank_7_days as csidf_bank_7_days
,csidf.csidf_bank_15_days as csidf_bank_15_days
,csidf.csidf_bank_30_days as csidf_bank_30_days
,csidf.csidf_bank_90_days as csidf_bank_90_days
,csidf.csidf_hmph_1_min as csidf_hmph_1_min
,csidf.csidf_hmph_10_min as csidf_hmph_10_min
,csidf.csidf_hmph_1_hour as csidf_hmph_1_hour
,csidf.csidf_hmph_24_hours as csidf_hmph_24_hours
,csidf.csidf_hmph_7_days as csidf_hmph_7_days
,csidf.csidf_hmph_15_days as csidf_hmph_15_days
,csidf.csidf_hmph_30_days as csidf_hmph_30_days
,csidf.csidf_hmph_90_days as csidf_hmph_90_days
,csidf.csidf_dl_1_min as csidf_dl_1_min
,csidf.csidf_dl_10_min as csidf_dl_10_min
,csidf.csidf_dl_1_hour as csidf_dl_1_hour
,csidf.csidf_dl_24_hours as csidf_dl_24_hours
,csidf.csidf_dl_7_days as csidf_dl_7_days
,csidf.csidf_dl_15_days as csidf_dl_15_days
,csidf.csidf_dl_30_days as csidf_dl_30_days
,csidf.csidf_dl_90_days as csidf_dl_90_days
,csidf.csidf_wkph_1_min as csidf_wkph_1_min
,csidf.csidf_wkph_10_min as csidf_wkph_10_min
,csidf.csidf_wkph_1_hour as csidf_wkph_1_hour
,csidf.csidf_wkph_24_hours as csidf_wkph_24_hours
,csidf.csidf_wkph_7_days as csidf_wkph_7_days
,csidf.csidf_wkph_15_days as csidf_wkph_15_days
,csidf.csidf_wkph_30_days as csidf_wkph_30_days
,csidf.csidf_wkph_90_days as csidf_wkph_90_days
,csidf.csidf_nmi_1_min as csidf_nmi_1_min
,csidf.csidf_nmi_10_min as csidf_nmi_10_min
,csidf.csidf_nmi_1_hour as csidf_nmi_1_hour
,csidf.csidf_nmi_24_hours as csidf_nmi_24_hours
,csidf.csidf_nmi_7_days as csidf_nmi_7_days
,csidf.csidf_nmi_15_days as csidf_nmi_15_days
,csidf.csidf_nmi_30_days as csidf_nmi_30_days
,csidf.csidf_nmi_90_days as csidf_nmi_90_days
,csidf.csidf_cell_1_min as csidf_cell_1_min
,csidf.csidf_cell_10_min as csidf_cell_10_min
,csidf.csidf_cell_1_hour as csidf_cell_1_hour
,csidf.csidf_cell_24_hours as csidf_cell_24_hours
,csidf.csidf_cell_7_days as csidf_cell_7_days
,csidf.csidf_cell_15_days as csidf_cell_15_days
,csidf.csidf_cell_30_days as csidf_cell_30_days
,csidf.csidf_cell_90_days as csidf_cell_90_days
,csidf.csidf_address_1_min as csidf_address_1_min
,csidf.csidf_address_10_min as csidf_address_10_min
,csidf.csidf_address_1_hour as csidf_address_1_hour
,csidf.csidf_address_24_hours as csidf_address_24_hours
,csidf.csidf_address_7_days as csidf_address_7_days
,csidf.csidf_address_15_days as csidf_address_15_days
,csidf.csidf_address_30_days as csidf_address_30_days
,csidf.csidf_address_90_days as csidf_address_90_days
,csidf.csidf_ssn_num_ssns as csidf_ssn_num_ssns
,csidf.csidf_ssn_num_bank_accts as csidf_ssn_num_bank_accts
,csidf.csidf_ssn_num_dl as csidf_ssn_num_dl
,csidf.csidf_ssn_num_emails as csidf_ssn_num_emails
,csidf.csidf_ssn_num_hmphs as csidf_ssn_num_hmphs
,csidf.csidf_ssn_num_cell as csidf_ssn_num_cell
,csidf.csidf_ssn_num_addresses as csidf_ssn_num_addresses
,csidf.csidf_dl_num_ssns as csidf_dl_num_ssns
,csidf.csidf_dl_num_bank_accts as csidf_dl_num_bank_accts
,csidf.csidf_dl_num_dl as csidf_dl_num_dl
,csidf.csidf_dl_num_emails as csidf_dl_num_emails
,csidf.csidf_dl_num_hmphs as csidf_dl_num_hmphs
,csidf.csidf_dl_num_cell as csidf_dl_num_cell
,csidf.csidf_dl_num_addresses as csidf_dl_num_addresses
,csidf.csidf_email_num_ssns as csidf_email_num_ssns
,csidf.csidf_email_num_bank_accts as csidf_email_num_bank_accts
,csidf.csidf_email_num_dl as csidf_email_num_dl
,csidf.csidf_email_num_emails as csidf_email_num_emails
,csidf.csidf_email_num_hmphs as csidf_email_num_hmphs
,csidf.csidf_email_num_cell as csidf_email_num_cell
,csidf.csidf_email_num_addresses as csidf_email_num_addresses
,csidf.csidf_hmph_num_ssns as csidf_hmph_num_ssns
,csidf.csidf_hmph_num_bank_accts as csidf_hmph_num_bank_accts
,csidf.csidf_hmph_num_dl as csidf_hmph_num_dl
,csidf.csidf_hmph_num_emails as csidf_hmph_num_emails
,csidf.csidf_hmph_num_hmphs as csidf_hmph_num_hmphs
,csidf.csidf_hmph_num_cell as csidf_hmph_num_cell
,csidf.csidf_hmph_num_addresses as csidf_hmph_num_addresses
,csidf.csidf_cell_num_ssns as csidf_cell_num_ssns
,csidf.csidf_cell_num_bank_accts as csidf_cell_num_bank_accts
,csidf.csidf_cell_num_dl as csidf_cell_num_dl
,csidf.csidf_cell_num_emails as csidf_cell_num_emails
,csidf.csidf_cell_num_hmphs as csidf_cell_num_hmphs
,csidf.csidf_cell_num_cell as csidf_cell_num_cell
,csidf.csidf_cell_num_addresses as csidf_cell_num_addresses
,csidf.csidf_address_num_ssns as csidf_address_num_ssns
,csidf.csidf_address_num_bank_accts as csidf_address_num_bank_accts
,csidf.csidf_address_num_dl as csidf_address_num_dl
,csidf.csidf_address_num_emails as csidf_address_num_emails
,csidf.csidf_address_num_hmphs as csidf_address_num_hmphs
,csidf.csidf_address_num_cell as csidf_address_num_cell
,csidf.csidf_address_num_addresses as csidf_address_num_addresses
,csidf.csidf_bank_num_ssns as csidf_bank_num_ssns
,csidf.csidf_bank_num_bank_accts as csidf_bank_num_bank_accts
,csidf.csidf_bank_num_dl as csidf_bank_num_dl
,csidf.csidf_bank_num_emails as csidf_bank_num_emails
,csidf.csidf_bank_num_hmphs as csidf_bank_num_hmphs
,csidf.csidf_bank_num_cell as csidf_bank_num_cell
,csidf.csidf_bank_num_addresses as csidf_bank_num_addresses
,csidf.csidf_assoc_ssn_count_ssn as csidf_assoc_ssn_count_ssn
,csidf.csidf_assoc_ssn_max_fraud_ind as csidf_assoc_ssn_max_fraud_ind
,csidf.csidf_assoc_ssn_max_micr_ssn as csidf_assoc_ssn_max_micr_ssn
,csidf.csidf_assoc_ssn_count_frauds as csidf_assoc_ssn_count_frauds
,csidf.csidf_idfraud_indicator as csidf_idfraud_indicator

,ids.id as ids_id
,ids.credit_report_id as ids_credit_report_id
,ids.created_on as ids_created_on
,ids.idscore as ids_idscore
,ids.idscore_result_code1 as ids_idscore_result_code1
,ids.idscore_result_code2 as ids_idscore_result_code2
,ids.idscore_result_code3 as ids_idscore_result_code3
,ids.idscore_result_code4 as ids_idscore_result_code4
,ids.idscore_result_code5 as ids_idscore_result_code5

,cr.id as cr_id
,cr.credit_report_id as cr_credit_report_id
,cr.created_on as cr_created_on
,cr.created_by as cr_created_by
,cr.active_military as cr_active_military
,cr.last_purchased as cr_last_purchased
,cr.last_purchased_by_group as cr_last_purchased_by_group
,cr.last_seen_by_account as cr_last_seen_by_account
,cr.last_seen_by_group as cr_last_seen_by_group
,cr.last_seen_by_location as cr_last_seen_by_location
,cr.number_of_ssns_with_bank_account as cr_number_of_ssns_with_bank_account
,cr.occupation_type as cr_occupation_type
,cr.ofac_match as cr_ofac_match
,cr.ofac_score as cr_ofac_score
,cr.social_security_birth_date_inconsistent as cr_social_security_birth_date_inconsistent
,cr.social_security_deceased as cr_social_security_deceased
,cr.social_security_valid as cr_social_security_valid

,trhp.id as trhp_id
,trhp.credit_report_id as trhp_credit_report_id
,trhp.created_on as trhp_created_on
,trhp.targus_phone_request_id as trhp_targus_phone_request_id
,trhp.cqr as trhp_cqr
,trhp.phone_verify as trhp_phone_verify
,trhp.phone_type as trhp_phone_type
,trhp.listing_type as trhp_listing_type
,trhp.valid_address as trhp_valid_address
,trhp.valid_phone as trhp_valid_phone
,trhp.phone_appears_active as trhp_phone_appears_active
,trhp.on_donot_call as trhp_on_donot_call
,trhp.near_address as trhp_near_address
,trhp.recent_change as trhp_recent_change
,trhp.phone_provider as trhp_phone_provider
,trhp.address_type as trhp_address_type
,trhp.name_based_on_phone as trhp_name_based_on_phone
,trhp.street_based_on_phone as trhp_street_based_on_phone
,trhp.city_based_on_phone as trhp_city_based_on_phone
,trhp.state_based_on_phone as trhp_state_based_on_phone
,trhp.zip_based_on_phone as trhp_zip_based_on_phone
,trhp.name_based_on_address as trhp_name_based_on_address
,trhp.street_based_on_address as trhp_street_based_on_address
,trhp.city_based_on_address as trhp_city_based_on_address
,trhp.state_based_on_address as trhp_state_based_on_address
,trhp.zip_based_on_address as trhp_zip_based_on_address
,trhp.phone_based_on_address as trhp_phone_based_on_address
,trhp.std_address_line1 as trhp_std_address_line1
,trhp.std_address_line2 as trhp_std_address_line2

,trwp.id as trwp_id
,trwp.credit_report_id as trwp_credit_report_id
,trwp.created_on as trwp_created_on
,trwp.targus_phone_request_id as trwp_targus_phone_request_id
,trwp.cqr as trwp_cqr
,trwp.phone_verify as trwp_phone_verify
,trwp.phone_type as trwp_phone_type
,trwp.listing_type as trwp_listing_type
,trwp.valid_address as trwp_valid_address
,trwp.valid_phone as trwp_valid_phone
,trwp.phone_appears_active as trwp_phone_appears_active
,trwp.on_donot_call as trwp_on_donot_call
,trwp.near_address as trwp_near_address
,trwp.recent_change as trwp_recent_change
,trwp.phone_provider as trwp_phone_provider
,trwp.address_type as trwp_address_type
,trwp.name_based_on_phone as trwp_name_based_on_phone
,trwp.street_based_on_phone as trwp_street_based_on_phone
,trwp.city_based_on_phone as trwp_city_based_on_phone
,trwp.state_based_on_phone as trwp_state_based_on_phone
,trwp.zip_based_on_phone as trwp_zip_based_on_phone
,trwp.name_based_on_address as trwp_name_based_on_address
,trwp.street_based_on_address as trwp_street_based_on_address
,trwp.city_based_on_address as trwp_city_based_on_address
,trwp.state_based_on_address as trwp_state_based_on_address
,trwp.zip_based_on_address as trwp_zip_based_on_address
,trwp.phone_based_on_address as trwp_phone_based_on_address
,trwp.std_address_line1 as trwp_std_address_line1
,trwp.std_address_line2 as trwp_std_address_line2

,cbb.id as cbb_id
,cbb.credit_report_id as cbb_credit_report_id
,cbb.created_on as cbb_created_on
,cbb.score as cbb_score
,cbb.estimated_bank_history as cbb_estimated_bank_history
,cbb.number_of_accounts_all as cbb_number_of_accounts_all
,cbb.number_of_accounts_active as cbb_number_of_accounts_active
,cbb.number_of_accounts_with_check_history as cbb_number_of_accounts_with_check_history
,cbb.positive_check_writing_history as cbb_positive_check_writing_history
,cbb.check_cashing_history as cbb_check_cashing_history
,cbb.days_since_last_check_cashing_activity as cbb_days_since_last_check_cashing_activity
,cbb.days_since_last_successful_check_cashed as cbb_days_since_last_successful_check_cashed
,cbb.number_of_accounts_at_high_risk_banks as cbb_number_of_accounts_at_high_risk_banks
,cbb.number_of_high_risk_accounts as cbb_number_of_high_risk_accounts
,cbb.number_of_low_risk_accounts as cbb_number_of_low_risk_accounts
,cbb.number_of_unknown_risk_accounts as cbb_number_of_unknown_risk_accounts
,cbb.number_of_accounts_with_default_history as cbb_number_of_accounts_with_default_history
,cbb.number_of_accounts_linked_to_fraud as cbb_number_of_accounts_linked_to_fraud
,cbb.number_of_accounts_with_alternate_ssns as cbb_number_of_accounts_with_alternate_ssns
,cbb.max_number_of_ssns_with_micr as cbb_max_number_of_ssns_with_micr
,cbb.cbb_nonscorable_reason as cbb_cbb_nonscorable_reason
,cbb.primary_account_has_retail_check_writing_history as cbb_primary_account_has_retail_check_writing_history
,cbb.primary_account_has_negative_retail_check_writing_history as cbb_primary_account_has_negative_retail_check_writing_history
,cbb.primary_account_has_positive_retail_check_writing_history as cbb_primary_account_has_positive_retail_check_writing_history
,cbb.primary_account_has_no_retail_check_writing_history as cbb_primary_account_has_no_retail_check_writing_history
,cbb.primary_account_ownership_confirmed as cbb_primary_account_ownership_confirmed
,cbb.primary_account_first_seen_by_clarity_in_last_30_days as cbb_primary_account_first_seen_by_clarity_in_last_30_days
,cbb.primary_account_first_seen_by_clarity_in_31_to_60_days as cbb_primary_account_first_seen_by_clarity_in_31_to_60_days
,cbb.primary_account_first_seen_by_clarity_in_61_to_90_days as cbb_primary_account_first_seen_by_clarity_in_61_to_90_days
,cbb.primary_account_first_seen_by_clarity_in_91_to_180_days as cbb_primary_account_first_seen_by_clarity_in_91_to_180_days
,cbb.primary_account_may_be_linked_to_a_prepaid_card as cbb_primary_account_may_be_linked_to_a_prepaid_card
,cbb.primary_account_may_be_fraudulent as cbb_primary_account_may_be_fraudulent
,cbb.primary_account_is_closed as cbb_primary_account_is_closed
,cbb.primary_account_was_closed_for_suspected_fraud as cbb_primary_account_was_closed_for_suspected_fraud
,cbb.primary_account_was_closed_for_abuse as cbb_primary_account_was_closed_for_abuse
,cbb.primary_account_was_paid_closed as cbb_primary_account_was_paid_closed
,cbb.primary_account_was_unpaid_closed as cbb_primary_account_was_unpaid_closed
,cbb.primary_account_was_closed_within_30_days as cbb_primary_account_was_closed_within_30_days
,cbb.primary_account_was_closed_within_31_to_60_days as cbb_primary_account_was_closed_within_31_to_60_days
,cbb.primary_account_was_closed_within_61_to_90_days as cbb_primary_account_was_closed_within_61_to_90_days
,cbb.primary_account_was_closed_within_91_to_180_days as cbb_primary_account_was_closed_within_91_to_180_days
,cbb.primary_account_was_closed_within_180_to_365_days as cbb_primary_account_was_closed_within_180_to_365_days
,cbb.primary_account_was_closed_within_1_to_2_years as cbb_primary_account_was_closed_within_1_to_2_years
,cbb.primary_account_was_closed_within_2_to_3_years as cbb_primary_account_was_closed_within_2_to_3_years
,cbb.primary_account_was_closed_within_3_to_4_years as cbb_primary_account_was_closed_within_3_to_4_years
,cbb.primary_account_was_closed_within_4_to_5_years as cbb_primary_account_was_closed_within_4_to_5_years
,cbb.primary_account_has_a_return_item as cbb_primary_account_has_a_return_item
,cbb.primary_account_has_an_unpaid_return_item as cbb_primary_account_has_an_unpaid_return_item
,cbb.primary_account_has_a_paid_return_item as cbb_primary_account_has_a_paid_return_item
,cbb.primary_account_has_a_return_item_within_30_days as cbb_primary_account_has_a_return_item_within_30_days
,cbb.primary_account_has_a_return_item_within_31_to_60_days as cbb_primary_account_has_a_return_item_within_31_to_60_days
,cbb.primary_account_has_a_return_item_within_61_to_90_days as cbb_primary_account_has_a_return_item_within_61_to_90_days
,cbb.primary_account_has_a_return_item_within_91_to_180_days as cbb_primary_account_has_a_return_item_within_91_to_180_days
,cbb.primary_account_has_a_return_item_within_180_to_365_days as cbb_primary_account_has_a_return_item_within_180_to_365_days
,cbb.primary_account_has_a_return_item_within_1_to_2_years as cbb_primary_account_has_a_return_item_within_1_to_2_years
,cbb.primary_account_has_a_return_item_within_2_to_3_years as cbb_primary_account_has_a_return_item_within_2_to_3_years
,cbb.primary_account_dl_return_item_does_not_match_current_inquiry as cbb_primary_account_dl_return_item_does_not_match_current_inquiry
,cbb.primary_account_dl_return_item_does_not_match_current_num_inq as cbb_primary_account_dl_return_item_does_not_match_current_num_inq
,cbb.secondary_account_has_retail_check_writing_history as cbb_secondary_account_has_retail_check_writing_history
,cbb.secondary_account_has_negative_retail_check_writing_history as cbb_secondary_account_has_negative_retail_check_writing_history
,cbb.secondary_account_has_positive_retail_check_writing_history as cbb_secondary_account_has_positive_retail_check_writing_history
,cbb.secondary_account_has_no_retail_check_writing_history as cbb_secondary_account_has_no_retail_check_writing_history
,cbb.secondary_account_ownership_confirmed as cbb_secondary_account_ownership_confirmed
,cbb.secondary_account_first_seen_by_clarity_in_last_30_days as cbb_secondary_account_first_seen_by_clarity_in_last_30_days
,cbb.secondary_account_first_seen_by_clarity_in_31_to_60_days as cbb_secondary_account_first_seen_by_clarity_in_31_to_60_days
,cbb.secondary_account_first_seen_by_clarity_in_61_to_90_days as cbb_secondary_account_first_seen_by_clarity_in_61_to_90_days
,cbb.secondary_account_first_seen_by_clarity_in_91_to_180_days as cbb_secondary_account_first_seen_by_clarity_in_91_to_180_days
,cbb.secondary_account_may_be_linked_to_a_prepaid_card as cbb_secondary_account_may_be_linked_to_a_prepaid_card
,cbb.secondary_account_may_be_fraudulent as cbb_secondary_account_may_be_fraudulent
,cbb.secondary_account_is_closed as cbb_secondary_account_is_closed
,cbb.secondary_account_was_closed_for_suspected_fraud as cbb_secondary_account_was_closed_for_suspected_fraud
,cbb.secondary_account_was_closed_for_abuse as cbb_secondary_account_was_closed_for_abuse
,cbb.secondary_account_was_paid_closed as cbb_secondary_account_was_paid_closed
,cbb.secondary_account_was_unpaid_closed as cbb_secondary_account_was_unpaid_closed
,cbb.secondary_account_was_closed_within_30_days as cbb_secondary_account_was_closed_within_30_days
,cbb.secondary_account_was_closed_within_31_to_60_days as cbb_secondary_account_was_closed_within_31_to_60_days
,cbb.secondary_account_was_closed_within_61_to_90_days as cbb_secondary_account_was_closed_within_61_to_90_days
,cbb.secondary_account_was_closed_within_91_to_180_days as cbb_secondary_account_was_closed_within_91_to_180_days
,cbb.secondary_account_was_closed_within_180_to_365_days as cbb_secondary_account_was_closed_within_180_to_365_days
,cbb.secondary_account_was_closed_within_1_to_2_years as cbb_secondary_account_was_closed_within_1_to_2_years
,cbb.secondary_account_was_closed_within_2_to_3_years as cbb_secondary_account_was_closed_within_2_to_3_years
,cbb.secondary_account_was_closed_within_3_to_4_years as cbb_secondary_account_was_closed_within_3_to_4_years
,cbb.secondary_account_was_closed_within_4_to_5_years as cbb_secondary_account_was_closed_within_4_to_5_years
,cbb.secondary_account_has_a_return_item as cbb_secondary_account_has_a_return_item
,cbb.secondary_account_has_an_unpaid_return_item as cbb_secondary_account_has_an_unpaid_return_item
,cbb.secondary_account_has_a_paid_return_item as cbb_secondary_account_has_a_paid_return_item
,cbb.secondary_account_has_a_return_item_within_30_days as cbb_secondary_account_has_a_return_item_within_30_days
,cbb.secondary_account_has_a_return_item_within_31_to_60_days as cbb_secondary_account_has_a_return_item_within_31_to_60_days
,cbb.secondary_account_has_a_return_item_within_61_to_90_days as cbb_secondary_account_has_a_return_item_within_61_to_90_days
,cbb.secondary_account_has_a_return_item_within_91_to_180_days as cbb_secondary_account_has_a_return_item_within_91_to_180_days
,cbb.secondary_account_has_a_return_item_within_180_to_365_days as cbb_secondary_account_has_a_return_item_within_180_to_365_days
,cbb.secondary_account_has_a_return_item_within_1_to_2_years as cbb_secondary_account_has_a_return_item_within_1_to_2_years
,cbb.secondary_account_has_a_return_item_within_2_to_3_years as cbb_secondary_account_has_a_return_item_within_2_to_3_years
,cbb.secondary_account_dl_return_item_does_not_match_current_inquiry as cbb_secondary_account_dl_return_item_does_not_match_current_inquiry
,cbb.secondary_account_dl_return_item_does_not_match_current_num_inq as cbb_secondary_account_dl_return_item_does_not_match_current_num_inq
,cbb.no_secondary_accounts as cbb_no_secondary_accounts
,cbb.concurrent_use_of_multiple_accounts_in_recent_applications as cbb_concurrent_use_of_multiple_accounts_in_recent_applications
,cbb.inquiry_cluster_account_stability_one_hour_ago as cbb_inquiry_cluster_account_stability_one_hour_ago
,cbb.inquiry_cluster_account_stability_twentyfour_hours_ago as cbb_inquiry_cluster_account_stability_twentyfour_hours_ago
,cbb.inquiry_cluster_account_stability_seven_days_ago as cbb_inquiry_cluster_account_stability_seven_days_ago
,cbb.inquiry_cluster_account_stability_fifteen_days_ago as cbb_inquiry_cluster_account_stability_fifteen_days_ago
,cbb.inquiry_cluster_account_stability_thirty_days_ago as cbb_inquiry_cluster_account_stability_thirty_days_ago
,cbb.inquiry_cluster_account_stability_ninety_days_ago as cbb_inquiry_cluster_account_stability_ninety_days_ago
,cbb.inquiry_cluster_account_stability_one_hundred_eighty_days_ago as cbb_inquiry_cluster_account_stability_one_hundred_eighty_days_ago
,cbb.inquiry_cluster_account_stability_one_year_ago as cbb_inquiry_cluster_account_stability_one_year_ago
,cbb.account_stability_one_hour_ago as cbb_account_stability_one_hour_ago
,cbb.account_stability_twentyfour_hours_ago as cbb_account_stability_twentyfour_hours_ago
,cbb.account_stability_seven_days_ago as cbb_account_stability_seven_days_ago
,cbb.account_stability_fifteen_days_ago as cbb_account_stability_fifteen_days_ago
,cbb.account_stability_thirty_days_ago as cbb_account_stability_thirty_days_ago
,cbb.account_stability_ninety_days_ago as cbb_account_stability_ninety_days_ago
,cbb.account_stability_one_hundred_eighty_days_ago as cbb_account_stability_one_hundred_eighty_days_ago
,cbb.account_stability_one_year_ago as cbb_account_stability_one_year_ago
,cbb.inquiry_cluster_stability_one_hour_ago as cbb_inquiry_cluster_stability_one_hour_ago
,cbb.inquiry_cluster_stability_twentyfour_hours_ago as cbb_inquiry_cluster_stability_twentyfour_hours_ago
,cbb.inquiry_cluster_stability_seven_days_ago as cbb_inquiry_cluster_stability_seven_days_ago
,cbb.inquiry_cluster_stability_fifteen_days_ago as cbb_inquiry_cluster_stability_fifteen_days_ago
,cbb.inquiry_cluster_stability_thirty_days_ago as cbb_inquiry_cluster_stability_thirty_days_ago
,cbb.inquiry_cluster_stability_ninety_days_ago as cbb_inquiry_cluster_stability_ninety_days_ago
,cbb.inquiry_cluster_stability_one_hundred_eighty_days_ago as cbb_inquiry_cluster_stability_one_hundred_eighty_days_ago
,cbb.inquiry_cluster_stability_one_year_ago as cbb_inquiry_cluster_stability_one_year_ago
,cbb.micr_ssn_24months as cbb_micr_ssn_24months
,cbb.micr_ssn_24_months_attempted as cbb_micr_ssn_24_months_attempted
,cbb.consumer_privacy_message_text as cbb_consumer_privacy_message_text
,cbb.no_fraud_closures as cbb_no_fraud_closures
,cbb.government_number_validation_message_text as cbb_government_number_validation_message_text
,cbb.debit_bureau_score as cbb_debit_bureau_score
,cbb.amount_fraud_closures_thirty_days_ago as cbb_amount_fraud_closures_thirty_days_ago
,cbb.amount_fraud_closures_sixty_days_ago as cbb_amount_fraud_closures_sixty_days_ago
,cbb.amount_fraud_closures_ninety_days_ago as cbb_amount_fraud_closures_ninety_days_ago
,cbb.amount_fraud_closures_one_hundred_eighty_days_ago as cbb_amount_fraud_closures_one_hundred_eighty_days_ago
,cbb.amount_fraud_closures_one_year_ago as cbb_amount_fraud_closures_one_year_ago
,cbb.amount_fraud_closures_two_years_ago as cbb_amount_fraud_closures_two_years_ago
,cbb.amount_fraud_closures_three_years_ago as cbb_amount_fraud_closures_three_years_ago
,cbb.amount_fraud_closures_four_years_ago as cbb_amount_fraud_closures_four_years_ago
,cbb.amount_fraud_closures_five_years_ago as cbb_amount_fraud_closures_five_years_ago
,cbb.number_fraud_closures_thirty_days_ago as cbb_number_fraud_closures_thirty_days_ago
,cbb.number_fraud_closures_sixty_days_ago as cbb_number_fraud_closures_sixty_days_ago
,cbb.number_fraud_closures_ninety_days_ago as cbb_number_fraud_closures_ninety_days_ago
,cbb.number_fraud_closures_one_hundred_eighty_days_ago as cbb_number_fraud_closures_one_hundred_eighty_days_ago
,cbb.number_fraud_closures_one_year_ago as cbb_number_fraud_closures_one_year_ago
,cbb.number_fraud_closures_two_years_ago as cbb_number_fraud_closures_two_years_ago
,cbb.number_fraud_closures_three_years_ago as cbb_number_fraud_closures_three_years_ago
,cbb.number_fraud_closures_four_years_ago as cbb_number_fraud_closures_four_years_ago
,cbb.number_fraud_closures_five_years_ago as cbb_number_fraud_closures_five_years_ago
,cbb.number_of_payday_inquiries_thirty_days_ago as cbb_number_of_payday_inquiries_thirty_days_ago
,cbb.number_of_payday_inquiries_sixty_days_ago as cbb_number_of_payday_inquiries_sixty_days_ago
,cbb.number_of_payday_inquiries_ninety_days_ago as cbb_number_of_payday_inquiries_ninety_days_ago
,cbb.number_of_payday_inquiries_one_hundred_eighty_days_ago as cbb_number_of_payday_inquiries_one_hundred_eighty_days_ago
,cbb.number_of_payday_inquiries_one_year_ago as cbb_number_of_payday_inquiries_one_year_ago
,cbb.number_of_payday_inquiries_two_years_ago as cbb_number_of_payday_inquiries_two_years_ago
,cbb.number_of_payday_inquiries_three_years_ago as cbb_number_of_payday_inquiries_three_years_ago
,cbb.days_since_first_payday_inquiry as cbb_days_since_first_payday_inquiry
,cbb.days_since_last_payday_inquiry as cbb_days_since_last_payday_inquiry
,cbb.number_of_inquiries_thirty_days_ago as cbb_number_of_inquiries_thirty_days_ago
,cbb.number_of_inquiries_sixty_days_ago as cbb_number_of_inquiries_sixty_days_ago
,cbb.number_of_inquiries_ninety_days_ago as cbb_number_of_inquiries_ninety_days_ago
,cbb.number_of_inquiries_one_hundred_eighty_days_ago as cbb_number_of_inquiries_one_hundred_eighty_days_ago
,cbb.number_of_inquiries_one_year_ago as cbb_number_of_inquiries_one_year_ago
,cbb.number_of_inquiries_two_years_ago as cbb_number_of_inquiries_two_years_ago
,cbb.number_of_inquiries_three_years_ago as cbb_number_of_inquiries_three_years_ago
,cbb.default_rate_60_days_ago as cbb_default_rate_60_days_ago
,cbb.default_rate_61_365_days_ago as cbb_default_rate_61_365_days_ago
,cbb.days_since_first_seen_by_clarity as cbb_days_since_first_seen_by_clarity
,cbb.inquiries_30_days_ago as cbb_inquiries_30_days_ago
,cbb.inquiries_ratio as cbb_inquiries_ratio
,cbb.number_of_ssns as cbb_number_of_ssns
,cbb.stability_thirty_days_ago as cbb_stability_thirty_days_ago
,cbb.days_since_first_seen_by_clarity_account_3 as cbb_days_since_first_seen_by_clarity_account_3
,cbb.reason_codes as cbb_reason_codes
,cbb.amount_of_checks_attempted_180_days_ago as cbb_amount_of_checks_attempted_180_days_ago
,cbb.number_checks_cashed as cbb_number_checks_cashed
,cbb.number_checks_cashed_30_days_ago as cbb_number_checks_cashed_30_days_ago
,cbb.amount_closures_unpaid_one_year_ago as cbb_amount_closures_unpaid_one_year_ago
,cbb.amount_closures_unpaid_three_years_ago as cbb_amount_closures_unpaid_three_years_ago
,cbb.amount_closures_unpaid_four_years_ago as cbb_amount_closures_unpaid_four_years_ago
,cbb.amount_closures_unpaid_five_years_ago as cbb_amount_closures_unpaid_five_years_ago
,cbb.max_amount_open_item_3_years_ago as cbb_max_amount_open_item_3_years_ago
,cbb.number_of_non_dda_inquiries_3_years_ago as cbb_number_of_non_dda_inquiries_3_years_ago
,cbb.number_of_non_dda_inquiries_60_days_ago as cbb_number_of_non_dda_inquiries_60_days_ago
,cbb.stability_ninety_days_ago as cbb_stability_ninety_days_ago
,cbb.count_of_checks_attempted_two_years_ago as cbb_count_of_checks_attempted_two_years_ago
,cbb.count_of_checks_cashed_one_hundred_eighty_days_ago as cbb_count_of_checks_cashed_one_hundred_eighty_days_ago
,cbb.count_of_checks_cashed_two_years_ago as cbb_count_of_checks_cashed_two_years_ago
,cbb.days_since_last_check_cashed as cbb_days_since_last_check_cashed
,cbb.number_of_closures_thirty_days_ago as cbb_number_of_closures_thirty_days_ago
,cbb.inquiries_app_state_ratio as cbb_inquiries_app_state_ratio
,cbb.number_of_non_dda_inquiries_number_since_first_inquiry as cbb_number_of_non_dda_inquiries_number_since_first_inquiry
,cbb.number_of_days_since_last_inquiry as cbb_number_of_days_since_last_inquiry
,cbb.inquiries_app_state_30_days_ago as cbb_inquiries_app_state_30_days_ago
,cbb.number_closures_unpaid_three_years_ago as cbb_number_closures_unpaid_three_years_ago
,cbb.number_closures_unpaid_five_years_ago as cbb_number_closures_unpaid_five_years_ago
,cbb.cbb_reason_codes as cbb_cbb_reason_codes
,cbb.stability_seven_days_ago as cbb_stability_seven_days_ago
,cbb.days_since_last_seen_by_clarity as cbb_days_since_last_seen_by_clarity
,cbb.days_since_validated_trade as cbb_days_since_validated_trade
,cbb.inquiries_31_365_days_ago as cbb_inquiries_31_365_days_ago
,cbb.inquiries_app_state_31_365_days_ago as cbb_inquiries_app_state_31_365_days_ago
,cbb.stability_fifteen_days_ago as cbb_stability_fifteen_days_ago
,cbb.amount_of_checks_attempted_two_years_ago as cbb_amount_of_checks_attempted_two_years_ago
,cbb.avg_amount_of_checks_cashed_one_year_ago as cbb_avg_amount_of_checks_cashed_one_year_ago
,cbb.number_checks_cashed_90_days_ago as cbb_number_checks_cashed_90_days_ago
,cbb.number_of_closures_one_year_ago as cbb_number_of_closures_one_year_ago
,cbb.number_of_closures_two_years_ago as cbb_number_of_closures_two_years_ago
,cbb.number_of_closures_five_years_ago as cbb_number_of_closures_five_years_ago
,cbb.number_of_days_since_first_inquiry as cbb_number_of_days_since_first_inquiry
,cbb.number_days_since_most_recent_closure_5_years_ago as cbb_number_days_since_most_recent_closure_5_years_ago
,cbb.number_of_non_dda_inquiries_number_since_last_inquiry as cbb_number_of_non_dda_inquiries_number_since_last_inquiry
,cbb.account_age_code as cbb_account_age_code
,cbb.avg_amount_of_checks_attempted_one_year_ago as cbb_avg_amount_of_checks_attempted_one_year_ago

,flex.id AS flex_id
,flex.credit_report_id AS flex_credit_report_id
,flex.created_on AS flex_created_on
,flex.verified_first_name::int AS flex_verified_first_name
,flex.verified_last_name::int AS flex_verified_last_name
,flex.verified_street_address::int AS flex_verified_street_address
,flex.verified_city::int AS flex_verified_city
,flex.verified_state::int AS flex_verified_state
,flex.verified_zip::int AS flex_verified_zip
,flex.verified_dob::int AS flex_verified_dob
,flex.verified_dob_match_level_id AS flex_verified_dob_match_level_id
,flex.verified_ssn AS flex_verified_ssn
,flex.verified_license AS flex_verified_license
,flex.valid_ssn AS flex_valid_ssn
,flex.valid_ssn_deceased AS flex_valid_ssn_deceased
,flex.valid_license AS flex_valid_license
,flex.valid_passport AS flex_valid_passport
,flex.name_address_ssn_id AS flex_name_address_ssn_id
,flex.verification_index_id AS flex_verification_index_id

into temp temp_reports
from temp_data td

left join lexis_nexis.risk_view_reports rv ON rv.id = td.risk_view_report_id
left join clarity.subprime_id_fraud_reports csidf ON csidf.id = td.subprime_id_fraud_report_id
left join idanalytics.idscore_reports ids ON ids.id = td.idscore_report_id
left join clearinquiry_reports cr ON cr.id = td.clearinquiry_report_id
left JOIN targus.targus_reports trhp ON trhp.id = td.targus_report_home_phone_id
left JOIN targus.targus_reports trwp ON trwp.id = td.targus_report_work_phone_id
left join clarity.behavior_reports cbb on cbb.id = td.clear_bank_behavior_report_id
left JOIN lexis_nexis.flex_id_reports flex ON flex.id = td.flex_id_report_id
;

commit;

--select * from temp_reports limit 10;


---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


drop table if exists temp_reports2;
select
tr.*

,(csidf_idfraud_indicators = '1' OR csidf_idfraud_indicators LIKE '1,%' OR csidf_idfraud_indicators LIKE '%,1,%' OR csidf_idfraud_indicators LIKE '%,1')::INTEGER as csidf_idfraud_indicators_1
,(csidf_idfraud_indicators = '2' OR csidf_idfraud_indicators LIKE '2,%' OR csidf_idfraud_indicators LIKE '%,2,%' OR csidf_idfraud_indicators LIKE '%,2')::INTEGER as csidf_idfraud_indicators_2
,(csidf_idfraud_indicators = '3' OR csidf_idfraud_indicators LIKE '3,%' OR csidf_idfraud_indicators LIKE '%,3,%' OR csidf_idfraud_indicators LIKE '%,3')::INTEGER as csidf_idfraud_indicators_3
,(csidf_idfraud_indicators = '4' OR csidf_idfraud_indicators LIKE '4,%' OR csidf_idfraud_indicators LIKE '%,4,%' OR csidf_idfraud_indicators LIKE '%,4')::INTEGER as csidf_idfraud_indicators_4
,(csidf_idfraud_indicators = '5' OR csidf_idfraud_indicators LIKE '5,%' OR csidf_idfraud_indicators LIKE '%,5,%' OR csidf_idfraud_indicators LIKE '%,5')::INTEGER as csidf_idfraud_indicators_5
,(csidf_idfraud_indicators = '6' OR csidf_idfraud_indicators LIKE '6,%' OR csidf_idfraud_indicators LIKE '%,6,%' OR csidf_idfraud_indicators LIKE '%,6')::INTEGER as csidf_idfraud_indicators_6
,(csidf_idfraud_indicators = '7' OR csidf_idfraud_indicators LIKE '7,%' OR csidf_idfraud_indicators LIKE '%,7,%' OR csidf_idfraud_indicators LIKE '%,7')::INTEGER as csidf_idfraud_indicators_7
,(csidf_idfraud_indicators = '8' OR csidf_idfraud_indicators LIKE '8,%' OR csidf_idfraud_indicators LIKE '%,8,%' OR csidf_idfraud_indicators LIKE '%,8')::INTEGER as csidf_idfraud_indicators_8
,(csidf_idfraud_indicators = '9' OR csidf_idfraud_indicators LIKE '9,%' OR csidf_idfraud_indicators LIKE '%,9,%' OR csidf_idfraud_indicators LIKE '%,9')::INTEGER as csidf_idfraud_indicators_9
,(csidf_idfraud_indicators = '10' OR csidf_idfraud_indicators LIKE '10,%' OR csidf_idfraud_indicators LIKE '%,10,%' OR csidf_idfraud_indicators LIKE '%,10')::INTEGER as csidf_idfraud_indicators_10
,(csidf_idfraud_indicators = '11' OR csidf_idfraud_indicators LIKE '11,%' OR csidf_idfraud_indicators LIKE '%,11,%' OR csidf_idfraud_indicators LIKE '%,11')::INTEGER as csidf_idfraud_indicators_11
,(csidf_idfraud_indicators = '12' OR csidf_idfraud_indicators LIKE '12,%' OR csidf_idfraud_indicators LIKE '%,12,%' OR csidf_idfraud_indicators LIKE '%,12')::INTEGER as csidf_idfraud_indicators_12
,(csidf_idfraud_indicators = '13' OR csidf_idfraud_indicators LIKE '13,%' OR csidf_idfraud_indicators LIKE '%,13,%' OR csidf_idfraud_indicators LIKE '%,13')::INTEGER as csidf_idfraud_indicators_13
,(csidf_idfraud_indicators = '14' OR csidf_idfraud_indicators LIKE '14,%' OR csidf_idfraud_indicators LIKE '%,14,%' OR csidf_idfraud_indicators LIKE '%,14')::INTEGER as csidf_idfraud_indicators_14
,(csidf_idfraud_indicators = '15' OR csidf_idfraud_indicators LIKE '15,%' OR csidf_idfraud_indicators LIKE '%,15,%' OR csidf_idfraud_indicators LIKE '%,15')::INTEGER as csidf_idfraud_indicators_15
,(csidf_idfraud_indicators = '16' OR csidf_idfraud_indicators LIKE '16,%' OR csidf_idfraud_indicators LIKE '%,16,%' OR csidf_idfraud_indicators LIKE '%,16')::INTEGER as csidf_idfraud_indicators_16
,(csidf_idfraud_indicators = '17' OR csidf_idfraud_indicators LIKE '17,%' OR csidf_idfraud_indicators LIKE '%,17,%' OR csidf_idfraud_indicators LIKE '%,17')::INTEGER as csidf_idfraud_indicators_17
,(csidf_idfraud_indicators = '18' OR csidf_idfraud_indicators LIKE '18,%' OR csidf_idfraud_indicators LIKE '%,18,%' OR csidf_idfraud_indicators LIKE '%,18')::INTEGER as csidf_idfraud_indicators_18
,(csidf_idfraud_indicators = '19' OR csidf_idfraud_indicators LIKE '19,%' OR csidf_idfraud_indicators LIKE '%,19,%' OR csidf_idfraud_indicators LIKE '%,19')::INTEGER as csidf_idfraud_indicators_19
,(csidf_idfraud_indicators = '20' OR csidf_idfraud_indicators LIKE '20,%' OR csidf_idfraud_indicators LIKE '%,20,%' OR csidf_idfraud_indicators LIKE '%,20')::INTEGER as csidf_idfraud_indicators_20
,(csidf_idfraud_indicators = '21' OR csidf_idfraud_indicators LIKE '21,%' OR csidf_idfraud_indicators LIKE '%,21,%' OR csidf_idfraud_indicators LIKE '%,21')::INTEGER as csidf_idfraud_indicators_21
,(csidf_idfraud_indicators = '25' OR csidf_idfraud_indicators LIKE '25,%' OR csidf_idfraud_indicators LIKE '%,25,%' OR csidf_idfraud_indicators LIKE '%,25')::INTEGER as csidf_idfraud_indicators_25
,(csidf_idfraud_indicators = '26' OR csidf_idfraud_indicators LIKE '26,%' OR csidf_idfraud_indicators LIKE '%,26,%' OR csidf_idfraud_indicators LIKE '%,26')::INTEGER as csidf_idfraud_indicators_26
,(csidf_idfraud_indicators = '27' OR csidf_idfraud_indicators LIKE '27,%' OR csidf_idfraud_indicators LIKE '%,27,%' OR csidf_idfraud_indicators LIKE '%,27')::INTEGER as csidf_idfraud_indicators_27
,(csidf_idfraud_indicators = '28' OR csidf_idfraud_indicators LIKE '28,%' OR csidf_idfraud_indicators LIKE '%,28,%' OR csidf_idfraud_indicators LIKE '%,28')::INTEGER as csidf_idfraud_indicators_28
,(csidf_idfraud_indicators = '29' OR csidf_idfraud_indicators LIKE '29,%' OR csidf_idfraud_indicators LIKE '%,29,%' OR csidf_idfraud_indicators LIKE '%,29')::INTEGER as csidf_idfraud_indicators_29
,(csidf_idfraud_indicators = '30' OR csidf_idfraud_indicators LIKE '30,%' OR csidf_idfraud_indicators LIKE '%,30,%' OR csidf_idfraud_indicators LIKE '%,30')::INTEGER as csidf_idfraud_indicators_30
,(csidf_reason_codes LIKE '%CL01%')::INTEGER as csidf_reason_codes_CL01
,(csidf_reason_codes LIKE '%CL02%')::INTEGER as csidf_reason_codes_CL02
,(csidf_reason_codes LIKE '%CL03%')::INTEGER as csidf_reason_codes_CL03
,(csidf_reason_codes LIKE '%CL04%')::INTEGER as csidf_reason_codes_CL04
,(csidf_reason_codes LIKE '%CL05%')::INTEGER as csidf_reason_codes_CL05
,(csidf_reason_codes LIKE '%CL06%')::INTEGER as csidf_reason_codes_CL06
,(csidf_reason_codes LIKE '%CL07%')::INTEGER as csidf_reason_codes_CL07
,(csidf_reason_codes LIKE '%CL08%')::INTEGER as csidf_reason_codes_CL08
,(csidf_reason_codes LIKE '%CL09%')::INTEGER as csidf_reason_codes_CL09
,(csidf_reason_codes LIKE '%CL10%')::INTEGER as csidf_reason_codes_CL10
,(csidf_reason_codes LIKE '%CL11%')::INTEGER as csidf_reason_codes_CL11
,(csidf_reason_codes LIKE '%CL12%')::INTEGER as csidf_reason_codes_CL12
,(csidf_reason_codes LIKE '%CL13%')::INTEGER as csidf_reason_codes_CL13
,(csidf_reason_codes LIKE '%CL14%')::INTEGER as csidf_reason_codes_CL14
,(csidf_reason_codes LIKE '%CL15%')::INTEGER as csidf_reason_codes_CL15
,(csidf_reason_codes LIKE '%CL16%')::INTEGER as csidf_reason_codes_CL16
,(csidf_reason_codes LIKE '%CL17%')::INTEGER as csidf_reason_codes_CL17
,(csidf_reason_codes LIKE '%CL18%')::INTEGER as csidf_reason_codes_CL18
,(csidf_reason_codes LIKE '%CL19%')::INTEGER as csidf_reason_codes_CL19
,(csidf_reason_codes LIKE '%CL20%')::INTEGER as csidf_reason_codes_CL20
,(csidf_reason_codes LIKE '%CL21%')::INTEGER as csidf_reason_codes_CL21
,(csidf_reason_codes LIKE '%CL22%')::INTEGER as csidf_reason_codes_CL22
,(csidf_reason_codes LIKE '%CL23%')::INTEGER as csidf_reason_codes_CL23
,(csidf_reason_codes LIKE '%C405%')::INTEGER as csidf_reason_codes_C405
,(csidf_reason_codes LIKE '%B7%')::INTEGER as csidf_reason_codes_B7
,(csidf_reason_codes LIKE '%B11%')::INTEGER as csidf_reason_codes_B11
,(csidf_reason_codes LIKE '%B12%')::INTEGER as csidf_reason_codes_B12
,(csidf_reason_codes LIKE '%B13%')::INTEGER as csidf_reason_codes_B13

into temp temp_reports2
from temp_reports tr
;

commit;

--select * from temp_reports2 limit 10;


---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


drop table if exists temp_data2;
select
tr.*

,case when EXTRACT(MONTH FROM a.processed_on) in (1,2,3) then 1 else 0 end as requested_q1
,case when EXTRACT(MONTH FROM a.processed_on) in (4,5,6) then 1 else 0 end as requested_q2
,case when EXTRACT(MONTH FROM a.processed_on) in (7,8,9) then 1 else 0 end as requested_q3
,case when EXTRACT(MONTH FROM a.processed_on) in (10,11,12) then 1 else 0 end as requested_q4
,extract(dow from a.processed_on) as weekday_application
,extract(day from a.processed_on) as monthday_application
,case when EXTRACT(HOUR from a.processed_on) + co.gmt + 6 >= 6 and EXTRACT(HOUR from a.processed_on) + co.gmt + 6 < 12 then 1 else 0 end as apply_at_localmorning
,case when EXTRACT(HOUR from a.processed_on) + co.gmt + 6 >= 12 and EXTRACT(HOUR from a.processed_on) + co.gmt + 6 < 20 then 1 else 0 end as apply_at_localafternoon
,case when EXTRACT(HOUR from a.processed_on) + co.gmt + 6 >= 20 or EXTRACT(HOUR from a.processed_on) + co.gmt + 6 < 6  then 1 else 0 end as apply_at_localevening

,COALESCE(dc.old_value, c.email) as email
,split_part(COALESCE(dc.old_value, c.email), '@', 2) AS email_domain
,case when COALESCE(dc.old_value, c.email) like '%@hotmail.co%' then 1 else 0 end as email_hotmail
,case when COALESCE(dc.old_value, c.email) like '%@gmail.com%' then 1 else 0 end as email_gmail
,case when COALESCE(dc.old_value, c.email) like '%@yahoo.co%' or COALESCE(dc.old_value, c.email) like '%@ymail.%' then 1 else 0 end as email_yahoo
,case when COALESCE(dc.old_value, c.email) like '%@ymail.%' then 1 else 0 end as email_ymail
,case when COALESCE(dc.old_value, c.email) like '%@live.co%' then 1 else 0 end as email_live
,case when COALESCE(dc.old_value, c.email) like '%@btinternet.%' then 1 else 0 end as email_btinternet
,case when COALESCE(dc.old_value, c.email) like '%@aol.%' then 1 else 0 end as email_aol
,case when COALESCE(dc.old_value, c.email) like '%@sky.%' then 1 else 0 end as email_sky
,case when COALESCE(dc.old_value, c.email) like '%@msn.%' then 1 else 0 end as email_msn

,case when (select sum(a.num_length) from (select length(ar.regexp_split_to_table) as num_length
	        from (select regexp_split_to_table(split_part(COALESCE(dc.old_value, c.email),'@',1),'[^\d]')) as ar
	        where ar.regexp_split_to_table !='') as a) is not null
      then (select sum(a.num_length) from (select length(ar.regexp_split_to_table) as num_length
    	    from (select regexp_split_to_table(split_part(COALESCE(dc.old_value, c.email),'@',1),'[^\d]')) as ar
    	    where ar.regexp_split_to_table !='') as a)
      else 0 end as email_num_digit

,position(substring(split_part(COALESCE(dc.old_value, c.email),'@',1),'\d+')
	in split_part(COALESCE(dc.old_value, c.email),'@',1)) != (length(split_part(COALESCE(dc.old_value, c.email),'@',1))-length(substring(split_part(COALESCE(dc.old_value, c.email),'@',1),'\d+'))+1)
	as email_digit_not_end

,split_part(COALESCE(dc.old_value, c.email),'@',1) like '%'||extract(year from a.processed_on)::text||'%' as email_has_current_year

,split_part(COALESCE(dc.old_value, c.email),'@',1) like '%'||extract(year from p.birth_date)::text||'%' or split_part(COALESCE(dc.old_value, c.email),'@',1) like '%'||right(extract(year from p.birth_date)::text,2)||'%' or split_part(COALESCE(dc.old_value, c.email),'@',1) like '%'||extract(month from p.birth_date)::text||extract(day from p.birth_date)::text||'%' as email_had_dob

,position('-' in split_part(COALESCE(dc.old_value, c.email),'@',1)) != 0 or position('.' in split_part(COALESCE(dc.old_value, c.email),'@',1)) != 0 or position('_' in split_part(COALESCE(dc.old_value, c.email),'@',1)) != 0 as email_has_hyphen_dot_underscore

,split_part(COALESCE(dc.old_value, c.email),'@',1) ilike '%'||p.first_name||'%' as email_has_first_name

,split_part(COALESCE(dc.old_value, c.email),'@',1) ilike '%'||p.last_name||'%' as email_has_last_name

,levenshtein(p.last_name,split_part(COALESCE(dc.old_value, c.email), '@',1)) as levenshtein_distance_lastname

,case when addr.line1 = lower(addr.line1) then 1 else 0 end as address_lower
,case when addr.line1 = upper(addr.line1) then 1 else 0 end as address_upper
,case when addr.city = lower(addr.city) then 1 else 0 end as city_lower
,case when addr.city = upper(addr.city) then 1 else 0 end as city_upper

,cp.next_paydate::date - a.processed_on::date as days_before_next_payday

,ai.paychecks_per_year AS paychecks_per_year
,ai.income_freq_type_cd AS income_freq_type_cd
,ai.income_monthly_net AS income_monthly_net
,case when ai.income_monthly_net > 5000 then 5000 else ai.income_monthly_net end as income_monthly_net_capped5k
,round(((ai.income_monthly_net*12)/ai.paychecks_per_year),2) as paycheck_amount
,round((((case when ai.income_monthly_net > 5000 then 5000 else ai.income_monthly_net end)*12)/ai.paychecks_per_year),2) as paycheck_amount_capped5k
,COALESCE(dc2.old_value, c.income_type_cd) AS income_type_cd
,COALESCE(dc3.old_value, c.income_payment_cd) AS income_payment_cd

--,bbr.*
,bbr.routing_num
,bbr.bad_rate
,bbr.bad_rate_individual
,bbr.bad_rate_30
,bbr.bad_rate_individual_30
,bbr.bad_rate_60
,bbr.bad_rate_individual_60
,bbr.credit_union_flg
,bbr.bank_bad_rate_month

,round(((a.processed_on::date - tr.customer_created::date)*12)/365.25,0) as customer_vintage_months

into temp temp_data2
from temp_reports2 tr

inner join loans l on l.id = tr.loan_id
inner join customers c on c.id = tr.customer_id
inner join approvals a on a.id = tr.approval_id
left join people p on p.id = c.person_id
left join bus_analytics.application_address add on add.application_id = tr.application_id
left join bus_analytics.application_income ai ON ai.application_id = tr.application_id
left join addresses addr on add.address_id = addr.id
left join counties co on co.zip = addr.zip

left outer join payment_instruments.bank_accounts bac on bac.payment_instrument_id =
(select bam.payment_instrument_id from payment_instruments.bank_accounts bam where bam.person_id = c.person_id and bam.created_on <= a.processed_on order by bam.created_on desc limit 1)

left join bus_analytics.bank_bad_rates bbr on bbr.routing_num = bac.routing_number::numeric AND bbr.bank_bad_rate_month::text = (case when extract(month from a.processed_on) >= 10 then extract(year from a.processed_on::date)::text || extract(month from a.processed_on)::text else extract(year from a.processed_on::date)::text || '0' || extract(month from a.processed_on)::text end )

left join customer_paydates cp on cp.id = (
    select id
    from customer_paydates
    where customer_id = a.customer_id
    and next_paydate > a.processed_on::date
    and next_paydate <= a.processed_on::date + interval '35 days'
    order by next_paydate asc
    limit 1)

LEFT JOIN data_changes dc on dc.id = (
    SELECT id
    FROM data_changes
    WHERE record_id = a.customer_id
    and table_name = 'customers'
    and column_name = 'email'
    and change_time > a.processed_on
    order by id asc
    limit 1)

LEFT JOIN data_changes dc2 on dc2.id = (
    SELECT id
    FROM data_changes
    WHERE record_id = a.customer_id
    and table_name = 'customers'
    and column_name = 'income_type_cd'
    and change_time > a.processed_on
    order by id asc
    limit 1)

LEFT JOIN data_changes dc3 on dc3.id = (
    SELECT id
    FROM data_changes
    WHERE record_id = a.customer_id
    and table_name = 'customers'
    and column_name = 'income_payment_cd'
    and change_time > a.processed_on
    order by id asc
    limit 1)
;

commit;

--select * from temp_data2 limit 10;


---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


drop table if exists temp_bad_rates;
select
td2.*

,(select avg(tb.first_installment_initial_default_flg) from mdalgleish.installment_bad_rates tb where tb.state = td2.state and tb.first_installment_due_date between td2.approval_processed - interval '33 days' and td2.approval_processed - interval '3 days' and tb.existing_flg = 'f') as state_def_rate_30_day

,(select avg(tb.first_installment_initial_default_flg) from mdalgleish.installment_bad_rates tb where tb.lead_provider = td2.lead_provider and tb.first_installment_due_date between td2.approval_processed - interval '33 days' and td2.approval_processed - interval '3 days' and tb.existing_flg = 'f') as lead_def_rate_30_day

,(select avg(tb.first_installment_initial_default_flg) from mdalgleish.installment_bad_rates tb where tb.rv_score_tier = (case when td2.rv_score >= 600.0 then 1 when td2.rv_score >= 578.0 then 2 when td2.rv_score >= 556.0 then 3 else 4 end) and tb.first_installment_due_date between td2.approval_processed - interval '33 days' and td2.approval_processed - interval '3 days' and tb.existing_flg = 'f') as rv_score_def_rate_30_day

,(select avg(tb.first_installment_initial_default_flg) from mdalgleish.installment_bad_rates tb where tb.state = td2.state and tb.first_installment_due_date between td2.approval_processed - interval '63 days' and td2.approval_processed - interval '3 days' and tb.existing_flg = 'f') as state_def_rate_60_day

,(select avg(tb.first_installment_initial_default_flg) from mdalgleish.installment_bad_rates tb where tb.lead_provider = td2.lead_provider and tb.first_installment_due_date between td2.approval_processed - interval '63 days' and td2.approval_processed - interval '3 days' and tb.existing_flg = 'f') as lead_def_rate_60_day

,(select avg(tb.first_installment_initial_default_flg) from mdalgleish.installment_bad_rates tb where tb.rv_score_tier = (case when td2.rv_score >= 600.0 then 1 when td2.rv_score >= 578.0 then 2 when td2.rv_score >= 556.0 then 3 else 4 end) and tb.first_installment_due_date between td2.approval_processed - interval '63 days' and td2.approval_processed - interval '3 days' and tb.existing_flg = 'f') as rv_score_def_rate_60_day

,(select avg(tb.first_installment_initial_default_flg) from mdalgleish.installment_bad_rates tb where tb.state = td2.state and tb.first_installment_due_date between td2.approval_processed - interval '93 days' and td2.approval_processed - interval '3 days' and tb.existing_flg = 'f') as state_def_rate_90_day

,(select avg(tb.first_installment_initial_default_flg) from mdalgleish.installment_bad_rates tb where tb.lead_provider = td2.lead_provider and tb.first_installment_due_date between td2.approval_processed - interval '93 days' and td2.approval_processed - interval '3 days' and tb.existing_flg = 'f') as lead_def_rate_90_day

,(select avg(tb.first_installment_initial_default_flg) from mdalgleish.installment_bad_rates tb where tb.rv_score_tier = (case when td2.rv_score >= 600.0 then 1 when td2.rv_score >= 578.0 then 2 when td2.rv_score >= 556.0 then 3 else 4 end) and tb.first_installment_due_date between td2.approval_processed - interval '93 days' and td2.approval_processed - interval '3 days' and tb.existing_flg = 'f') as rv_score_def_rate_90_day

--,(select avg(tb.early_payoff_flg) from mdalgleish.installment_bad_rates tb where tb.state = td2.state and tb.first_installment_due_date between td2.approval_processed - interval '33 days' and td2.approval_processed - interval '3 days' and tb.existing_flg = 'f') as state_early_payoff_rate_30_day

--,(select avg(tb.early_payoff_flg) from mdalgleish.installment_bad_rates tb where tb.lead_provider = td2.lead_provider and tb.first_installment_due_date between td2.approval_processed - interval '33 days' and td2.approval_processed - interval '3 days' and tb.existing_flg = 'f') as lead_early_payoff_rate_30_day

--,(select avg(tb.early_payoff_flg) from mdalgleish.installment_bad_rates tb where tb.rv_score_tier = (case when td2.rv_score >= 600.0 then 1 when td2.rv_score >= 578.0 then 2 when td2.rv_score >= 556.0 then 3 else 4 end) and tb.first_installment_due_date between td2.approval_processed - interval '33 days' and td2.approval_processed - interval '3 days' and tb.existing_flg = 'f') as rv_score_early_payoff_rate_30_day

--,(select avg(tb.early_payoff_flg) from mdalgleish.installment_bad_rates tb where tb.state = td2.state and tb.first_installment_due_date between td2.approval_processed - interval '63 days' and td2.approval_processed - interval '3 days' and tb.existing_flg = 'f') as state_early_payoff_rate_60_day

--,(select avg(tb.early_payoff_flg) from mdalgleish.installment_bad_rates tb where tb.lead_provider = td2.lead_provider and tb.first_installment_due_date between td2.approval_processed - interval '63 days' and td2.approval_processed - interval '3 days' and tb.existing_flg = 'f') as lead_early_payoff_rate_60_day

--,(select avg(tb.early_payoff_flg) from mdalgleish.installment_bad_rates tb where tb.rv_score_tier = (case when td2.rv_score >= 600.0 then 1 when td2.rv_score >= 578.0 then 2 when td2.rv_score >= 556.0 then 3 else 4 end) and tb.first_installment_due_date between td2.approval_processed - interval '63 days' and td2.approval_processed - interval '3 days' and tb.existing_flg = 'f') as rv_score_early_payoff_rate_60_day

into temp temp_bad_rates
from temp_data2 td2
;

commit;

--select * from temp_bad_rates limit 10;


---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


drop table if exists temp_performance;
select
tbr.*

,(select count(*) from installments where loan_id = tbr.loan_id) as number_installments_expected
,(select count(*) from installments where loan_id = tbr.loan_id and due_date <= current_date - interval '90 days') as number_installments_come_due
,(select count(*) from installments where loan_id = tbr.loan_id and due_date <= current_date - interval '90 days' and installment_status_id in (5)) as number_installments_paid

,case when (select count(*) from installments where loan_id = tbr.loan_id) > bl.number_paid_installments and tbr.loan_status = 'paid_off' then 1 else 0 end as early_payoff_flg

,case when (select count(*) from installments where loan_id = tbr.loan_id) > bl.number_paid_installments and tbr.loan_status = 'paid_off' then nice_div(bl.number_paid_installments,(select count(*) from installments where loan_id = tbr.loan_id)) else null end as percent_through_early_payoff

,(select count(distinct ii.id) from installments ii
                      left join installment_status_history ish on ish.installments_static_id = ii.id
                      where ii.loan_id = tbr.loan_id
                      and ii.installment_number = 1
                      and ish.installment_status_id in (3,4)
                      ) as first_inst_ini_def_flg

,case when (select count(distinct ii.id) from installments ii
            left join installment_status_history ish on ish.installments_static_id = ii.id
            where ii.loan_id = tbr.loan_id
            and ii.installment_number <= 4
            and ish.installment_status_id in (3,4)
            ) > 0 then 1 else 0 end as fourth_inst_ini_def_flg

,case when (select count(distinct ii.id) from installments ii
            left join installment_status_history ish on ish.installments_static_id = ii.id
            where ii.loan_id = tbr.loan_id
            and ii.installment_number <= 6
            and ish.installment_status_id in (3,4)
                    ) > 0 then 1 else 0 end as sixth_inst_ini_def_flg

,(select count(distinct ii.id) from installments ii
    left join installment_status_history ish on ish.installments_static_id = ii.id
    where ii.loan_id = tbr.loan_id
    and ii.installment_number <= 4
    and ish.installment_status_id in (3,4)
    ) as fourth_inst_ini_def_count

,(select count(distinct ii.id) from installments ii
    left join installment_status_history ish on ish.installments_static_id = ii.id
    where ii.loan_id = tbr.loan_id
    and ii.installment_number <= 6
    and ish.installment_status_id in (3,4)
    ) as sixth_inst_ini_def_count

,case when i1.id is null then null when i1.installment_status_id in (3,4) then 1 else 0 end as first_inst_fin_def_flg
,case when i2.id is null then null when i2.installment_status_id in (3,4) then 1 else 0 end as second_inst_fin_def_flg
,case when i3.id is null then null when i3.installment_status_id in (3,4) then 1 else 0 end as third_inst_fin_def_flg
,case when i4.id is null then null when i4.installment_status_id in (3,4) then 1 else 0 end as fourth_inst_fin_def_flg
,case when i5.id is null then null when i5.installment_status_id in (3,4) then 1 else 0 end as fifth_inst_fin_def_flg
,case when i6.id is null then null when i6.installment_status_id in (3,4) then 1 else 0 end as sixth_inst_fin_def_flg
,case when i7.id is null then null when i7.installment_status_id in (3,4) then 1 else 0 end as seventh_inst_fin_def_flg
,case when i8.id is null then null when i8.installment_status_id in (3,4) then 1 else 0 end as eigth_inst_fin_def_flg
,case when i9.id is null then null when i9.installment_status_id in (3,4) then 1 else 0 end as ninth_inst_fin_def_flg
,case when i10.id is null then null when i10.installment_status_id in (3,4) then 1 else 0 end as tenth_inst_fin_def_flg
,case when i11.id is null then null when i11.installment_status_id in (3,4) then 1 else 0 end as eleventh_inst_fin_def_flg
,case when i12.id is null then null when i12.installment_status_id in (3,4) then 1 else 0 end as twelfth_inst_fin_def_flg

--,case when tbr.loan_status in ('paid_off','in_default','in_default_pmt_proc') then (select sum(total_amount) from bus_analytics.all_products_cash_flow where base_loan_id = tbr.loan_id and sub_type_cd in ('principal_out','payment') and status_cd in ('created','ach')) else null end as net_fees

--,nice_div((case when tbr.loan_status in ('paid_off','in_default','in_default_pmt_proc') then (select sum(total_amount) from bus_analytics.all_products_cash_flow where base_loan_id = tbr.loan_id and sub_type_cd in ('principal_out','payment') and status_cd in ('created','ach')) else null end),tbr.loan_amount) as profit_rate

,bl.net_fees

,nice_div(bl.net_fees,tbr.loan_amount) as profit_rate

,case when tbr.funding_date_actual <= current_date - interval '9 months' then (select net_fees from mdalgleish.all_customer_net_fees where cnu_customer_id = tbr.customer_id and month_calculated = 6) else null end as six_month_net_fees
,case when tbr.funding_date_actual <= current_date - interval '12 months' then (select net_fees from mdalgleish.all_customer_net_fees where cnu_customer_id = tbr.customer_id and month_calculated = 9) else null end as nine_month_net_fees
,case when tbr.funding_date_actual <= current_date - interval '15 months' then (select net_fees from mdalgleish.all_customer_net_fees where cnu_customer_id = tbr.customer_id and month_calculated = 12) else null end as twelve_month_net_fees
,case when tbr.funding_date_actual <= current_date - interval '18 months' then (select net_fees from mdalgleish.all_customer_net_fees where cnu_customer_id = tbr.customer_id and month_calculated = 15) else null end as fifteen_month_net_fees
,case when tbr.funding_date_actual <= current_date - interval '21 months' then (select net_fees from mdalgleish.all_customer_net_fees where cnu_customer_id = tbr.customer_id and month_calculated = 18) else null end as eighteen_month_net_fees

,case when tbr.funding_date_actual <= current_date - interval '6 months' then (select count(*) from loans where customer_id = tbr.customer_id and id > tbr.loan_id and funding_date_actual <= tbr.funding_date_actual + interval '6 months' and base_loan_id is null and status_cd not in ('declined','withdrawn','on_hold','approved','applied')) else null end as additional_base_loans_six_months

,case when tbr.funding_date_actual <= current_date - interval '12 months' then (select count(*) from loans where customer_id = tbr.customer_id and id > tbr.loan_id and funding_date_actual <= tbr.funding_date_actual + interval '12 months' and base_loan_id is null and status_cd not in ('declined','withdrawn','on_hold','approved','applied')) else null end as additional_base_loans_twelve_months

,case when tbr.funding_date_actual <= current_date - interval '18 months' then (select count(*) from loans where customer_id = tbr.customer_id and id > tbr.loan_id and funding_date_actual <= tbr.funding_date_actual + interval '18 months' and base_loan_id is null and status_cd not in ('declined','withdrawn','on_hold','approved','applied')) else null end as additional_base_loans_eighteen_months

into temp temp_performance
from temp_bad_rates tbr

left join bus_analytics.installment_loans_performance bl on bl.loan_id = tbr.loan_id

left join installments i1 on i1.id =
(select id from installments where loan_id = tbr.loan_id and installment_number = 1 and due_date < current_date - interval '90 days')

left join installments i2 on i2.id =
(select id from installments where loan_id = tbr.loan_id and installment_number = 2 and due_date < current_date - interval '90 days')

left join installments i3 on i3.id =
(select id from installments where loan_id = tbr.loan_id and installment_number = 3 and due_date < current_date - interval '90 days')

left join installments i4 on i4.id =
(select id from installments where loan_id = tbr.loan_id and installment_number = 4 and due_date < current_date - interval '90 days')

left join installments i5 on i5.id =
(select id from installments where loan_id = tbr.loan_id and installment_number = 5 and due_date < current_date - interval '90 days')

left join installments i6 on i6.id =
(select id from installments where loan_id = tbr.loan_id and installment_number = 6 and due_date < current_date - interval '90 days')

left join installments i7 on i7.id =
(select id from installments where loan_id = tbr.loan_id and installment_number = 7 and due_date < current_date - interval '90 days')

left join installments i8 on i8.id =
(select id from installments where loan_id = tbr.loan_id and installment_number = 8 and due_date < current_date - interval '90 days')

left join installments i9 on i9.id =
(select id from installments where loan_id = tbr.loan_id and installment_number = 9 and due_date < current_date - interval '90 days')

left join installments i10 on i10.id =
(select id from installments where loan_id = tbr.loan_id and installment_number = 10 and due_date < current_date - interval '90 days')

left join installments i11 on i11.id =
(select id from installments where loan_id = tbr.loan_id and installment_number = 11 and due_date < current_date - interval '90 days')

left join installments i12 on i12.id =
(select id from installments where loan_id = tbr.loan_id and installment_number = 12 and due_date < current_date - interval '90 days')

--left join loans l12d on l12d.id =
--(select max(id) from loans where customer_id = tbr.customer_id and due_date_adjusted <= tbr.funding_date_actual + interval '12 months' and status_cd not in ('declined','withdrawn','on_hold','approved','applied'))
;

commit;

--select * from temp_performance limit 10;


---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
