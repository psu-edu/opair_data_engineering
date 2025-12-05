INSERT INTO ugs.tbl_ug_survey_response (
  stud_id, term, publ_srvc_cd, publ_srvc, fell_pgrm_nm, fell_city, fell_st_cd,
  fell_st, fell_ctry_cd, fell_ctry, job_asst_email, edu_pursu_cd, edu_pursu,
  edu_coll, edu_appl_stg_cd, edu_appl_stg, edu_offers_recv_numb, edu_crculm_nm,
  edu_inst_cd, edu_inst_nm, empl_mth_job_offer_recv, empl_yr_job_offer_recv,
  empl_date_job_offer_recv, empl_numb_job_offers, empl_co_nm, empl_dept, empl_title,
  empl_relate, empl_ctry_cd, empl_st_cd, empl_city, empl_sal, empl_bonus_recv_indc,
  empl_bonus_amt, empl_relo_indc, empl_relo_amt, empl_job_pri_func_cd, empl_job_pri_func,
  empld_dur_edu_indc, empld_dur_edu_cd, empld_dur_edu, conxn_email, conxn_cell_phone_no,
  empl_job_offer_timing_code, EMP_remote, EMP_remote_definition, FS_Province
)
SELECT
  a.student_id, a.term,
  a.PS_committment,
  CASE WHEN a.ps_committment_definition='Other'
       THEN a.ps_committment_definition + ' - ' + COALESCE(a.PS_committment_other,'Unknown')
       ELSE a.ps_committment_definition END,
  a.FS_program, a.FS_city, a.FS_state,
  s.descr AS fell_st,
  a.FS_country, c.descr AS fell_ctry,
  a.SFT_email,
  a.FE_type, a.FE_type_definition, a.FE_college, a.FE_stage, a.FE_stage_definition, a.FE_offers,
  a.FE_program, a.FE_college_code,
  CASE WHEN a.term < '2165' THEN nsc.NSC_COLLEGE_NAME
       WHEN a.term >= '2165' THEN org.descr50
       ELSE 'Unknown' END AS edu_inst_nm,
  a.EMP_Month, a.EMP_Year, NULL,
  a.offers_count,
  COALESCE(map.standardized_company_name, a.EMP_company),
  a.EMP_department, a.EMP_title, a.EMP_relate,

  -- FIXED LINE:
  LEFT(NULLIF(LTRIM(RTRIM(a.EMP_country)), ''), 2) AS empl_ctry_cd,

  a.EMP_state, a.EMP_city,
  a.EMP_salary, a.EMP_bonus, a.EMP_bonus_amount,
  a.EMP_relocate, a.EMP_relocate_amount,
  a.EMP_functional, a.EMP_functional_definition,
  a.EMP_emp_edu, a.EMP_emp_edu_manner, a.EMP_emp_edu_manner_definition,
  a.connect_email, a.connect_cell,
  a.EMP_off_recv,
  a.EMP_remote, a.EMP_remote_definition, a.FS_Province
FROM dbo.tbl_stage_UG_Survey a
LEFT JOIN lp_reference.dbo.tbl_Country c
  ON a.FS_country = c.country
LEFT JOIN lp_reference.dbo.tbl_state s
  ON a.FS_state = s.STATE AND s.country = 'USA'
LEFT JOIN ugs.tbl_ug_survey_nsc_ipeds_codes nsc
  ON a.FE_college_code = LEFT(nsc.NSC_COLLEGE_AND_BRANCH_CODE, 6)
LEFT JOIN lp_reference.dbo.tbl_external_org_details org
  ON a.FE_college_code = org.external_system_id AND org.ls_school_type='COL'
LEFT JOIN dbo.tbl_stage_survey_company_mappings map
  ON a.EMP_company = map.company_name_variant
WHERE
  a.connect_cell IS NOT NULL OR a.connect_email IS NOT NULL OR
  a.FE_stage_definition IS NOT NULL OR a.FE_stage IS NOT NULL OR
  a.FE_program IS NOT NULL OR a.FE_college_code IS NOT NULL OR
  a.FE_college IS NOT NULL OR a.FE_offers IS NOT NULL OR
  a.FE_type_definition IS NOT NULL OR a.FE_type IS NOT NULL OR
  a.EMP_bonus_amount IS NOT NULL OR a.EMP_bonus IS NOT NULL OR
  a.EMP_city IS NOT NULL OR a.EMP_company IS NOT NULL OR
  a.EMP_country IS NOT NULL OR a.EMP_off_recv IS NOT NULL OR
  a.EMP_department IS NOT NULL OR a.EMP_functional_definition IS NOT NULL OR
  a.EMP_functional IS NOT NULL OR a.EMP_Month IS NOT NULL OR
  a.offers_count IS NOT NULL OR a.EMP_relocate_amount IS NOT NULL OR
  a.EMP_relocate IS NOT NULL OR a.EMP_salary IS NOT NULL OR
  a.EMP_state IS NOT NULL OR a.EMP_title IS NOT NULL OR
  a.EMP_relate IS NOT NULL OR a.EMP_Year IS NOT NULL OR
  a.EMP_emp_edu_manner_definition IS NOT NULL OR a.EMP_emp_edu_manner IS NOT NULL OR
  a.EMP_emp_edu IS NOT NULL OR a.FS_city IS NOT NULL OR
  a.FS_country IS NOT NULL OR a.FS_program IS NOT NULL OR
  a.FS_state IS NOT NULL OR a.SFT_email IS NOT NULL OR
  a.ps_committment_definition IS NOT NULL OR a.PS_committment_other IS NOT NULL OR
  a.PS_committment IS NOT NULL;
