INSERT INTO ugs.tbl_ug_survey_stud_attr (
  stud_id, term, intern_numb, coltn_mthd_cd, coltn_mthd, indc_have_information,
  post_graduation_stat_cd, post_graduation_stat, post_graduation_other,
  indc_intern, indc_intl_exp, indc_ug_rsrch, indc_job_asst, indc_conn_to_psu,
  indc_ft_empl, indc_pt_empl, indc_any_empl, indc_fut_edu, indc_publ_srvc,
  indc_fell, indc_post_intern_resid, indc_still_seeking, indc_mil, indc_entre,
  indc_other_plans, indc_exp_semAbroad, indc_exp_AYEA, indc_exp_embedded,
  indc_exp_studorg, indc_exp_other, indc_exp_none, exp_other_fill,
  indc_exp_long_via_psu, indc_exp_short_psu_course, indc_exp_short_psu_cluborg,
  indc_exp_ed_not_psu, indc_exp_internship, indc_exp_did_not_have
)
SELECT
  student_id,
  term,
  intern_count,
  collection_method,
  CASE
    WHEN collection_method = '0' THEN 'Did not collect/or respond'
    WHEN collection_method = '1' THEN 'Survey'
    WHEN collection_method = '2' THEN 'NSLC -- National Student Clearinghouse'
    WHEN collection_method = '3' THEN 'Smeal Business  School'
    WHEN collection_method = '4' THEN 'Eberly College of Science'
    ELSE NULL
  END,
  Have_PostGrad_Info,
  Plans_Cleaned,
  plans_cleaned_definition,
  plans_other,
  indicator_internship,
  indicator_international_experience,
  indicator_undergrad_research,
  SFT_assist,
  indicator_stay_connected_PSU,
  indicator_fulltime_employment,
  indicator_parttime_employment,
  indicator_any_employment,
  indicator_further_education,
  indicator_ps_committment,
  indicator_fellowship,
  indicator_post_intern_residency,
  indicator_still_seeking,
  indicator_military,
  indicator_entrepreneurship,
  indicator_other_plans,
  CASE WHEN int_exp_semAbroad='Y' OR int_exp_semAbroad_definition IS NOT NULL THEN 'Y' END,
  CASE WHEN int_exp_AYEA='Y' OR int_exp_AYEA_definition IS NOT NULL THEN 'Y' END,
  CASE WHEN int_exp_embedded='Y' OR int_exp_embedded_definition IS NOT NULL THEN 'Y' END,
  CASE WHEN int_exp_studorg='Y' OR int_exp_studorg_definition IS NOT NULL THEN 'Y' END,
  CASE WHEN int_exp_other='Y' OR int_exp_other_fill IS NOT NULL THEN 'Y' END,
  int_exp_none,
  int_exp_other_fill,
  CASE WHEN int_exp_long_via_psu='Y' OR int_exp_long_via_psu_defin IS NOT NULL THEN 'Y' END,
  CASE WHEN int_exp_short_psu_course='Y' OR int_exp_short_psu_course_defin IS NOT NULL THEN 'Y' END,
  CASE WHEN int_exp_short_psu_club_org='Y' OR int_exp_short_psu_club_org_def IS NOT NULL THEN 'Y' END,
  CASE WHEN int_exp_educ_prog_not_psu='Y' OR int_exp_educ_prog_not_psu_defin IS NOT NULL THEN 'Y' END,
  CASE WHEN int_exp_internship='Y' OR int_exp_internship_definition IS NOT NULL THEN 'Y' END,
  CASE WHEN int_exp_did_not_have='Y' OR int_exp_did_not_have_defin IS NOT NULL THEN 'Y' END
FROM dbo.tbl_stage_UG_Survey;
