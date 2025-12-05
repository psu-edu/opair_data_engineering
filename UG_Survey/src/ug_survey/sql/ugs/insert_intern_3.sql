-- insert_intern_3.sql
INSERT INTO ugs.tbl_ug_survey_intern (
    stud_id,
    term,
    intern_numb,
    intern_co_nm,
    intern_dept,
    intern_job_title,
    intern_len_wks,
    indc_intern_paid,
    intern_amt_paid
)
SELECT
    s.student_id AS stud_id,
    s.term,
    3 AS intern_numb,
    COALESCE(m.standardized_company_name, s.intern_organization3) AS intern_co_nm,
    s.intern_unit3  AS intern_dept,
    s.intern_title3 AS intern_job_title,
    s.intern_exp3   AS intern_len_wks,
    s.intern_paid3  AS indc_intern_paid,
    s.intern_paid_amt3 AS intern_amt_paid
FROM dbo.tbl_stage_UG_Survey AS s
LEFT JOIN dbo.tbl_stage_survey_company_mappings AS m
    ON s.intern_organization3 = m.company_name_variant
WHERE s.intern_organization3 IS NOT NULL
   OR s.intern_unit3          IS NOT NULL
   OR s.intern_title3         IS NOT NULL;
