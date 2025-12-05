-- insert_intern_2.sql
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
    2 AS intern_numb,
    COALESCE(m.standardized_company_name, s.intern_organization2) AS intern_co_nm,
    s.intern_unit2  AS intern_dept,
    s.intern_title2 AS intern_job_title,
    s.intern_exp2   AS intern_len_wks,
    s.intern_paid2  AS indc_intern_paid,
    s.intern_paid_amt2 AS intern_amt_paid
FROM dbo.tbl_stage_UG_Survey AS s
LEFT JOIN dbo.tbl_stage_survey_company_mappings AS m
    ON s.intern_organization2 = m.company_name_variant
WHERE s.intern_organization2 IS NOT NULL
   OR s.intern_unit2          IS NOT NULL
   OR s.intern_title2         IS NOT NULL;
