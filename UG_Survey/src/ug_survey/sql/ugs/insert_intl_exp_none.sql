INSERT INTO ugs.tbl_ug_survey_intl_exp (
    stud_id,
    term,
    intl_exp_type
)
SELECT
    student_id AS stud_id,
    term,
    'None' AS intl_exp_type
FROM dbo.tbl_stage_UG_Survey
WHERE int_exp_none = 'Y'

UNION
SELECT
    student_id AS stud_id,
    term,
    'No International Experience' AS intl_exp_type
FROM dbo.tbl_stage_UG_Survey
WHERE int_exp_did_not_have = 'Y'
   OR int_exp_did_not_have_defin IS NOT NULL;
