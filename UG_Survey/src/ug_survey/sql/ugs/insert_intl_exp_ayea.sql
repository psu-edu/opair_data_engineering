INSERT INTO ugs.tbl_ug_survey_intl_exp (
    stud_id,
    term,
    intl_exp_type
)
SELECT
    student_id AS stud_id,
    term,
    'Academic Year Education Abroad' AS intl_exp_type
FROM dbo.tbl_stage_UG_Survey
WHERE int_exp_AYEA = 'Y'
   OR int_exp_AYEA_definition IS NOT NULL;
