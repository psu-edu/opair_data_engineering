INSERT INTO ugs.tbl_ug_survey_intl_exp (
    stud_id,
    term,
    intl_exp_type
)
SELECT
    student_id AS stud_id,
    term,
    'Semester/Summer/Year Abroad (PSU)' AS intl_exp_type
FROM dbo.tbl_stage_UG_Survey
WHERE int_exp_long_via_psu = 'Y'
   OR int_exp_long_via_psu_defin IS NOT NULL

UNION
SELECT
    student_id AS stud_id,
    term,
    'Course Short Term Travel (PSU)' AS intl_exp_type
FROM dbo.tbl_stage_UG_Survey
WHERE int_exp_short_psu_course = 'Y'
   OR int_exp_short_psu_course_defin IS NOT NULL

UNION
SELECT
    student_id AS stud_id,
    term,
    'Club/Org Short Term Travel (PSU)' AS intl_exp_type
FROM dbo.tbl_stage_UG_Survey
WHERE int_exp_short_psu_club_org = 'Y'
   OR int_exp_short_psu_club_org_def IS NOT NULL

UNION
SELECT
    student_id AS stud_id,
    term,
    'Educational Exp Abroad (not PSU)' AS intl_exp_type
FROM dbo.tbl_stage_UG_Survey
WHERE int_exp_educ_prog_not_psu = 'Y'
   OR int_exp_educ_prog_not_psu_defin IS NOT NULL

UNION
SELECT
    student_id AS stud_id,
    term,
    'International Internship' AS intl_exp_type
FROM dbo.tbl_stage_UG_Survey
WHERE int_exp_internship = 'Y'
   OR int_exp_internship_definition IS NOT NULL;
