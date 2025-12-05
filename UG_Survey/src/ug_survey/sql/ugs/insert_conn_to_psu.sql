INSERT INTO ugs.tbl_ug_survey_conn_to_psu (
    stud_id,
    term,
    conn_to_psu
)
SELECT
    student_id AS stud_id,
    term,
    CASE
        WHEN connect_assist   = 'Y' THEN connect_assist_definition
        WHEN connect_emp_panel= 'Y' THEN connect_emp_panel_definition
        WHEN connect_ldship   = 'Y' THEN connect_ldship_definition
        WHEN connect_mentor   = 'Y' THEN connect_mentor_definition
        WHEN connect_network  = 'Y' THEN connect_network_definition
        WHEN connect_no       = 'Y' THEN connect_no_definition
        ELSE NULL
    END AS conn_to_psu
FROM dbo.tbl_stage_UG_Survey
WHERE connect_assist   = 'Y'
   OR connect_emp_panel= 'Y'
   OR connect_ldship   = 'Y'
   OR connect_mentor   = 'Y'
   OR connect_network  = 'Y'
   OR connect_no       = 'Y';
