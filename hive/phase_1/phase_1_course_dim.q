INSERT OVERWRITE TABLE phase_1_out_course_dim
  SELECT
    phase_1_in_course_dim.id,
    phase_1_in_course_dim.canvas_id,
    phase_1_in_course_dim.root_account_id,
    phase_1_in_course_dim.account_id,
    phase_1_in_course_dim.enrollment_term_id,
    phase_1_in_course_dim.name,
    phase_1_in_course_dim.code,
    phase_1_in_course_dim.type,
    phase_1_in_course_dim.created_at,
    phase_1_in_course_dim.start_at,
    phase_1_in_course_dim.conclude_at,
    phase_1_in_course_dim.publicly_visible,
    phase_1_in_course_dim.sis_source_id,
    phase_1_in_course_dim.workflow_state,
    phase_1_in_course_dim.wiki_id,
    active_user_count.cnt
  FROM
    phase_1_in_course_dim
  JOIN (
      SELECT
        course_id, count(*) as cnt
      FROM (
          SELECT
            course_id, user_id, count(*)
          FROM
            phase_1_in_enrollment_dim
          WHERE
            type='StudentEnrollment' and workflow_state='active'
          GROUP BY course_id, user_id
        ) active_users
        GROUP BY
          active_users.course_id
        ) active_user_count
  ON
    phase_1_in_course_dim.id=active_user_count.course_id
;
