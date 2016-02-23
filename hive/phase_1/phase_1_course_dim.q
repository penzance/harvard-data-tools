INSERT OVERWRITE TABLE out_course_dim
  SELECT
    in_course_dim.id,
    in_course_dim.canvas_id,
    in_course_dim.root_account_id,
    in_course_dim.account_id,
    in_course_dim.enrollment_term_id,
    in_course_dim.name,
    in_course_dim.code,
    in_course_dim.type,
    in_course_dim.created_at,
    in_course_dim.start_at,
    in_course_dim.conclude_at,
    in_course_dim.publicly_visible,
    in_course_dim.sis_source_id,
    in_course_dim.workflow_state,
    in_course_dim.wiki_id,
    active_user_count.cnt
  FROM
    in_course_dim
  JOIN (
      SELECT
        course_id, count(*) as cnt
      FROM (
          SELECT
            course_id, user_id, count(*)
          FROM
            in_enrollment_dim
          WHERE
            type='StudentEnrollment' and workflow_state='active'
          GROUP BY course_id, user_id
        ) active_users
        GROUP BY
          active_users.course_id
        ) active_user_count
  ON
    in_course_dim.id=active_user_count.course_id
;
