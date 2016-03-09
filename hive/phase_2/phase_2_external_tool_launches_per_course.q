INSERT OVERWRITE TABLE out_external_tool_launches_per_course 
  SELECT
    course_id, count(id) 
  FROM
    in_requests
  WHERE
    web_application_controller='external_tools'
  AND
    web_applicaiton_action='show'
  GROUP BY
    course_id;
