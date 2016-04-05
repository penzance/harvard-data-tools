INSERT OVERWRITE TABLE out_requests_per_user 
  SELECT
    user_id, COUNT(*) 
  AS 
    request_count 
  FROM
    in_requests 
  GROUP BY
    user_id 
  ORDER BY
    request_count;
