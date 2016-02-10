INSERT OVERWRITE TABLE phase_1_out_requests_per_user SELECT user_id, COUNT(*) AS request_count FROM phase_1_in_requests GROUP BY user_id ORDER BY request_count;
