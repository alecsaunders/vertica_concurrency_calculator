SELECT EXTRACT(EPOCH FROM start_timestamp), EXTRACT(EPOCH FROM end_timestamp)
FROM
        query_requests q
        JOIN
        (
                SELECT DISTINCT pool_name, node_name, transaction_id, statement_id
                FROM dc_resource_acquisitions
        ) r
                ON q.node_name = r.node_name
                AND q.transaction_id = r.transaction_id
                AND q.statement_id = r.statement_id
WHERE
        q.start_timestamp > CURRENT_DATE - INTERVAL '60 day'
        AND r.pool_name = 'general'
        AND q.request_duration_ms IS NOT NULL
ORDER BY 1, 2;
