# Vertica Concurrency Calculator
Python script to calculate concurrency statistics for Vertica queries

## Setup

### Example Query:

```sql
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
```

### Export Results to CSV file

With the example query above, use vsql to export the unformatted results to a csv file.

```sh
$ vsql -U <user> -h <host> -CAtX example_query.sql > output.csv
```

### Evaluate Concurrency

```sh
$ python concurrency.py -f output.csv
```

Use `-h` for a full list of options for the `concurrency.py` script.

## Dependencies

+ [NumPy](http://www.numpy.org/)
