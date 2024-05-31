SELECT table1.id,
    table2.cloumn1,
    table3.id
FROM table1,
    table2,
    (
        SELECT tableInner1.id
        FROM tableInner1,
            tableInner2
        WHERE tableInner1.id = tableInner2.id(+)
    ) AS table3
WHERE table1.id = table2.id(+)
    AND table1.id = table3.id(+);
WITH table3 AS (
    SELECT tableInner1.id
    FROM tableInner1
        LEFT JOIN tableInner2 ON tableInner1.id = tableInner2.id
)
SELECT table1.id,
    table2.cloumn1,
    table3.id
FROM table1
    LEFT JOIN table2 ON table1.id = table2.id
    LEFT JOIN table3 ON table1.id = table3.id