SELECT *
FROM table1,
    table2
WHERE table1.column = table2.column(+);
SELECT *
FROM table1
    LEFT JOIN table2 ON table1.column = table2.column;
SELECT *
FROM table1,
    table2,
    table3,
    table4
WHERE table1.column = table2.column(+)
    AND table2.column >= table3.column(+)
    AND table1.column = table4.column(+);
SELECT *
FROM table1
    LEFT JOIN table2 ON table1.column = table2.column
    LEFT JOIN table3 ON table2.column >= table3.column
    LEFT JOIN table4 ON table1.column = table4.column;
SELECT *
FROM table1,
    table2,
    table3
WHERE table1.column = table2.column(+)
    AND table2.column >= table3.column(+);
SELECT *
FROM table1
    LEFT JOIN table2 ON table1.column = table2.column
    LEFT JOIN table3 ON table2.column >= table3.column;
SELECT e1.x,
    e2.x
FROM e e1,
    e e2
WHERE e1.y (+) = e2.y;
SELECT e1.x,
    e2.x
FROM e e1
    LEFT JOIN e e2 ON e1.y = e2.y;