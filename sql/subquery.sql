SELECT table1.id,
    table2.cloumn1
FROM table1,
    table2,
    (
        SELECT tableInner1.id
        FROM tableInner1,
            tableInner2
        WHERE tableInner1.id = tableInner2.id(+)
    ) table3
WHERE table1.id = table2.id(+)
    AND table1.id = table3.id(+)
    AND table2.id(+) = table3.id
    AND table1.id = 1;