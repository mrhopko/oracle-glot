SELECT T1.d,
    T2.c
FROM T1,
    T2
WHERE T1.x = T2.x (+)
    and T2.y (+) IS NULL;
SELECT T1.d,
    T2.c
FROM T1
    LEFT JOIN T2 ON T1.x = T2.x
    and T2.y IS NULL;
SELECT T1.d,
    T2.c
FROM T1,
    T2
WHERE T1.x = T2.x (+)
    and T2.y IS NULL;
SELECT T1.d,
    T2.c
FROM T1
    LEFT JOIN T2 ON T1.x = T2.x
WHERE T2.y IS NULL;