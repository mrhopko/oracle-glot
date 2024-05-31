SELECT T1.d,
    T2.c
FROM T1,
    T2
WHERE T1.x = T2.x (+)
    and T2.y (+) > 5;
SELECT T1.d,
    T2.c
FROM T1
    LEFT JOIN T2 ON T1.x = T2.x
    and T2.y > 5;