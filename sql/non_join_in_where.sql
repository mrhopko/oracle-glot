SELECT T1.d,
    T2.c
FROM T1,
    T2
WHERE T1.x = T2.x (+)
    and T1.Z > 4;
SELECT T1.d,
    T2.c
FROM T1
    LEFT JOIN T2 ON T1.x = T2.x
WHERE T1.Z > 4;