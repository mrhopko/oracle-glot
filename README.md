# oracle-glot

An attempt to use the excellent sqlglot library to convert old oracle join operators (+) into outer joins.

usage:

```python
sql = """SELECT *
FROM table1,
    table2
WHERE table1.column = table2.column(+);
"""
converted_sql = convert.remove_join_marks_from_oracle_sql(sql)
print(converted_sql)
```
