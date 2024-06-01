from typing import Dict, List
import sqlglot
from sqlglot import exp
from oracle_glot import convert
import logging
import itertools
import re

logger = logging.getLogger(__name__)


def read_asts(file: str):
    with open(file, "r") as f:
        sqls = f.read()
    asts = sqlglot.parse(sqls, dialect="oracle")
    logger.debug("asts: %s", len(asts))
    return asts


def create_join(x: int):
    return sqlglot.parse_one(
        f"INNER JOIN table{x} on table{x}.id = other_table.id", into=exp.Join
    )


def create_join_dict(xs: List[int]) -> Dict[str, exp.Join]:
    return {create_join(x).alias_or_name: create_join(x) for x in xs}


def assert_ast_pair(join_marks, oracle):
    remove_marks = convert.remove_join_marks_from_select(join_marks)
    assert remove_marks.sql(dialect="oracle") == oracle.sql(dialect="oracle")


def test_update_from():
    sql = "SELECT * FROM table1 INNER JOIN table2 on table2.id = table1.id"
    ast = sqlglot.parse_one(sql, dialect="oracle")
    old_joins = create_join_dict([2, 3])
    new_joins = create_join_dict([1, 2, 4])
    convert._update_from(ast, new_joins, old_joins)
    new_from = ast.args["from"]
    assert new_from.alias_or_name == "table3"

    ast = sqlglot.parse_one(sql, dialect="oracle")
    old_joins = create_join_dict([2, 3])
    new_joins = create_join_dict([5, 2, 4])
    convert._update_from(ast, new_joins, old_joins)
    new_from = ast.args["from"]
    assert new_from.alias_or_name == "table1"


def test_update_join():
    join_dict = create_join_dict([1, 2])
    new_join = create_join(3)
    convert._update_join_dict(new_join, join_dict)
    assert len(join_dict) == 3
    assert join_dict["table3"] == new_join

    new_join = create_join(2)
    convert._update_join_dict(new_join, join_dict)
    assert len(join_dict) == 3


def test_remove_join_marks():
    # test for convert.remove_join_marks()
    asts = read_asts("sql/simple_select.sql")
    assert_ast_pair(asts[0], asts[1])
    assert_ast_pair(asts[2], asts[3])


def test_remove_join_marks_from_oracle_sql():
    # test for convert.remove_join_marks()
    file = "sql/subquery.sql"
    with open(file, "r") as f:
        sql = f.read()
    sqls = sql.split(";")
    join_marks = sqls[0]
    oracle = sqls[1]
    remove_marks = convert.remove_join_marks_from_oracle_sql(join_marks)
    assert re.sub("\s", "", remove_marks) == re.sub("\s", "", oracle)


def test_remove_join_marks_constant_in_join():
    asts = read_asts("sql/constant_in_join.sql")
    assert_ast_pair(asts[0], asts[1])


def test_remove_join_marks_join_predicate_is_null():
    asts = read_asts("sql/join_predicate_is_null.sql")
    assert_ast_pair(asts[0], asts[1])
    assert_ast_pair(asts[2], asts[3])


def test_non_join_in_where():
    asts = read_asts("sql/non_join_in_where.sql")
    assert_ast_pair(asts[0], asts[1])
