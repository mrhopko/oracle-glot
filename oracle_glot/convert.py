import sqlglot
from sqlglot import exp
import sqlglot.optimizer
from sqlglot.optimizer.eliminate_subqueries import eliminate_subqueries
from sqlglot.optimizer.scope import build_scope, find_all_in_scope
import copy
from typing import Dict, Optional, Tuple
import logging

# if the parent is the expression being replaced, then replace returns the new expression without replacing the node
# walk creates a list of nodes in the tree.
# if we want to update the parent, we create a new tree.
# we have to update the parent last.
# we need to get a new tree after each replacement.

# where equality is not being popped from the tree. after node.pop.

logger = logging.getLogger(__name__)


def _update_from(
    select: exp.Select,
    new_join_dict: Dict[str, exp.Join],
    old_join_dict: Dict[str, exp.Join],
):
    old_from = select.args["from"]
    if not old_from.alias_or_name in new_join_dict.keys():
        logger.debug("Old From not replaced")
        return
    in_old_not_new = old_join_dict.keys() - new_join_dict.keys()
    if len(in_old_not_new) >= 1:
        logger.debug("Replacing old from")
        new_from_name = list(old_join_dict.keys() - new_join_dict.keys())[0]
        new_from = sqlglot.parse_one(f"FROM {new_from_name}", into=exp.From)
        del old_join_dict[new_from_name]
        select.set("from", new_from)
    else:
        raise ValueError("Cannot determine which table to use as the new from")


def _update_join_dict(
    join: exp.Join, join_dict: Dict[str, exp.Join]
) -> Dict[str, exp.Join]:
    if join.alias_or_name in join_dict.keys():
        join_dict[join.alias_or_name].set(
            "on",
            exp.And(
                this=join_dict[join.alias_or_name].args["on"],
                expression=join.args["on"],
            ),
        )
    else:
        join_dict[join.alias_or_name] = join
    return join_dict


def _clean_binary_node(node: exp.Expression):
    # promote child if binary is incomplete.
    if isinstance(node, exp.Binary):
        if node.left is None:
            node.replace(node.right)
        elif node.right is None:
            node.replace(node.left)


def remove_join_marks(select: exp.Select) -> exp.Select:
    """Remove join marks from the where columns in this select statement
    Converts them to joins and replaces any existing joins

    Args:
        node (exp.Select): The AST to remove join marks from

    Returns:
        exp.Select: The AST with join marks removed
    """
    select = eliminate_subqueries(select)
    old_joins: Dict[str, exp.Join] = {
        join.alias_or_name: join for join in list(select.args.get("joins", []))
    }
    new_joins: Dict[str, exp.Join] = {}
    for node in select.find_all(exp.Column):
        logger.debug(f"Checking column {node}")
        if _has_join_mark(node):
            logger.debug(f"Found join mark {node}")
            predicate = node.find_ancestor(exp.Predicate)
            logger.debug(f"join_mark belongs to Predicate: {predicate}")
            if not isinstance(predicate, exp.Binary):
                logger.debug("Predicate is not a binary - skipping")
                continue
            predicate_parent = predicate.parent
            logger.debug(f"predicate parent: {predicate_parent}")
            join_on = predicate.pop()
            new_join = _equality_to_join(join_on)
            logger.debug(f"new_join: {new_join}")
            new_joins = _update_join_dict(new_join, new_joins)
            _clean_binary_node(predicate_parent)
    _update_from(select, new_joins, old_joins)
    replacement_joins = [
        new_joins.get(join.alias_or_name, join) for join in old_joins.values()
    ]
    select.set("joins", replacement_joins)
    where = select.args["where"]
    if where:
        if not where.this:
            select.set("where", None)
    return select


def remove_join_marks_from_oracle_sql(sql: str) -> str:
    """Remove join marks from the where columns in all subqueries within the provided select statement.
    subqueries are converted to CTEs using eliminate_subqueries.

    Args:
        sql (str): The SQL statement to remove join marks from

    Returns:
        str: The SQL statement with join marks removed"""
    ast = sqlglot.parse_one(sql, dialect="oracle")
    ast = eliminate_subqueries(ast)
    select_nodes = list(ast.find_all(exp.Select))
    select_nodes.reverse()
    # convert inner nodes first
    for node in select_nodes:
        if not node.parent:
            continue
        replacement = remove_join_marks(node)
        node.replace(replacement)
    # transform outer node
    if isinstance(ast, exp.Select):
        ast = remove_join_marks(ast)
    return ast.sql(dialect="oracle", pretty=True)
