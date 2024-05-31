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


class OracleToAnsi:

    dialect = "oracle"
    logger = logging.getLogger(__name__)

    def __init__(self, ast: exp.Select):
        self.logger.debug("Initializing OracleToAnsi")
        assert isinstance(ast, exp.Select)
        self.original_ast = ast
        self.ast_transform = copy.deepcopy(self.original_ast)
        self.logger.debug(f"original_sql: {self.original_sql()}")
        self.scope = build_scope(self.ast_transform)
        if self.scope is None:
            self.logger.debug("No scope found")
        self.has_no_scope = self.scope is None
        self.join_count = len(self.ast_transform.args.get("joins", []))
        self.has_no_joins = self.join_count == 0
        if self.has_no_joins:
            self.logger.debug("No joins found")
        self.has_no_join_marks = False
        self.old_join_dict = {
            join.alias_or_name: join for join in self.ast_transform.args["joins"]
        }
        self.new_join_dict: Dict[str, exp.Join] = {}

    def insert_join(self, join: exp.Join):
        if join.alias_or_name in self.new_join_dict.keys():
            self.new_join_dict[join.alias_or_name].set(
                "on",
                exp.And(
                    this=self.new_join_dict[join.alias_or_name].args["on"],
                    expression=join.args["on"],
                ),
            )
        else:
            self.new_join_dict[join.alias_or_name] = join

    def pop_join_marks(self):
        """pop join marks into new_join_dict.
        If the grandparent is a binary, replace the parent with the other child.

        Args:
            scope (sqlglot.optimizer.Scope): The scope of the AST
        """
        self.logger.debug("Popping join marks")
        assert self.scope
        self.has_no_join_marks = True
        for node in find_all_in_scope(self.scope, exp.Column):
            if isinstance(node, exp.Column):
                if _has_join_mark(node) and isinstance(node.parent, exp.Binary):
                    self.logger.debug(f"Found join mark {node}")
                    self.has_no_join_marks = False
                    grand_parent = node.parent.parent
                    pop_node = _equality_to_join(node.parent.pop())
                    logger.debug(f"Pop node: {pop_node}")
                    logger.debug(f"ast_transform: {self.ast_transform}")
                    assert pop_node
                    self.insert_join(pop_node)
                    if isinstance(grand_parent, exp.Binary):
                        if grand_parent.left is None:
                            self.logger.debug("promote right child")
                            grand_parent.replace(grand_parent.right)
                        elif grand_parent.right is None:
                            self.logger.debug("promote left child")
                            grand_parent.replace(grand_parent.left)

    def replace_from(self):
        old_from = self.ast_transform.args["from"]
        if not old_from.alias_or_name in self.new_join_dict.keys():
            self.logger.debug(
                "From alias_or_name is not in new_join_dict so is not replaced"
            )
            return
        self.logger.debug("From alias_or_name is in new_join_dict so will be replaced")
        if len(self.old_join_dict.keys() - self.new_join_dict.keys()) > 1:
            new_from_name = (self.old_join_dict.keys() - self.new_join_dict.keys())[0]
            self.logger.debug(f"New from name: {new_from_name}")
            new_from = sqlglot.parse_one(f"FROM {new_from_name}", into=exp.From)
            self.logger.debug(f"Removing {new_from_name} from old_join_dict")
            del self.old_join_dict[new_from_name]
            self.logger.debug(f"Inserting new from {new_from}")
            self.ast_transform.set("from", new_from)
            self.logger.debug(f"ast_transform: {self.ast_transform}")
        else:
            ValueError("Cannot determine which table to use as the new from")

    def transform(self):
        # if self.has_no_scope:
        #     self.logger.warning("No scope found - no transformation applied")
        #     print("No scope found")
        #     return
        if self.has_no_joins:
            self.logger.warning("No joins found - no transformation applied")
            print("No joins found")
            return
        self.pop_join_marks()
        if self.has_no_join_marks:
            self.logger.warning("No join marks found - no transformation applied")
            return
        self.replace_from()
        self.replacement_joins = [
            self.new_join_dict.get(join.alias_or_name, join)
            for join in self.old_join_dict.values()
        ]
        self.logger.debug(f"Replacing joins with {self.replacement_joins}")
        self.ast_transform.set("joins", self.replacement_joins)
        self.logger.debug(f"ast_transform: {self.ast_transform}")
        where = self.ast_transform.args["where"]
        if where:
            if not where.this:
                self.logger.debug("Where is empty")
                self.ast_transform.set("where", None)

    def original_sql(self):
        return self.original_ast.sql(dialect=self.dialect)

    def sql(self):
        return self.ast_transform.sql(dialect=self.dialect)


def _has_join_mark(col: exp.Column) -> bool:
    if not isinstance(col, exp.Column):
        return False
    result = col.args.get("join_mark", False)
    if isinstance(result, bool):
        return bool(result)
    return False


def _equality_to_join(eq: exp.Binary) -> Optional[exp.Join]:
    if not (isinstance(eq.left, exp.Column) and isinstance(eq.right, exp.Column)):
        logger.warn("Equality is not between two columns - cannot convert to join")
        return None
    left: exp.Column = eq.left
    right: exp.Column = eq.right
    new_eq = copy.deepcopy(eq)
    new_eq.left.set("join_mark", False)
    new_eq.right.set("join_mark", False)
    if _has_join_mark(left) and _has_join_mark(right):
        return sqlglot.parse_one(f"OUTER JOIN {right.table}", into=exp.Join).on(new_eq)
    if _has_join_mark(eq.left):
        return sqlglot.parse_one(f"LEFT JOIN {left.table}", into=exp.Join).on(new_eq)
    if _has_join_mark(eq.right):
        return sqlglot.parse_one(f"LEFT JOIN {right.table}", into=exp.Join).on(new_eq)

    return None


def oracle_to_ansi_transform(node: exp.Expression) -> exp.Expression:
    if not isinstance(node, exp.Select):
        return node
    converter = OracleToAnsi(node)
    converter.transform()
    return converter.ast_transform


def oracle_to_ansi_sql(sql: str) -> str:
    ast = sqlglot.parse_one(sql, dialect="oracle")
    ast = eliminate_subqueries(ast)
    select_nodes = list(ast.find_all(exp.Select))
    select_nodes.reverse()
    # convert inner nodes first
    for node in select_nodes:
        if not node.parent:
            continue
        replacement = oracle_to_ansi_transform(node)
        node.replace(replacement)
    # transform outer node
    ast = oracle_to_ansi_transform(ast)
    return ast.sql(dialect="oracle", pretty=True)


def oracle_to_ansi_file(file_path: str, output_path: str):
    with open(file_path, "r") as file:
        sqls = file.read().split(";")
    new_sqls = [oracle_to_ansi_sql(sql) for sql in sqls]
    with open(output_path, "w") as file:
        file.write(";".join(new_sqls))
