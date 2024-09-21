import argparse
import copy
from enum import Enum, auto
from os import walk
from pathlib import Path
from pprint import pprint
from sys import stderr
import sys
from typing import Any, Callable, Dict, List, Self

from pglast import parse_sql
from pglast.stream import IndentedStream
import pglast.ast as ast
import pglast.enums.parsenodes as nodes

from queries import *


OBJECT_TYPES = (
    "FUNCTION",
    "INDEX",
    "SCHEMA",
    "TABLE",
    "TRIGGER",
    "TYPE",
)


def eprint(*args, **kwargs):
    print(*args, file=stderr, **kwargs)


def appended(container: tuple | None, value: Any) -> tuple:
    return (container or tuple()) + (value,)


def removed(container: tuple, index: int) -> tuple:
    return container[:index] + container[index + 1 :]


def removed_by(
    container: tuple | None, is_missing_ok: bool, pred: Callable[[Any], bool]
) -> tuple:
    container = container or tuple()

    for index, item in enumerate(container):
        if pred(item):
            return removed(container, index)

    if not is_missing_ok:
        raise ValueError("Value is absent from the tuple")

    return container


def find_by(
    container: tuple | None, pred: Callable[[Any], bool]
) -> Tuple[int | None, Any | None]:
    for index, item in enumerate(container or tuple()):
        if pred(item):
            return index, item

    return None, None


def _constraint_name(
    relation_name: str, column_name: str, constraint_type: nodes.ConstrType
) -> str | None:
    mapping = {
        nodes.ConstrType.CONSTR_PRIMARY: "pkey",
        nodes.ConstrType.CONSTR_UNIQUE: "key",
        nodes.ConstrType.CONSTR_EXCLUSION: "excl",
        nodes.ConstrType.CONSTR_IDENTITY: "idx",
        nodes.ConstrType.CONSTR_FOREIGN: "fkey",
        nodes.ConstrType.CONSTR_CHECK: "check",
    }

    suffix = mapping.get(constraint_type, "None")

    if suffix:
        return "_".join([relation_name, column_name, suffix])


class UnsupportedStatementError(Exception):
    def __init__(self, statement, *args: object) -> None:
        self.statement = statement
        super().__init__(*args)


class Repository:
    def __init__(self) -> None:
        self.rows = dict()

    def create(self, statement: Create):
        name = statement.name()

        if name in self.rows:
            match statement.conflict_behavior:
                case ConflictBehavior.FAIL:
                    raise ValueError(f"Entity '{name}' already exists in the database")
                case ConflictBehavior.IGNORE:
                    return

        self.rows[statement.name()] = statement

    def alter(self, statement: AlterTable):
        this: Create = self.rows[statement.name()]

        def find_column_by_name(name: str) -> ast.ColumnDef | None:
            return find_by(this.columns(), lambda x: x.colname == name)[1]

        def add_column_constraint(column: ast.ColumnDef, constraint: ast.Constraint):
            column.constraints = appended(column.constraints, constraint)

        unsupported = []

        for command in statement.statement.cmds:
            match command.subtype:
                case nodes.AlterTableType.AT_AddColumn:
                    this.children = appended(this.children, command.def_)
                case nodes.AlterTableType.AT_DropColumn:
                    this.children = removed_by(
                        this.children,
                        command.missing_ok,
                        lambda x: isinstance(x, ast.ColumnDef)
                        and x.colname == command.name,
                    )
                case nodes.AlterTableType.AT_AddConstraint:
                    this.children = appended(this.children, command.def_)
                case nodes.AlterTableType.AT_DropConstraint:

                    def constraint_matches_by_name(
                        constraint: ast.ColumnDef | ast.Constraint,
                    ):
                        return (
                            isinstance(constraint, ast.Constraint)
                            and constraint.conname == command.name
                        )

                    index, constraint = find_by(
                        this.children, constraint_matches_by_name
                    )

                    if constraint:
                        this.children = removed(this.children, index)
                    else:
                        constraint_removed = False

                        for column in this.columns():
                            index, _ = find_by(
                                column.constraints,
                                lambda x: _constraint_name(
                                    statement.statement.relation.relname,
                                    column.colname,
                                    x.contype,
                                )
                                == command.name,
                            )

                            if index is not None:
                                constraint_removed = True
                                column.constraints = removed(column.constraints, index)
                                break

                        if not (command.missing_ok or constraint_removed):
                            raise ValueError(
                                f"Failed to update constraint with name {command.name}"
                            )
                case nodes.AlterTableType.AT_SetNotNull:
                    add_column_constraint(
                        find_column_by_name(command.name),
                        ast.Constraint(
                            contype=nodes.ConstrType.CONSTR_NOTNULL,
                        ),
                    )
                case nodes.AlterTableType.AT_DropNotNull:
                    column = find_column_by_name(command.name)
                    column.constraints = removed_by(
                        column.constraints,
                        command.missing_ok,
                        lambda x: x.contype == nodes.ConstrType.CONSTR_NOTNULL,
                    )
                case nodes.AlterTableType.AT_ColumnDefault:
                    add_column_constraint(
                        find_column_by_name(command.name),
                        ast.Constraint(
                            contype=nodes.ConstrType.CONSTR_DEFAULT,
                            raw_expr=command.def_,
                        ),
                    )
                case _:
                    unsupported.append(command)

        if unsupported:
            clone = copy.deepcopy(statement.statement)
            clone.cmds = unsupported

            raise UnsupportedStatementError(clone)

    def drop(self, statement: Drop):
        for name in statement.names():
            if name in self.rows or not statement.statement.missing_ok:
                del self.rows[name]


class ObjectType(Enum):
    ENUM = auto()
    FUNCTION = auto()
    INDEX = auto()
    SCHEMA = auto()
    TABLE = auto()
    TRIGGER = auto()
    TYPE = auto()
    UNKNOWN = auto()

    @staticmethod
    def from_object_type(object_type: nodes.ObjectType) -> Self:
        mapping = {
            nodes.ObjectType.OBJECT_FUNCTION: ObjectType.FUNCTION,
            nodes.ObjectType.OBJECT_INDEX: ObjectType.INDEX,
            nodes.ObjectType.OBJECT_SCHEMA: ObjectType.SCHEMA,
            nodes.ObjectType.OBJECT_TABLE: ObjectType.TABLE,
            nodes.ObjectType.OBJECT_TRIGGER: ObjectType.TRIGGER,
            nodes.ObjectType.OBJECT_TYPE: ObjectType.TYPE,
        }

        return mapping.get(object_type, ObjectType.UNKNOWN)


class Schema:
    def __init__(self) -> None:
        types = [
            ObjectType.ENUM,
            ObjectType.FUNCTION,
            ObjectType.INDEX,
            ObjectType.SCHEMA,
            ObjectType.TABLE,
            ObjectType.TRIGGER,
            ObjectType.TYPE,
        ]

        self.repositories: Dict[nodes.ObjectType, Repository] = {
            key: Repository() for key in types
        }

    def execute(self, statement: ast.Expr):
        match statement:
            # CREATE family
            case ast.CreateStmt():
                self.repositories[ObjectType.TABLE].create(CreateTable(statement))
            case ast.CreateSchemaStmt():
                self.repositories[ObjectType.SCHEMA].create(CreateSchema(statement))
            case ast.CreateFunctionStmt():
                self.repositories[ObjectType.FUNCTION].create(CreateFunction(statement))
            case ast.CreateTrigStmt():
                self.repositories[ObjectType.TRIGGER].create(CreateTrigger(statement))
            case ast.CreateEnumStmt():
                self.repositories[ObjectType.ENUM].create(CreateEnum(statement))
            case ast.CompositeTypeStmt():
                self.repositories[ObjectType.TYPE].create(CreateType(statement))
            case ast.IndexStmt():
                self.repositories[ObjectType.INDEX].create(CreateIndex(statement))
            # ALTER family
            case ast.AlterTableStmt():
                self.repositories[ObjectType.from_object_type(statement.objtype)].alter(
                    AlterTable(statement)
                )
            # ToDo: support ALTER TYPE statements
            # case ast.AlterTypeStmt():
            #     pass
            # case ast.AlterEnumStmt():
            #     object_type, method = ObjectType.ENUM, "alter"
            # case ast.RenameStmt():
            #     object_type, method = ObjectType.TYPE, "rename"
            # DROP family
            case ast.DropStmt():
                self.repositories[
                    ObjectType.from_object_type(statement.removeType)
                ].drop(Drop(statement))
            # Ignored non-DDL queries and the rest
            case ast.DeleteStmt():
                # Non-DDL DELETE is ignored
                print("Non-DDL DELETE statement is deliberately ignored")
            case ast.UpdateStmt():
                # Non-DDL UPDATE is ignored
                print("Non-DDL UPDATE statement is deliberately ignored")
            case _:
                raise UnsupportedStatementError(statement)


def _parse_arguments(argv) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="migration_aggregator",
        description="For a given directory, the program collects the statements from the migration files. "
        "After that, statements are grouped by type. "
        "Only CREATE statements are emitted, while taking into account sequential ALTER and DROP statements",
    )

    parser.add_argument(
        "migration_dir",
        help="A directory where all migrations are stored",
    )
    parser.add_argument(
        "output_dir",
        help="An output directory where to emit the resulting aggregated schemas",
    )
    parser.add_argument(
        "-t",
        "--types",
        choices=OBJECT_TYPES,
        default=OBJECT_TYPES,
        nargs="*",
        type=tuple,
    )

    return parser.parse_args(argv)


def _sort_migration_files(files: List[str]) -> List[str]:
    files = [file.split("__") for file in files]

    files.sort(key=lambda x: int(x[0][1:]))

    return ["__".join(file) for file in files]


def main():
    arguments = _parse_arguments(sys.argv[1:])

    migration_dir = Path(arguments.migration_dir)

    raw_statements = []

    for dir, _, files in walk(migration_dir):
        for filename in _sort_migration_files(files):
            with open(Path(dir, filename), "r") as f:
                raw_statements.extend(parse_sql(f.read()))

    schema = Schema()
    skipped = []

    for raw in raw_statements:
        try:
            schema.execute(raw.stmt)
        except UnsupportedStatementError as e:
            skipped.append(e.statement)

    print(f"Finished the aggregation. Statements skipped: {len(skipped)}")

    output_dir = Path(arguments.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Emitting the final schemas to: '{output_dir}'...")

    for object_type in arguments.types:
        repo = schema.repositories[ObjectType[object_type]]

        with open(
            output_dir.joinpath(f"final_schema_{object_type.lower()}.sql"), "w"
        ) as f:
            for statement in repo.rows.values():
                print(IndentedStream()(statement.statement), "\n", sep=";", file=f)

    print(f"Emitting the unsupported statements to: '{output_dir}/unsupported.sql'")

    with open(output_dir.joinpath(f"unsupported.sql"), "w") as f:
        for statement in skipped:
            print(IndentedStream()(statement), "\n", sep=";", file=f)

    print("Work finished")


if __name__ == "__main__":
    main()
