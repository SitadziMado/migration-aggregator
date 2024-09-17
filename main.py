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


def _constraint_name(
    relation: ast.RangeVar, column: ast.ColumnDef, constraint_type: nodes.ConstrType
):
    mapping = {
        nodes.ConstrType.CONSTR_PRIMARY: "pkey",
        nodes.ConstrType.CONSTR_UNIQUE: "key",
        nodes.ConstrType.CONSTR_EXCLUSION: "excl",
        nodes.ConstrType.CONSTR_IDENTITY: "idx",
        nodes.ConstrType.CONSTR_FOREIGN: "fkey",
        nodes.ConstrType.CONSTR_CHECK: "check",
    }

    # ToDo: fix this
    return "_".join(
        [relation.relname, column.colname, mapping.get(constraint_type, "None")]
    )


class UnsupportedStatementError(Exception):
    def __init__(self, statement, *args: object) -> None:
        self.statement = statement
        super().__init__(*args)


class Repository:
    def __init__(self) -> None:
        self.rows = dict()

    def create(self, statement: Create):
        # ToDo: process IF NOT EXISTS
        self.rows[statement.name()] = statement

    def alter(self, statement: AlterTable):
        this: Create = self.rows[statement.name()]
        unsupported = []

        for command in statement.statement.cmds:
            match command.subtype:
                case nodes.AlterTableType.AT_AddColumn:
                    this.children = this.children + (command.def_,)
                case nodes.AlterTableType.AT_DropColumn:
                    this.children = tuple(
                        column
                        for column in this.children
                        if isinstance(column, ast.ColumnDef)
                        and column.colname != command.name
                    )
                # case nodes.AlterTableType.AT_SetNotNull:
                #     for child in this.columns():
                #         if child.colname == command.name:
                #             child.is_not_null = True
                # case nodes.AlterTableType.AT_DropNotNull:
                #     for child in this.columns():
                #         if child.colname == command.name:
                #             child.is_not_null = False
                case nodes.AlterTableType.AT_ColumnDefault:
                    for child in this.columns():
                        if child.colname == command.name:
                            child.constraints = child.constraints + (
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
        #     case nodes.AlterTableType.AT_AddConstraint:
        #       this.tableElts = this.tableElts + (command.def_,)
        #     case nodes.AlterTableType.AT_DropConstraint:
        #       # def is_suitable_constraint(constraint: ast.ColumnDef | ast.Constraint):
        #       #   if isinstance(constraint, ast.Constraint):
        #       #     return constraint.conname == command.name or _constraint_name(statement.relation, column, constraint.contype) == command.name

        #       # constraint = _find(target, is_suitable_constraint)

        #       for column in this.tableElts:
        #         if isinstance(column, ast.ColumnDef):
        #           for index, constraint in enumerate(column.constraints or []):
        #             if constraint.conname == command.name or _constraint_name(statement.relation, column, constraint.contype) == command.name:
        #               this.tableElts = this.tableElts[:index] + this.tableElts[index + 1:]
        #               return  # ToDo: fix the copypaste

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
                print(IndentedStream()(statement.statement), '\n', sep=';', file=f)

    print(f"Emitting the unsupported statements to: '{output_dir}/unsupported.sql'")

    with open(output_dir.joinpath(f"unsupported.sql"), "w") as f:
        for statement in skipped:
            print(IndentedStream()(statement), '\n', sep=';', file=f)

    print("Work finished")


if __name__ == "__main__":
    main()
