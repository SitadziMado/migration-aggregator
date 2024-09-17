from abc import abstractmethod
from sys import stderr
from typing import Any, Callable, Dict, List, Self, Tuple

import pglast.ast as ast
import pglast.enums.parsenodes as nodes


def _range_var_to_tuple(range_var: ast.RangeVar) -> Tuple[str, str]:
    return (
        range_var.schemaname,
        range_var.relname,
    )


class Create:
    @abstractmethod
    def name(self) -> tuple:
        pass

    def columns(self) -> List[ast.ColumnDef]:
        return [child for child in self.children if isinstance(child, ast.ColumnDef)]


class CreateTable(Create):
    def __init__(self, statement: ast.CreateStmt) -> None:
        self.statement: ast.CreateStmt = statement

    def name(self):
        return _range_var_to_tuple(self.statement.relation)

    @property
    def children(self):
        return self.statement.tableElts

    @children.setter
    def children(self, value):
        self.statement.tableElts = value


class CreateType(Create):
    def __init__(self, statement: ast.CompositeTypeStmt) -> None:
        self.statement: ast.CompositeTypeStmt = statement

    def name(self):
        return _range_var_to_tuple(self.statement.typevar)

    @property
    def children(self):
        return self.statement.coldeflist

    @children.setter
    def children(self, value):
        self.statement.coldeflist = value


class CreateIndex(Create):
    def __init__(self, statement: ast.IndexStmt) -> None:
        self.statement: ast.IndexStmt = statement

    def name(self) -> tuple:
        return (
            self.statement.relation.schemaname,
            self.statement.idxname,
        )


class CreateFunction(Create):
    def __init__(self, statement: ast.CreateFunctionStmt) -> None:
        self.statement: ast.CreateFunctionStmt = statement

    def name(self) -> tuple:
        return tuple(part.sval for part in self.statement.funcname)


class CreateEnum(Create):
    def __init__(self, statement: ast.CreateEnumStmt) -> None:
        self.statement = statement

    def name(self):
        return tuple(part.sval for part in self.statement.typeName)


class CreateTrigger(Create):
    def __init__(self, statement: ast.CreateTrigStmt) -> None:
        self.statement: ast.CreateTrigStmt = statement

    def name(self) -> tuple:
        return (
            self.statement.relation.schemaname,
            self.statement.trigname,
        )


class CreateSchema(Create):
    def __init__(self, statement: ast.CreateSchemaStmt) -> None:
        self.statement: ast.CreateSchemaStmt = statement

    def name(self) -> Tuple:
        return (self.statement.schemaname,)


class AlterTable:
    def __init__(self, statement: ast.AlterTableStmt) -> None:
        self.statement: ast.AlterTableStmt = statement

    def name(self) -> tuple:
        return _range_var_to_tuple(self.statement.relation)


class Drop:
    def __init__(self, statement: ast.DropStmt) -> None:
        self.statement: ast.DropStmt = statement

    def names(self) -> list:
        result = []

        for object in self.statement.objects:
            match object:
                case (ast.String(), ast.String()):
                    result.append(tuple(part.sval for part in object))
                case ast.TypeName():
                    result.append(tuple(part.sval for part in object.names))
                case ast.ObjectWithArgs():
                    result.append(tuple(part.sval for part in object.objname))
                case _:
                    raise TypeError(f"Incorrect object type: {type(object)}")

        return result
