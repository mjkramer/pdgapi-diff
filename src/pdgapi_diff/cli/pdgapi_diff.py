from dataclasses import dataclass
from enum import auto, Enum

from colorama import Fore, Style
import sqlalchemy
from sqlalchemy import select

from tqdm import tqdm


@dataclass
class Row:
    vals: list[tuple]
    matched_vals: list[tuple]


@dataclass
class Delta:
    row: tuple

@dataclass
class Insert(Delta):
    pass

@dataclass
class Delete(Delta):
    pass

@dataclass
class Update(Delta):
    new_row: tuple


class DB:
    def __init__(self, path):
        url = f'sqlite:///{path}'
        engine = sqlalchemy.create_engine(url)
        self.conn = engine.connect()
        self.meta = sqlalchemy.MetaData()
        self.meta.reflect(engine)

    def get_all(self, table: str,
                exclude_cols: list | None = None) -> list:
        cols = list(self.meta.tables[table].columns)
        if exclude_cols:
            cols = [c for c in cols
                    if c.name not in exclude_cols]

        query = select(*cols)
        return self.conn.execute(query).fetchall()


def distance(vals1: tuple, vals2: tuple):
    assert len(vals1) == len(vals2)
    return sum(v1 != v2 for v1, v2 in zip(vals1, vals2))


def find_nearest(needle: tuple, haystack: list[tuple],
                 max_dist: int) -> tuple | None:
    dists = [distance(needle, straw) for straw in haystack]
    min_dist = min(dists)
    idcs = [i for i, d in enumerate(dists) if d == min_dist]
    if min_dist > max_dist:
        return None
    assert len(idcs) == 1, \
        f'ambiguous match: {needle} --> {[haystack[i] for i in idcs]}'
    return haystack[idcs[0]]


def compare(rows1: list[tuple], rows2: list[tuple],
            max_dist: int) -> list[Delta]:
    deltas: list[Delta] = []
    rows2_new = set(rows2)

    for row in tqdm(rows1):
        nearest = find_nearest(row, rows2, max_dist)
        if not nearest:
            deltas.append(Delete(row))
        else:
            reverse_nearest = find_nearest(nearest, rows1, max_dist)
            assert reverse_nearest == row, \
                f'asymmetry: {row} -> {nearest} <- {reverse_nearest}'
            rows2_new.remove(nearest)
            if nearest != row:
                deltas.append(Update(row, nearest))

    for row in rows2_new:
        deltas.append(Insert(row))

    return deltas


def pprint(deltas: list[Delta]):
    for d in deltas:
        match d:
            case Insert(row):
                print(f'{Fore.GREEN}{row}{Style.RESET_ALL}')
            case Delete(row):
                print(f'{Fore.RED}{row}{Style.RESET_ALL}')
            case Update(old_row, new_row):
                changed = [v1 == v2 for v1, v2 in zip(old_row, new_row)]
                def fmt(i, v):
                    if changed[i]:
                        return str(v)
                    else:
                        return Style.BRIGHT + str(v) + Style.RESET_ALL
                old_strs = [fmt(i, v) for i, v in enumerate(old_row)]
                new_strs = [fmt(i, v) for i, v in enumerate(new_row)]
                print('(' + ','.join(old_strs) + ')')
                print('(' + ','.join(new_strs) + ')')
        print()


def run_compare(path1: str, path2: str, table: str,
                max_dist = 1, exclude_cols = ['id']):
    db1, db2 = DB(path1), DB(path2)
    rows1 = db1.get_all(table, exclude_cols)
    rows2 = db2.get_all(table, exclude_cols)
    deltas = compare(rows1, rows2, max_dist)
    pprint(deltas)


def main():
    import fire
    fire.Fire(run_compare)
