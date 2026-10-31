"""A small deterministic PAIRWISE (all-pairs) generator: rows that put every pair of factor levels together at least
once, never a pair a constraint forbids. The returning-user cells (returning.py) are generated with it; the table it
makes is checked into the repo, so a cell keeps its id and its levels from one run to the next.

Why pairwise and not the full product: eleven factors of two to four levels are 2^7·3·4^2 = 6144 combinations, each a
two-session cell of several minutes on the fake cluster. Most failures a returning user meets come from one leftover,
or from two stacked (the Perlmutter report, plugin 0.1.20, 2026-10-29: a dead login-node pin under an endpoint left
online, a cached record whose login had become password + one-time code). Every pair of levels in at least one cell
reaches every such two-way interaction in a few dozen cells.

The algorithm is AETG-style greedy, made deterministic: each new row starts from one of the first uncovered pairs (in
factor, then level order), the remaining factors are filled in their order with the level that covers the most
still-uncovered pairs with what is already chosen (ties: the earlier level), skipping any level a constraint forbids
with a level already chosen; of the candidate rows the one that covers the most new pairs wins (ties: the earlier
seed). Constraints are pairs, so a pair is coverable exactly when no constraint names it, and a row built this way never
breaks one. Stdlib only, no randomness: the same factors and constraints always give the same rows.

A row may also have to satisfy a whole-row rule (`valid`: the returning-user cells keep only rows where every changed
level bites — a changed host key behind a reattach that makes no SSH is a no-op). A greedy row that breaks it is mended
by changing one, then two, of its non-seed levels (the mend that covers the most new pairs wins); a pair no valid row
can hold must be named as a constraint, or the generator says which pairs it could not place.
"""
from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from itertools import combinations

Factors = Mapping[str, Sequence[str]]          # name -> levels, in order (the first level is the "nothing changed" one)
Pair = tuple[tuple[str, str], tuple[str, str]]  # ((factor, level), (factor, level)), factors in declaration order
Forbidden = Sequence[tuple[Mapping[str, str], str]]   # ({factor: level, factor: level}, why) — always two factors


def _key(factors: Factors, a: tuple[str, str], b: tuple[str, str]) -> Pair:
    order = list(factors)
    return (a, b) if order.index(a[0]) < order.index(b[0]) else (b, a)


def check_constraints(factors: Factors, forbidden: Forbidden) -> None:
    """Every constraint names exactly two different factors, each with one of its levels (else ValueError)."""
    for pair, why in forbidden:
        if len(pair) != 2:
            raise ValueError(f"a constraint names two factors, not {sorted(pair)}: {why}")
        for f, lv in pair.items():
            if f not in factors or lv not in factors[f]:
                raise ValueError(f"constraint {dict(pair)} ({why}): {f}={lv} is no level of the factors")


def forbidden_pairs(factors: Factors, forbidden: Forbidden) -> set[Pair]:
    check_constraints(factors, forbidden)
    return {_key(factors, *list(p.items())) for p, _why in forbidden}


def all_pairs(factors: Factors, forbidden: Forbidden = ()) -> set[Pair]:
    """Every pair of levels of two different factors that no constraint forbids: what the rows must cover."""
    bad = forbidden_pairs(factors, forbidden)
    out: set[Pair] = set()
    for fa, fb in combinations(list(factors), 2):
        for la in factors[fa]:
            for lb in factors[fb]:
                p = ((fa, la), (fb, lb))
                if p not in bad:
                    out.add(p)
    return out


def violations(row: Mapping[str, str], factors: Factors, forbidden: Forbidden) -> list[str]:
    """The constraints `row` breaks, as their reasons (empty: none)."""
    return [why for pair, why in forbidden if all(row.get(f) == lv for f, lv in pair.items())]


def pairs_of(row: Mapping[str, str], factors: Factors) -> set[Pair]:
    names = [f for f in factors if f in row]
    return {((fa, row[fa]), (fb, row[fb])) for fa, fb in combinations(names, 2)}


def _sorted_pairs(factors: Factors, pairs: set[Pair]) -> list[Pair]:
    order = {f: i for i, f in enumerate(factors)}
    lvl = {f: {lv: i for i, lv in enumerate(levels)} for f, levels in factors.items()}
    return sorted(pairs, key=lambda p: (order[p[0][0]], order[p[1][0]], lvl[p[0][0]][p[0][1]], lvl[p[1][0]][p[1][1]]))


Valid = Callable[[dict[str, str]], bool]


def _complete(seed: Pair, factors: Factors, uncovered: set[Pair], bad: set[Pair],
              valid: Valid | None = None) -> dict[str, str] | None:
    row = {seed[0][0]: seed[0][1], seed[1][0]: seed[1][1]}
    for f, levels in factors.items():
        if f in row:
            continue
        best, best_gain = None, -1
        for lv in levels:
            if any(_key(factors, (f, lv), (g, row[g])) in bad for g in row):
                continue
            gain = sum(_key(factors, (f, lv), (g, row[g])) in uncovered for g in row)
            if gain > best_gain:
                best, best_gain = lv, gain
        if best is None:
            return None   # every level of `f` is forbidden with what is chosen: this seed cannot make a row
        row[f] = best
    out = {f: row[f] for f in factors}
    if valid is None or valid(out):
        return out
    return _mend(out, seed, factors, uncovered, bad, valid)


def _allowed(row: dict[str, str], factors: Factors, bad: set[Pair]) -> bool:
    names = list(factors)
    return not any(_key(factors, (a, row[a]), (b, row[b])) in bad for a, b in combinations(names, 2))


def _mend(row: dict[str, str], seed: Pair, factors: Factors, uncovered: set[Pair], bad: set[Pair],
          valid: Valid) -> dict[str, str] | None:
    """A row that breaks the whole-row rule, mended by changing one, then two, of its non-seed levels — the mend that
    keeps the most new pairs (ties: factor, then level, order). None when no such mend is valid."""
    free = [f for f in factors if f not in (seed[0][0], seed[1][0])]
    for width in (1, 2):
        best, best_gain = None, -1
        for fs in combinations(free, width):
            options: list[list[tuple[str, str]]] = [[(f, lv) for lv in factors[f] if lv != row[f]] for f in fs]
            for choice in (options[0] if width == 1 else [(a, b) for a in options[0] for b in options[1]]):
                cand = dict(row)
                for f, lv in ([choice] if width == 1 else choice):
                    cand[f] = lv
                if not _allowed(cand, factors, bad) or not valid(cand):
                    continue
                gain = len(pairs_of(cand, factors) & uncovered)
                if gain > best_gain:
                    best, best_gain = cand, gain
        if best is not None:
            return best
    return None


def generate(factors: Factors, forbidden: Forbidden = (), *, seeds: int = 40,
             valid: Valid | None = None) -> list[dict[str, str]]:
    """The rows: each a {factor: level} dict in factor order. `seeds`: how many of the first uncovered pairs each new
    row is tried from (more can make the table shorter, never different for the same inputs). `valid`: a whole-row rule
    every row must keep (see the module's note)."""
    bad = forbidden_pairs(factors, forbidden)
    uncovered = all_pairs(factors, forbidden)
    rows: list[dict[str, str]] = []
    while uncovered:
        best_row, best_gain = None, 0
        ordered = _sorted_pairs(factors, uncovered)
        # a seed that cannot make a valid row is passed over; when the first `seeds` all fail, every other is tried
        for seed in ordered[:max(1, seeds)] + (ordered[max(1, seeds):] if valid is not None else []):
            if best_row is not None and seed not in ordered[:max(1, seeds)]:
                break
            row = _complete(seed, factors, uncovered, bad, valid)
            if row is None:
                continue
            gain = len(pairs_of(row, factors) & uncovered)
            if gain > best_gain:
                best_row, best_gain = row, gain
        if best_row is None:
            raise ValueError(f"no row can cover the pairs left: {_sorted_pairs(factors, uncovered)[:3]}")
        rows.append(best_row)
        uncovered -= pairs_of(best_row, factors)
    return rows


def uncovered_pairs(rows: Sequence[Mapping[str, str]], factors: Factors, forbidden: Forbidden = ()) -> list[Pair]:
    """The valid pairs no row covers (empty: the rows are pairwise-complete)."""
    left = all_pairs(factors, forbidden)
    for row in rows:
        left -= pairs_of(row, factors)
    return _sorted_pairs(factors, left)
