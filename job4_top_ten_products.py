"""
MapReduce Job 4: Identify the Top Ten Most-Reviewed Products.

Two-step MapReduce pipeline:
  Step 1 – (a) Mapper emits (asin, 1); Reducer counts reviews per product.
  Step 2 – (b) Mapper sends all (count, asin) pairs to a single reducer
               which sorts descending and extracts the Top 10.

Run:
    python job4_top_ten_products.py
    python job4_top_ten_products.py --sample 200000
"""

import json
import sys
import os
import tempfile
import heapq
from mrjob.job import MRJob
from mrjob.step import MRStep


# ──────────────────────────────────────────────────────────────────────────────
# MapReduce definition
# ──────────────────────────────────────────────────────────────────────────────

class MRTopTenProducts(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_count,
                   combiner=self.combiner_count,
                   reducer=self.reducer_count),
            MRStep(mapper=self.mapper_sort,
                   reducer=self.reducer_top10),
        ]

    # ── Step 1: count reviews per product ────────────────────────────────────

    def mapper_count(self, _, line):
        """(a) Map each review to its product ID."""
        try:
            rec = json.loads(line)
        except (json.JSONDecodeError, ValueError):
            return
        asin = rec.get("asin")
        if asin:
            yield asin, 1

    def combiner_count(self, asin, counts):
        yield asin, sum(counts)

    def reducer_count(self, asin, counts):
        total = sum(counts)
        yield asin, total

    # ── Step 2: sort and extract Top 10 ──────────────────────────────────────

    def mapper_sort(self, asin, count):
        """Route every (asin, count) to a single reducer via key=None."""
        yield None, (count, asin)

    def reducer_top10(self, _, pairs):
        """(b) Sort products by review count, extract top 10."""
        top10 = heapq.nlargest(10, pairs, key=lambda x: x[0])
        for rank, (count, asin) in enumerate(top10, 1):
            yield rank, {"asin": asin, "review_count": count}


# ──────────────────────────────────────────────────────────────────────────────
# Output formatting
# ──────────────────────────────────────────────────────────────────────────────

def _bar(count, max_count, width=40):
    filled = int(count / max_count * width) if max_count else 0
    return "█" * filled + "░" * (width - filled)


def _medal(rank):
    return {1: "(1)", 2: "(2)", 3: "(3)"}.get(rank, f"#{rank:2d}")


def print_and_save(results, out_path):
    lines = []
    def w(text=""):
        print(text)
        lines.append(str(text))

    # results: list of (rank_int, {"asin":..., "review_count":...})
    sorted_results = sorted(results, key=lambda x: x[0])
    max_count = sorted_results[0][1]["review_count"] if sorted_results else 1
    total_reviews_top10 = sum(v["review_count"] for _, v in sorted_results)

    sep = "=" * 65
    w(sep)
    w("  JOB 4: TOP 10 MOST-REVIEWED PRODUCTS")
    w(sep)

    w(f"\n  Reviews captured in Top 10 : {total_reviews_top10:>12,}")
    w(f"  #1 product review count    : {max_count:>12,}")
    w("")

    w(f"  {'Rank':<5}  {'ASIN':<15}  {'Reviews':>10}  Visual (relative to #1)")
    w(f"  {'─'*5}  {'─'*15}  {'─'*10}  {'─'*40}")

    for rank, info in sorted_results:
        asin  = info["asin"]
        count = info["review_count"]
        bar   = _bar(count, max_count)
        pct   = count / max_count * 100
        medal = _medal(rank)
        w(f"  {medal:<5}  {asin:<15}  {count:>10,}  {bar}  {pct:5.1f}%")

    w("")
    w("  (Rank 1 = bar filled 100%; others scaled proportionally)")

    # ── Gap analysis ──────────────────────────────────────────────────────────
    if len(sorted_results) >= 2:
        w(f"\nGAP ANALYSIS  (reviews lost per rank drop)")
        w("-" * 55)
        counts = [v["review_count"] for _, v in sorted_results]
        for i in range(len(counts) - 1):
            gap = counts[i] - counts[i + 1]
            w(f"  Rank {i+1} → Rank {i+2} : -{gap:>8,} reviews")

    w("\n" + sep)

    with open(out_path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    w(f"\n  Results saved → {out_path}")


# ──────────────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────────────

def main():
    sys.stdout.reconfigure(encoding="utf-8")

    input_file = "Health_and_Household.jsonl"
    sample = 0

    args = sys.argv[1:]
    if "--sample" in args:
        idx = args.index("--sample")
        sample = int(args[idx + 1])
        args = args[:idx] + args[idx + 2:]
    if args:
        input_file = args[0]

    tmp_path = None
    if sample > 0:
        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl",
                                          delete=False, encoding="utf-8")
        with open(input_file, encoding="utf-8") as src:
            for i, line in enumerate(src):
                if i >= sample:
                    break
                tmp.write(line)
        tmp.close()
        tmp_path = tmp.name
        input_file = tmp_path
        print(f"Using sample: first {sample:,} records")

    print("Running Job 4: Finding Top 10 most-reviewed products … (2 MapReduce steps)")

    job = MRTopTenProducts(args=["-r", "inline", "--no-conf", input_file])
    results = []
    with job.make_runner() as runner:
        runner.run()
        for line in runner.cat_output():
            line = line.decode("utf-8").strip()
            if "\t" in line:
                k, v = line.split("\t", 1)
                results.append((json.loads(k), json.loads(v)))

    if tmp_path:
        os.unlink(tmp_path)

    print_and_save(results, "job4_results.txt")


if __name__ == "__main__":
    main()
