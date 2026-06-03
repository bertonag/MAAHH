"""
MapReduce Job 3: Compute the Average Star Rating for Each Product.

  (a) Mapper  – emits (asin, (rating, 1)) for every review.
  (b) Reducer – accumulates total rating and count, yields average.

Demonstrates aggregating numerical data across the full dataset.

Run:
    python job3_average_rating.py
    python job3_average_rating.py --sample 100000
"""

import json
import sys
import os
import tempfile
from mrjob.job import MRJob
from mrjob.step import MRStep


# ──────────────────────────────────────────────────────────────────────────────
# MapReduce definition
# ──────────────────────────────────────────────────────────────────────────────

class MRAverageRating(MRJob):

    def steps(self):
        return [MRStep(mapper=self.mapper,
                       combiner=self.combiner,
                       reducer=self.reducer)]

    def mapper(self, _, line):
        """(a) Emit (asin, [rating_sum, count]) for each review."""
        try:
            rec = json.loads(line)
        except (json.JSONDecodeError, ValueError):
            return
        asin   = rec.get("asin")
        rating = rec.get("rating")
        if asin and rating is not None:
            yield asin, [rating, 1]

    def combiner(self, asin, values):
        """Local pre-aggregation to reduce shuffle data."""
        total = 0.0
        count = 0
        for rating_sum, cnt in values:
            total += rating_sum
            count += cnt
        yield asin, [total, count]

    def reducer(self, asin, values):
        """(b) Aggregate numerical data → compute average rating."""
        total = 0.0
        count = 0
        for rating_sum, cnt in values:
            total += rating_sum
            count += cnt
        if count > 0:
            yield asin, {"avg_rating": round(total / count, 3),
                         "total_rating": total,
                         "review_count": count}


# ──────────────────────────────────────────────────────────────────────────────
# Output formatting
# ──────────────────────────────────────────────────────────────────────────────

def _stars(avg):
    """ASCII star bar out of 5."""
    filled = int(round(avg))
    return "*" * filled + "-" * (5 - filled)


def print_and_save(results, out_path):
    lines = []
    def w(text=""):
        print(text)
        lines.append(str(text))

    # results: list of (asin, {"avg_rating":..., "review_count":...})
    total_products = len(results)
    all_avgs   = [v["avg_rating"]   for _, v in results]
    all_counts = [v["review_count"] for _, v in results]

    overall_avg = sum(v["avg_rating"] * v["review_count"] for _, v in results) \
                  / sum(all_counts) if sum(all_counts) else 0

    sep = "=" * 65
    w(sep)
    w("  JOB 3: AVERAGE STAR RATING PER PRODUCT")
    w(sep)

    w(f"\n  Total unique products   : {total_products:>12,}")
    w(f"  Overall weighted avg    : {overall_avg:>12.3f}  {_stars(overall_avg)}")
    w(f"  Highest product avg     : {max(all_avgs):>12.3f}")
    w(f"  Lowest  product avg     : {min(all_avgs):>12.3f}")
    w(f"  Mean of product avgs    : {sum(all_avgs)/len(all_avgs):>12.3f}"
      if all_avgs else "")

    # ── Rating-group distribution ─────────────────────────────────────────────
    w("\nPRODUCT AVERAGE RATING DISTRIBUTION")
    w("-" * 50)
    brackets = [
        ("Excellent  (4.5 – 5.0)", lambda a: 4.5 <= a <= 5.0),
        ("Good       (4.0 – 4.4)", lambda a: 4.0 <= a < 4.5),
        ("Average    (3.0 – 3.9)", lambda a: 3.0 <= a < 4.0),
        ("Below avg  (2.0 – 2.9)", lambda a: 2.0 <= a < 3.0),
        ("Poor       (1.0 – 1.9)", lambda a: a < 2.0),
    ]
    for label, pred in brackets:
        cnt = sum(1 for a in all_avgs if pred(a))
        pct = cnt / total_products * 100 if total_products else 0
        bar_len = int(pct / 100 * 35)
        bar = "█" * bar_len + "░" * (35 - bar_len)
        w(f"  {label:<26} |{bar}|  {cnt:>7,}  ({pct:5.1f}%)")

    # ── Top 20 highest-rated products (min 10 reviews) ────────────────────────
    qualified = [(asin, v) for asin, v in results if v["review_count"] >= 10]
    top_rated = sorted(qualified, key=lambda x: x[1]["avg_rating"], reverse=True)[:20]

    w(f"\nTOP 20 HIGHEST-RATED PRODUCTS  (min 10 reviews)")
    w("-" * 65)
    w(f"  {'Rank':<5} {'ASIN':<15} {'Avg Rating':>10}  {'Stars':<8} {'Reviews':>8}")
    w(f"  {'-'*5} {'-'*15} {'-'*10}  {'-'*8} {'-'*8}")
    for rank, (asin, v) in enumerate(top_rated, 1):
        w(f"  {rank:<5} {asin:<15} {v['avg_rating']:>10.3f}  "
          f"{_stars(v['avg_rating']):<7} {v['review_count']:>8,}")

    # ── Bottom 20 lowest-rated products (min 10 reviews) ─────────────────────
    bottom_rated = sorted(qualified,
                          key=lambda x: x[1]["avg_rating"])[:20]
    w(f"\nBOTTOM 20 LOWEST-RATED PRODUCTS  (min 10 reviews)")
    w("-" * 65)
    w(f"  {'Rank':<5} {'ASIN':<15} {'Avg Rating':>10}  {'Stars':<7} {'Reviews':>8}")
    w(f"  {'-'*5} {'-'*15} {'-'*10}  {'-'*7} {'-'*8}")
    for rank, (asin, v) in enumerate(bottom_rated, 1):
        w(f"  {rank:<5} {asin:<15} {v['avg_rating']:>10.3f}  "
          f"{_stars(v['avg_rating']):<7} {v['review_count']:>8,}")

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

    print("Running Job 3: Computing average rating per product ...")

    job = MRAverageRating(args=["-r", "inline", "--no-conf", input_file])
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

    print_and_save(results, "job3_results.txt")


if __name__ == "__main__":
    main()
