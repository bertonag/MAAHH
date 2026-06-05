"""
MapReduce Job 2: Count the Number of Reviews per Product.

  (a) Mapper  – emits (asin, 1) for every review line.
  (b) Reducer – sums the 1s to get the total review count per product.

Output printed to console and saved to job2_results.txt.

Run:
    python job2_reviews_per_product.py
    python job2_reviews_per_product.py --sample 50000
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

class MRReviewsPerProduct(MRJob):

    def steps(self):
        return [MRStep(mapper=self.mapper, reducer=self.reducer)]

    def mapper(self, _, line):
        """(a) Map each review to its product ID."""
        try:
            rec = json.loads(line)
        except (json.JSONDecodeError, ValueError):
            return
        asin = rec.get("asin")
        if asin:
            yield asin, 1

    def reducer(self, asin, counts):
        """(b) Sum review counts for each product."""
        yield asin, sum(counts)


# ──────────────────────────────────────────────────────────────────────────────
# Output formatting
# ──────────────────────────────────────────────────────────────────────────────

def _bar(count, max_count, width=30):
    filled = int(count / max_count * width) if max_count else 0
    return "█" * filled + "░" * (width - filled)


def print_and_save(results, out_path):
    lines = []
    def w(text=""):
        print(text)
        lines.append(str(text))

    # Sort by review count descending
    sorted_results = sorted(results, key=lambda x: x[1], reverse=True)
    total_products = len(sorted_results)
    total_reviews  = sum(v for _, v in sorted_results)
    max_count      = sorted_results[0][1] if sorted_results else 1

    counts_only = [v for _, v in sorted_results]
    avg_reviews = total_reviews / total_products if total_products else 0
    median_idx  = total_products // 2
    median      = counts_only[median_idx] if counts_only else 0

    sep = "=" * 65
    w(sep)
    w("  JOB 2: NUMBER OF REVIEWS PER PRODUCT")
    w(sep)

    w(f"\n  Total unique products  : {total_products:>12,}")
    w(f"  Total reviews          : {total_reviews:>12,}")
    w(f"  Avg reviews / product  : {avg_reviews:>12.1f}")
    w(f"  Median reviews/product : {median:>12,}")
    w(f"  Max reviews (1 product): {max_count:>12,}")
    w(f"  Min reviews (1 product): {counts_only[-1]:>12,}" if counts_only else "")

    # ── Distribution buckets ──────────────────────────────────────────────────
    w("\nREVIEW COUNT DISTRIBUTION (buckets)")
    w("-" * 40)
    buckets = [(1, 1), (2, 5), (6, 20), (21, 100), (101, 500), (501, None)]
    for lo, hi in buckets:
        if hi is None:
            cnt = sum(1 for v in counts_only if v > lo)
            label = f"{lo}+ reviews"
        else:
            cnt = sum(1 for v in counts_only if lo <= v <= hi)
            label = f"{lo}-{hi} reviews" if lo != hi else f"{lo} review"
        pct = cnt / total_products * 100 if total_products else 0
        w(f"  {label:<16} : {cnt:>8,} products  ({pct:5.1f}%)")

    # ── Top 20 products ───────────────────────────────────────────────────────
    w(f"\nTOP 20 MOST-REVIEWED PRODUCTS")
    w("-" * 65)
    w(f"  {'Rank':<6} {'Product (ASIN)':<15} {'Reviews':>10}  Bar")
    w(f"  {'-'*6} {'-'*15} {'-'*10}  {'-'*30}")
    for rank, (asin, cnt) in enumerate(sorted_results[:20], 1):
        bar = _bar(cnt, max_count)
        w(f"  {rank:<6} {asin:<15} {cnt:>10,}  {bar}")

    # ── Bottom 10 products ────────────────────────────────────────────────────
    w(f"\nBOTTOM 10 LEAST-REVIEWED PRODUCTS")
    w("-" * 50)
    w(f"  {'Rank':<10} {'ASIN':<15} {'Reviews':>10}")
    w(f"  {'-'*10} {'-'*15} {'-'*10}")
    for rank, (asin, cnt) in enumerate(
            sorted_results[-10:], total_products - 9):
        w(f"  {rank:<10} {asin:<15} {cnt:>10,}")

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

    print("Running Job 2: Counting reviews per product …")

    job = MRReviewsPerProduct(args=["-r", "inline", "--no-conf", input_file])
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

    print_and_save(results, "job2_results.txt")


if __name__ == "__main__":
    main()
