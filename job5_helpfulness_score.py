"""
MapReduce Job 5: Calculate the Average Helpfulness Score for Reviews.

Dataset note
────────────
The dataset field `helpful_vote` records how many users marked a review as
helpful.  There is NO separate "total votes" (helpful + unhelpful) field in
this version of the dataset, so the ratio formula

    Helpfulness Score = helpful_votes / (helpful_votes + unhelpful_votes)

cannot be computed directly.

This job therefore reports:
  • Average helpful_vote count (overall and per star-rating group)
  • Distribution of helpful_vote values across all reviews
  • Top-helpfulness reviews (highest helpful_vote counts)

(a) Mapper  – emits grouped keys: "overall", "rating_<N>", and per-bucket keys.
(b) Reducer – sums totals and counts, then yields the average.

Run:
    python job5_helpfulness_score.py
    python job5_helpfulness_score.py --sample 50000
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

class MRHelpfulnessScore(MRJob):

    def steps(self):
        return [MRStep(mapper=self.mapper,
                       combiner=self.combiner,
                       reducer=self.reducer)]

    def mapper(self, _, line):
        """(a) Emit helpful_vote totals grouped by rating and bucket."""
        try:
            rec = json.loads(line)
        except (json.JSONDecodeError, ValueError):
            return

        hv     = int(rec.get("helpful_vote") or 0)
        rating = rec.get("rating")

        # overall aggregate
        yield "overall", [hv, 1]

        # per star-rating group
        if rating is not None:
            yield f"rating_{int(rating)}_star", [hv, 1]

        # helpful_vote bucket distribution
        if hv == 0:
            yield "bucket_0", [hv, 1]
        elif hv <= 5:
            yield "bucket_1-5", [hv, 1]
        elif hv <= 10:
            yield "bucket_6-10", [hv, 1]
        elif hv <= 50:
            yield "bucket_11-50", [hv, 1]
        elif hv <= 100:
            yield "bucket_51-100", [hv, 1]
        else:
            yield "bucket_101+", [hv, 1]

        # verified vs unverified
        vp = rec.get("verified_purchase")
        if vp is True:
            yield "verified_true", [hv, 1]
        elif vp is False:
            yield "verified_false", [hv, 1]

    def combiner(self, key, values):
        total = 0
        count = 0
        for hv, n in values:
            total += hv
            count += n
        yield key, [total, count]

    def reducer(self, key, values):
        """(b) Compute average helpful_vote for each group."""
        total = 0
        count = 0
        for hv, n in values:
            total += hv
            count += n
        if count > 0:
            yield key, {"total_helpful_votes": total,
                        "review_count": count,
                        "avg_helpful_votes": round(total / count, 4)}


# ──────────────────────────────────────────────────────────────────────────────
# Output formatting
# ──────────────────────────────────────────────────────────────────────────────

def _bar(val, max_val, width=30):
    filled = int(val / max_val * width) if max_val else 0
    return "█" * filled + "░" * (width - filled)


def print_and_save(results, out_path):
    lines = []
    def w(text=""):
        print(text)
        lines.append(str(text))

    d = {k: v for k, v in results}

    sep = "=" * 65
    w(sep)
    w("  JOB 5: AVERAGE HELPFULNESS SCORE FOR REVIEWS")
    w(sep)

    w("\n  NOTE: The dataset contains `helpful_vote` (number of users who")
    w("  marked a review helpful) but does NOT include total votes")
    w("  (helpful + unhelpful). Scores below use helpful_vote directly.\n")

    # ── Overall ───────────────────────────────────────────────────────────────
    ov = d.get("overall", {})
    total_reviews = ov.get("review_count",        0)
    total_hv      = ov.get("total_helpful_votes",  0)
    avg_hv        = ov.get("avg_helpful_votes",    0)
    w("OVERALL HELPFULNESS STATISTICS")
    w("-" * 40)
    w(f"  Total reviews examined  : {total_reviews:>12,}")
    w(f"  Total helpful votes     : {total_hv:>12,}")
    w(f"  Avg helpful votes/review: {avg_hv:>12.4f}")

    # ── Per star-rating ───────────────────────────────────────────────────────
    w("\nAVERAGE HELPFUL VOTES BY STAR RATING")
    w("-" * 55)
    w(f"  {'Rating':<12}  {'Avg Helpful Votes':>18}  {'# Reviews':>12}")
    w(f"  {'─'*12}  {'─'*18}  {'─'*12}")
    max_avg = max((d.get(f"rating_{s}_star", {}).get("avg_helpful_votes", 0)
                   for s in range(1, 6)), default=1)
    for star in range(1, 6):
        info = d.get(f"rating_{star}_star", {})
        avg  = info.get("avg_helpful_votes", 0)
        cnt  = info.get("review_count", 0)
        bar  = _bar(avg, max_avg, width=25)
        w(f"  {star}*  {'*'*star+'-'*(5-star):<6}  {avg:>8.4f}  "
          f"|{bar}|  {cnt:>10,}")

    # ── Bucket distribution ───────────────────────────────────────────────────
    w("\nHELPFUL VOTE COUNT DISTRIBUTION")
    w("-" * 65)
    buckets = [
        ("bucket_0",      "0 (no votes)  "),
        ("bucket_1-5",    "1 – 5 votes   "),
        ("bucket_6-10",   "6 – 10 votes  "),
        ("bucket_11-50",  "11 – 50 votes "),
        ("bucket_51-100", "51 – 100 votes"),
        ("bucket_101+",   "101+ votes    "),
    ]
    max_cnt = max(
        (d.get(k, {}).get("review_count", 0) for k, _ in buckets), default=1
    )
    w(f"  {'Bucket':<18}  {'Reviews':>10}  {'%':>6}  Bar")
    w(f"  {'─'*18}  {'─'*10}  {'─'*6}  {'─'*30}")
    for key, label in buckets:
        info = d.get(key, {})
        cnt  = info.get("review_count", 0)
        pct  = cnt / total_reviews * 100 if total_reviews else 0
        bar  = _bar(cnt, max_cnt)
        w(f"  {label}  {cnt:>10,}  {pct:>5.1f}%  {bar}")

    # ── Verified vs Unverified ────────────────────────────────────────────────
    w("\nHELPFULNESS BY PURCHASE VERIFICATION")
    w("-" * 55)
    for key, label in [("verified_true", "Verified Purchase  "),
                        ("verified_false","Unverified Purchase")]:
        info = d.get(key, {})
        avg  = info.get("avg_helpful_votes", 0)
        cnt  = info.get("review_count", 0)
        w(f"  {label} : avg {avg:.4f} helpful votes  ({cnt:,} reviews)")

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

    print("Running Job 5: Computing helpfulness scores …")

    job = MRHelpfulnessScore(args=["-r", "inline", "--no-conf", input_file])
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

    print_and_save(results, "job5_results.txt")


if __name__ == "__main__":
    main()
