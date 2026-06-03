"""
MapReduce Job 1: Examine the Health_and_Household.jsonl dataset structure.

Collects:
  - Total record count and parse errors
  - Field presence / missing rates for all 10 fields
  - Star rating distribution (1-5)
  - verified_purchase breakdown
  - Image attachment statistics
  - Review text length (min / max / avg)
  - Timestamp date range (earliest and latest review)
  - helpful_vote distribution buckets

Run:
    python job1_examine_dataset.py                        # full dataset
    python job1_examine_dataset.py --sample 50000         # first 50k records
"""

import json
import sys
import os
import tempfile
from datetime import datetime, timezone
from mrjob.job import MRJob
from mrjob.step import MRStep


class MRExamineDataset(MRJob):

    def steps(self):
        return [MRStep(mapper=self.mapper, reducer=self.reducer)]

    def mapper(self, _, line):
        try:
            rec = json.loads(line)
        except (json.JSONDecodeError, ValueError):
            yield "meta:parse_error", 1
            return

        yield "meta:total_records", 1

        # field presence
        fields = [
            "rating", "title", "text", "images", "asin",
            "parent_asin", "user_id", "timestamp", "helpful_vote", "verified_purchase",
        ]
        for field in fields:
            val = rec.get(field)
            present = val is not None and val != "" and val != []
            yield f"field_present:{field}", 1 if present else 0
            yield f"field_missing:{field}", 0 if present else 1

        # rating distribution
        rating = rec.get("rating")
        if rating is not None:
            yield f"rating:{int(rating)}_star", 1

        # verified_purchase
        vp = rec.get("verified_purchase")
        if vp is True:
            yield "verified_purchase:true", 1
        elif vp is False:
            yield "verified_purchase:false", 1
        else:
            yield "verified_purchase:missing", 1

        # image counts
        images = rec.get("images", [])
        img_count = len(images) if isinstance(images, list) else 0
        bucket = "0" if img_count == 0 else "1" if img_count == 1 else "2+"
        yield f"image_count:{bucket}", 1
        yield "image_count:total_images", img_count

        # text length
        text = rec.get("text") or ""
        tl = len(text)
        yield "text_length:sum", tl
        yield "text_length:min", tl
        yield "text_length:max", tl
        yield "text_length:count", 1

        # helpful_vote buckets
        hv = rec.get("helpful_vote") or 0
        if hv == 0:
            yield "helpful_vote:0", 1
        elif hv <= 5:
            yield "helpful_vote:1-5", 1
        elif hv <= 20:
            yield "helpful_vote:6-20", 1
        else:
            yield "helpful_vote:21+", 1

        # timestamp range (ms → keep as int for min/max)
        ts = rec.get("timestamp")
        if ts:
            yield "timestamp:min", int(ts)
            yield "timestamp:max", int(ts)

    def reducer(self, key, values):
        if key in ("text_length:min", "timestamp:min"):
            yield key, min(values)
        elif key in ("text_length:max", "timestamp:max"):
            yield key, max(values)
        else:
            yield key, sum(values)


# ──────────────────────────────────────────────────────────────────────────────
# Output helpers
# ──────────────────────────────────────────────────────────────────────────────

def _bar(count, total, width=35):
    pct = count / total * 100 if total else 0
    filled = int(pct / 100 * width)
    return f"{'█' * filled}{'░' * (width - filled)}", pct


def _ts_to_date(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


def print_and_save(data, out_path):
    """Format results, print to console, and write to out_path."""
    d = dict(data)
    total = d.get("meta:total_records", 0)
    errors = d.get("meta:parse_error", 0)

    lines = []
    def w(*args, **kwargs):
        text = " ".join(str(a) for a in args)
        print(text, **kwargs)
        lines.append(text)

    sep = "=" * 65
    w(sep)
    w("  JOB 1: DATASET STRUCTURE EXAMINATION")
    w("  Source: Health_and_Household.jsonl  (Amazon Product Reviews)")
    w(sep)

    # ── Overview ──────────────────────────────────────────────────────────────
    w("\nDATASET OVERVIEW")
    w("-" * 40)
    w(f"  Total Records  : {total:>15,}")
    w(f"  Parse Errors   : {errors:>15,}")

    # ── Field completeness ────────────────────────────────────────────────────
    w("\nFIELD COMPLETENESS")
    w("-" * 40)
    w(f"  {'Field':<20} {'Present':>12} {'Missing':>12}  {'Null %':>8}")
    w(f"  {'-'*20} {'-'*12} {'-'*12}  {'-'*8}")
    fields = [
        "rating", "title", "text", "images", "asin",
        "parent_asin", "user_id", "timestamp", "helpful_vote", "verified_purchase",
    ]
    for f in fields:
        present = d.get(f"field_present:{f}", 0)
        missing = d.get(f"field_missing:{f}", 0)
        pct = missing / total * 100 if total else 0
        w(f"  {f:<20} {present:>12,} {missing:>12,}  {pct:>7.2f}%")

    # ── Rating distribution ───────────────────────────────────────────────────
    w("\nRATING DISTRIBUTION")
    w("-" * 40)
    for star in range(1, 6):
        cnt = d.get(f"rating:{star}_star", 0)
        bar, pct = _bar(cnt, total)
        w(f"  {star} star  |{bar}|  {cnt:>9,}  ({pct:5.1f}%)")

    # ── Verified purchase ─────────────────────────────────────────────────────
    w("\nVERIFIED PURCHASE BREAKDOWN")
    w("-" * 40)
    for label in ("true", "false", "missing"):
        cnt = d.get(f"verified_purchase:{label}", 0)
        bar, pct = _bar(cnt, total)
        w(f"  {label:<8} |{bar}|  {cnt:>9,}  ({pct:5.1f}%)")

    # ── Image statistics ──────────────────────────────────────────────────────
    w("\nIMAGE ATTACHMENT STATISTICS")
    w("-" * 40)
    for bucket, label in [("0", "No images"), ("1", "1 image"), ("2+", "2+ images")]:
        cnt = d.get(f"image_count:{bucket}", 0)
        bar, pct = _bar(cnt, total)
        w(f"  {label:<10} |{bar}|  {cnt:>9,}  ({pct:5.1f}%)")
    total_imgs = d.get("image_count:total_images", 0)
    w(f"\n  Total images across dataset : {total_imgs:,}")
    w(f"  Average images per review   : {total_imgs/total:.3f}" if total else "")

    # ── Text length ───────────────────────────────────────────────────────────
    w("\nREVIEW TEXT LENGTH")
    w("-" * 40)
    tl_min  = d.get("text_length:min",   0)
    tl_max  = d.get("text_length:max",   0)
    tl_sum  = d.get("text_length:sum",   0)
    tl_cnt  = d.get("text_length:count", 1)
    w(f"  Min length : {tl_min:>8,} chars")
    w(f"  Max length : {tl_max:>8,} chars")
    w(f"  Avg length : {tl_sum/tl_cnt:>8,.1f} chars")

    # ── Timestamp range ───────────────────────────────────────────────────────
    w("\nREVIEW DATE RANGE")
    w("-" * 40)
    ts_min = d.get("timestamp:min")
    ts_max = d.get("timestamp:max")
    if ts_min and ts_max:
        w(f"  Earliest review : {_ts_to_date(ts_min)}")
        w(f"  Latest  review  : {_ts_to_date(ts_max)}")
        span = (ts_max - ts_min) / (1000 * 86400 * 365.25)
        w(f"  Date span       : {span:.1f} years")

    # ── Helpful vote buckets ──────────────────────────────────────────────────
    w("\nHELPFUL VOTE DISTRIBUTION")
    w("-" * 40)
    for bucket, label in [("0", "0 votes"), ("1-5", "1–5 votes"),
                           ("6-20", "6–20 votes"), ("21+", "21+ votes")]:
        cnt = d.get(f"helpful_vote:{bucket}", 0)
        bar, pct = _bar(cnt, total)
        w(f"  {label:<10} |{bar}|  {cnt:>9,}  ({pct:5.1f}%)")

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

    # simple --sample N flag (without conflicting with mrjob's arg parser)
    args = sys.argv[1:]
    if "--sample" in args:
        idx = args.index("--sample")
        sample = int(args[idx + 1])
        args = args[:idx] + args[idx + 2:]
    if args:
        input_file = args[0]

    # optionally create a temp sample file
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

    print(f"Running Job 1: Examining dataset …  (this may take a few minutes)")

    job = MRExamineDataset(args=["-r", "inline", "--no-conf", input_file])
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

    print_and_save(results, "job1_results.txt")


if __name__ == "__main__":
    main()
