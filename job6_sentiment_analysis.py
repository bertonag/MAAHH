"""
MapReduce Job 6: Sentiment Analysis of Review Text.

  (a) Each review's text is analysed with a keyword-based scorer.
  (b) Sentiment is categorised as POSITIVE, NEUTRAL, or NEGATIVE.
  (c) Distribution is printed as an ASCII bar chart and, if matplotlib is
      available, also saved as job6_sentiment_chart.png.

Sentiment scoring (no external NLP library needed):
  • Count positive-signal words in the review text.
  • Count negative-signal words in the review text.
  • net = pos_count - neg_count
  • If net > 0  → positive
  • If net < 0  → negative
  • If net == 0 → use rating as tiebreak (≥4 positive, ≤2 negative, else neutral)

Run:
    python job6_sentiment_analysis.py
    python job6_sentiment_analysis.py --sample 50000
"""

import json
import sys
import os
import re
import tempfile
from mrjob.job import MRJob
from mrjob.step import MRStep


# ──────────────────────────────────────────────────────────────────────────────
# Sentiment word lists
# ──────────────────────────────────────────────────────────────────────────────

POSITIVE_WORDS = frozenset({
    "excellent", "amazing", "fantastic", "wonderful", "perfect", "outstanding",
    "superb", "brilliant", "great", "good", "love", "loved", "best", "awesome",
    "terrific", "nice", "happy", "pleased", "satisfied", "recommend",
    "recommended", "quality", "works", "worked", "impressed", "impressive",
    "effective", "comfortable", "convenient", "reliable", "durable", "enjoy",
    "enjoyed", "easy", "fresh", "clean", "safe", "healthy", "value", "worth",
    "helpful", "helpful", "delightful", "smooth", "gentle", "natural",
    "beautiful", "fabulous", "gorgeous", "lovely", "sturdy", "solid",
})

NEGATIVE_WORDS = frozenset({
    "terrible", "awful", "horrible", "disgusting", "worst", "bad", "poor",
    "hate", "hated", "dislike", "disappointing", "disappointed", "useless",
    "broken", "defective", "faulty", "waste", "garbage", "trash", "cheap",
    "flimsy", "fragile", "smell", "smells", "stinks", "rotten", "expired",
    "wrong", "return", "returned", "refund", "avoid", "never", "fake",
    "counterfeit", "misleading", "cheated", "scam", "fraud", "unusable",
    "damaged", "leaking", "leaks", "cracked", "ineffective", "itching",
    "irritate", "rash", "allergy", "allergic", "toxic", "dangerous",
})

_WORD_RE = re.compile(r"[a-z]+")


def classify_sentiment(text: str, rating: float) -> str:
    """Rule-based sentiment classification."""
    words = _WORD_RE.findall((text or "").lower())
    word_set = set(words)
    pos = len(word_set & POSITIVE_WORDS)
    neg = len(word_set & NEGATIVE_WORDS)
    net = pos - neg

    if net > 0:
        return "positive"
    if net < 0:
        return "negative"
    # tiebreak on star rating
    if rating is not None:
        if rating >= 4.0:
            return "positive"
        if rating <= 2.0:
            return "negative"
    return "neutral"


# ──────────────────────────────────────────────────────────────────────────────
# MapReduce definition
# ──────────────────────────────────────────────────────────────────────────────

class MRSentimentAnalysis(MRJob):

    def steps(self):
        return [MRStep(mapper=self.mapper,
                       combiner=self.combiner,
                       reducer=self.reducer)]

    def mapper(self, _, line):
        """(a) Classify each review and emit keyed counts."""
        try:
            rec = json.loads(line)
        except (json.JSONDecodeError, ValueError):
            return

        text   = rec.get("text", "") or ""
        rating = rec.get("rating")
        vp     = rec.get("verified_purchase")

        sentiment = classify_sentiment(text, rating)

        # overall sentiment count
        yield f"sentiment:{sentiment}", 1

        # sentiment × star rating
        if rating is not None:
            yield f"sentiment_x_rating:{sentiment}:{int(rating)}", 1

        # sentiment × verified_purchase
        vp_label = "verified" if vp else "unverified"
        yield f"sentiment_x_verified:{sentiment}:{vp_label}", 1

        # total processed
        yield "meta:total", 1

    def combiner(self, key, values):
        yield key, sum(values)

    def reducer(self, key, values):
        """(b) Sum counts for each sentiment category and cross-tab."""
        yield key, sum(values)


# ──────────────────────────────────────────────────────────────────────────────
# Output formatting + (c) visualization
# ──────────────────────────────────────────────────────────────────────────────

ICONS  = {"positive": "[+]", "neutral": "[~]", "negative": "[-]"}


def _bar_ascii(count, total, width=40):
    pct = count / total * 100 if total else 0
    filled = int(pct / 100 * width)
    return "█" * filled + "░" * (width - filled), pct


def _sentiment_chart(counts, total, out_png):
    """Save a matplotlib pie + bar chart if matplotlib is available."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.patches as mpatches
    except ImportError:
        return False

    labels  = list(counts.keys())
    values  = list(counts.values())
    palette = {"positive": "#4CAF50", "neutral": "#FFC107", "negative": "#F44336"}
    colors  = [palette.get(l, "#888") for l in labels]
    pcts    = [v / total * 100 for v in values]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
    fig.suptitle("Sentiment Distribution – Health & Household Reviews",
                 fontsize=14, fontweight="bold")

    # Pie chart
    wedges, texts, autotexts = ax1.pie(
        values, labels=labels, colors=colors,
        autopct="%1.1f%%", startangle=140,
        textprops={"fontsize": 11},
    )
    for at in autotexts:
        at.set_fontweight("bold")
    ax1.set_title("Proportion of Sentiments")

    # Horizontal bar chart
    bars = ax2.barh(labels, values, color=colors, edgecolor="white", height=0.5)
    ax2.set_xlabel("Number of Reviews")
    ax2.set_title("Review Count by Sentiment")
    ax2.xaxis.set_major_formatter(
        matplotlib.ticker.FuncFormatter(lambda x, _: f"{int(x):,}")
    )
    for bar, v, p in zip(bars, values, pcts):
        ax2.text(bar.get_width() * 1.01, bar.get_y() + bar.get_height() / 2,
                 f"{v:,}  ({p:.1f}%)", va="center", fontsize=10)
    ax2.set_xlim(0, max(values) * 1.2)

    plt.tight_layout()
    plt.savefig(out_png, dpi=150, bbox_inches="tight")
    plt.close()
    return True


def print_and_save(results, out_path, out_png="job6_sentiment_chart.png"):
    lines = []
    def w(text=""):
        print(text)
        lines.append(str(text))

    d = {k: v for k, v in results}
    total = d.get("meta:total", 0)

    sentiments = ["positive", "neutral", "negative"]
    counts = {s: d.get(f"sentiment:{s}", 0) for s in sentiments}

    sep = "=" * 65
    w(sep)
    w("  JOB 6: SENTIMENT ANALYSIS OF REVIEW TEXT")
    w(sep)

    w(f"\n  Total reviews processed : {total:>12,}")
    w(f"  Sentiment method        : keyword scoring + rating tiebreak")
    w(f"  Positive keywords       : {len(POSITIVE_WORDS)} words")
    w(f"  Negative keywords       : {len(NEGATIVE_WORDS)} words\n")

    # ── (b) Sentiment distribution ────────────────────────────────────────────
    w("(b) OVERALL SENTIMENT DISTRIBUTION")
    w("-" * 65)
    w(f"  {'Sentiment':<12}  {'Count':>12}  {'Percentage':>10}  Visualisation")
    w(f"  {'─'*12}  {'─'*12}  {'─'*10}  {'─'*40}")
    for s in sentiments:
        cnt = counts[s]
        bar, pct = _bar_ascii(cnt, total)
        icon = ICONS[s]
        w(f"  {icon} {s:<10}  {cnt:>12,}  {pct:>9.2f}%  {bar}")

    # ── Sentiment × star rating cross-tab ─────────────────────────────────────
    w("\n(c) SENTIMENT BREAKDOWN BY STAR RATING")
    w("-" * 65)
    header = f"  {'Star':<6}" + "".join(f"  {s.capitalize():<12}" for s in sentiments)
    w(header)
    w("  " + "─" * 63)
    for star in range(1, 6):
        row = f"  {star}★    "
        for s in sentiments:
            cnt = d.get(f"sentiment_x_rating:{s}:{star}", 0)
            pct = cnt / total * 100 if total else 0
            row += f"  {cnt:>8,} ({pct:4.1f}%)"
        w(row)

    # ── Verified vs unverified ────────────────────────────────────────────────
    w("\n(c) SENTIMENT BREAKDOWN BY PURCHASE VERIFICATION")
    w("-" * 65)
    for vp_label in ("verified", "unverified"):
        w(f"\n  {vp_label.capitalize()} purchases:")
        for s in sentiments:
            cnt = d.get(f"sentiment_x_verified:{s}:{vp_label}", 0)
            bar, pct = _bar_ascii(cnt, total)
            w(f"    {ICONS[s]} {s:<10}: {cnt:>10,}  ({pct:5.1f}%)")

    # ── Key insight ───────────────────────────────────────────────────────────
    w("\nKEY INSIGHTS")
    w("-" * 40)
    dom = max(counts, key=counts.get)
    pct_dom = counts[dom] / total * 100 if total else 0
    w(f"  Dominant sentiment  : {ICONS[dom]} {dom.upper()}  ({pct_dom:.1f}% of reviews)")

    neg_pct = counts["negative"] / total * 100 if total else 0
    pos_pct = counts["positive"] / total * 100 if total else 0
    ratio   = pos_pct / neg_pct if neg_pct else float("inf")
    w(f"  Positive : Negative : {ratio:.1f} : 1")
    w(f"  Positive reviews    : {counts['positive']:,}")
    w(f"  Neutral  reviews    : {counts['neutral']:,}")
    w(f"  Negative reviews    : {counts['negative']:,}")

    w("\n" + sep)

    # ── (c) Visualisation ─────────────────────────────────────────────────────
    saved_chart = _sentiment_chart(counts, total, out_png)
    if saved_chart:
        w(f"\n  Chart saved → {out_png}")
    else:
        w("\n  (matplotlib not installed – ASCII chart above is the visualisation)")

    with open(out_path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    w(f"  Results saved → {out_path}")


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

    print("Running Job 6: Sentiment analysis …")

    job = MRSentimentAnalysis(args=["-r", "inline", "--no-conf", input_file])
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

    print_and_save(results, "job6_results.txt")


if __name__ == "__main__":
    main()
