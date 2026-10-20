"""Generate the combined E2/E3 performance figure from benchmark evidence."""

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


PROJECT_ROOT: Path = Path(__file__).resolve().parents[1]
CSV: Path = PROJECT_ROOT / "report" / "evidence" / "e2-e3-performance.csv"
FIGURES: Path = PROJECT_ROOT / "report" / "figures"


def main() -> None:
    """Create the combined warm/cold bar-and-whisker performance figure."""
    if not CSV.exists():
        raise FileNotFoundError(
            f"{CSV} does not exist; run experiments/verify_perf.py first"
        )

    FIGURES.mkdir(parents=True, exist_ok=True)
    data = pd.read_csv(CSV)
    summary = (
        data.groupby(["system", "mode", "workers"])["seconds"]
        .agg(median="median", low="min", high="max")
        .reset_index()
    )

    labels = ["Centralized\nSQLite", "1 worker", "2 workers", "4 workers", "8 workers"]
    colors = ["#9e9e9e", "#b388ff", "#b388ff", "#b388ff", "#b388ff"]
    configurations = [
        ("centralized SQLite", 1),
        ("MiniShardDB", 1),
        ("MiniShardDB", 2),
        ("MiniShardDB", 4),
        ("MiniShardDB", 8),
    ]

    figure, axes = plt.subplots(1, 2, figsize=(13, 5.5), sharey=True)
    figure.suptitle(
        "MiniShardDB and centralized SQLite performance",
        fontsize=16,
        fontweight="bold",
    )

    for axis, mode in zip(axes, ("warm", "cold")):
        values = []
        lows = []
        highs = []
        for system, workers in configurations:
            match = summary[
                (summary["system"] == system)
                & (summary["mode"] == mode)
                & (summary["workers"] == workers)
            ]
            if match.empty:
                raise RuntimeError(f"missing {mode} result for {system}, {workers} workers")
            row = match.iloc[0]
            values.append(float(row["median"]))
            lows.append(float(row["low"]))
            highs.append(float(row["high"]))

        values_array = np.array(values)
        error_low = values_array - np.array(lows)
        error_high = np.array(highs) - values_array
        best_index = int(np.argmin(values_array))
        bar_colors = colors.copy()
        bar_colors[best_index] = "#66bb6a"

        bars = axis.bar(
            labels,
            values,
            color=bar_colors,
            edgecolor=["#424242"] * len(labels),
            linewidth=1.2,
            yerr=[error_low, error_high],
            capsize=5,
            error_kw={"elinewidth": 1.5, "ecolor": "#424242"},
        )
        bars[best_index].set_edgecolor("#1b5e20")
        bars[best_index].set_linewidth(2.5)

        label_offset = max(highs) * 0.025
        for index, value in enumerate(values):
            axis.text(
                index,
                highs[index] + label_offset,
                f"{value:.3f}s",
                ha="center",
                va="bottom",
                fontsize=9,
                fontweight="bold" if index == best_index else "normal",
            )

        axis.set_title(
            f"{mode.capitalize()} execution"
            + (" (persistent pool)" if mode == "warm" else " (pool per query)"),
            fontweight="bold",
        )
        axis.set_ylabel("Execution time (seconds)")
        axis.grid(axis="y", alpha=0.25)
        axis.set_axisbelow(True)
        axis.text(
            best_index,
            0.97,
            "fastest",
            transform=axis.get_xaxis_transform(),
            ha="center",
            va="top",
            color="#1b5e20",
            fontweight="bold",
        )

    figure.text(
        0.5,
        0.01,
        "Bars show median time; whiskers show the minimum-to-maximum range across repetitions",
        ha="center",
        fontsize=9,
    )
    figure.tight_layout(rect=(0, 0.05, 1, 0.93))
    output_path = FIGURES / "e2-e3-bar-whiskers.png"
    figure.savefig(output_path, dpi=180)
    plt.close(figure)

    print(f"Wrote {output_path}")


if __name__ == "__main__":
    main()
