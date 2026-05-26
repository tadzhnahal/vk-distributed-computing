from pathlib import Path
import csv
import re

import matplotlib.pyplot as plt


root_dir = Path(__file__).resolve().parents[1]
benchmark_dir = root_dir / "docs" / "benchmark"

input_files = {
    "before-http": benchmark_dir / "before-http.txt",
    "after-grpc": benchmark_dir / "after-grpc.txt",
}

csv_path = benchmark_dir / "results.csv"
png_path = benchmark_dir / "benchmark.png"


def parse_custom_metric(text, name):
    match = re.search(rf"{name}:\s+([0-9.]+)", text)
    if not match:
        raise ValueError(f"Cannot parse metric: {name}")

    return float(match.group(1))


def parse_wrk_output(path):
    text = path.read_text(encoding="utf-8")

    return {
        "p50_ms": parse_custom_metric(text, "custom_p50_ms"),
        "p95_ms": parse_custom_metric(text, "custom_p95_ms"),
        "p99_ms": parse_custom_metric(text, "custom_p99_ms"),
        "rps": parse_custom_metric(text, "custom_rps"),
    }


def write_csv(results):
    with csv_path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["case", "p50_ms", "p95_ms", "p99_ms", "rps"])

        for name, values in results.items():
            writer.writerow([
                name,
                values["p50_ms"],
                values["p95_ms"],
                values["p99_ms"],
                values["rps"],
            ])


def draw_plot(results):
    metrics = ["p50_ms", "p95_ms", "p99_ms", "rps"]
    labels = ["p50, ms", "p95, ms", "p99, ms", "RPS"]

    plt.figure(figsize=(9, 5))

    for name, values in results.items():
        y = [values[metric] for metric in metrics]
        plt.plot(labels, y, marker="o", label=name)

    plt.title("HTTP proxy vs gRPC proxy")
    plt.xlabel("Metric")
    plt.ylabel("Value")
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.savefig(png_path, dpi=160)


def main():
    results = {}

    for name, path in input_files.items():
        results[name] = parse_wrk_output(path)

    write_csv(results)
    draw_plot(results)

    print(f"Saved CSV: {csv_path}")
    print(f"Saved PNG: {png_path}")


if __name__ == "__main__":
    main()
