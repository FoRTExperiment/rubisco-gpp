from csv import DictReader
from pathlib import Path

from eenirv import ModisPoint


def main():
    yr_start = 2003
    yr_end = 2019
    parent_dir = Path().resolve().parent
    site_fname = Path().joinpath(parent_dir, "data", "sites.csv")
    out_dir = Path().joinpath(parent_dir, "data", "nirv")
    Path(out_dir).mkdir(parents=True, exist_ok=True)  # ensure it exists

    with open(site_fname) as f:
        reader = DictReader(f)
        for row in reader:
            print(f"Processing {row['site_id']} -- {yr_start} to {yr_end}")
            site_df = ModisPoint(row["lat"], row["lon"], yr_start, yr_end).run()
            out_fname = Path().joinpath(out_dir, f"{row['site_id']}.csv")
            site_df.to_csv(out_fname, float_format="%.2f")


if __name__ == "__main__":
    main()
