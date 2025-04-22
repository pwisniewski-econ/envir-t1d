import polars as pl
import gc

def import_write(file: str) -> None:
    import_path = f"data/pmsi/{file}.csv"
    export_path = f"data/interim/{file}.feather"
    df = pl.read_csv(import_path)
    df.write_ipc(export_path, compression="zstd")
    gc.collect()

files = ["act23", "ano23", "das23", "pgv23", "rdth23", "rsa23", "rum23"]

for f in files[:5]:
    import_write(f)

for f in files[5:]:
    import_write(f)