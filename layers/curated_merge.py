from utilities.logger import log

def write_curated(df, curated_path, full_load):
    mode = "overwrite" if full_load else "append"
    df.write.mode(mode).parquet(curated_path)
    log("info", "curated", "Curated write done", mode=mode)
