from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql import functions as F

# Initialize Spark session
spark = SparkSession.builder.appName("S3 to Delta Comparison").getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", "1")
spark.conf.set("spark.default.parallelism", "1")

# Load metadata (update this path to your own secure location)
metadata_path = "<your_metadata_csv_path_here>"
metadata_df = spark.read.option("header", "true").option("inferSchema", "true").csv(metadata_path)

# Track table counts
total_matched_tables = 0
total_mismatched_tables = 0
total_missing_tables = 0

# Track lists of table names
matched_tables = []
mismatched_tables = []
missing_tables = []

# Loop through tables listed in metadata
for row in metadata_df.toLocalIterator():
    source_table_name = row['source_table_name']
    raw_table_name = row['raw_table_name']
    source_path = "<source_json_path_prefix>"  # update with placeholder
    raw_path = "<target_delta_path_prefix>"    # update with placeholder
    key_column = row['key']
    row_limit = 5

    json_s3_path = f"{source_path}{source_table_name}/"
    delta_table_path = f"{raw_path}{raw_table_name}/"

    print(f"\nComparing: {source_table_name} (JSON) vs {raw_table_name} (Delta)")

    try:
        source_spark_df = spark.read.option("inferSchema", "true").json(json_s3_path)
        source_count = source_spark_df.count()
    except Exception:
        source_count = 0
        missing_tables.append(source_table_name)
        total_missing_tables += 1

    try:
        target_spark_df = spark.read.format("delta").load(delta_table_path)
        target_count = target_spark_df.count()
    except Exception:
        target_count = 0
        missing_tables.append(raw_table_name)
        total_missing_tables += 1

    if source_count == 0 or target_count == 0:
        print(f"Skipping: {source_table_name} or {raw_table_name} missing.")
        continue

    if source_count != target_count:
        print(f"Row count mismatch: {source_table_name} vs {raw_table_name}")
        mismatched_tables.append(f"{source_table_name} <-> {raw_table_name}")
        total_mismatched_tables += 1
        continue

    # Column alignment
    common_columns = [col for col in source_spark_df.columns if col in target_spark_df.columns]
    source_spark_df = source_spark_df.select(*common_columns)
    target_spark_df = target_spark_df.select(*common_columns)

    key_columns = [col.strip() for col in key_column.split(",") if col.strip() in common_columns]

    source_spark_df = source_spark_df.orderBy(*[F.col(c).asc() for c in key_columns])
    order_expr = [F.col(c).cast(source_spark_df.schema[c].dataType).asc() for c in key_columns]
    target_spark_df = target_spark_df.orderBy(*order_expr)

    try:
        source_df = source_spark_df.limit(row_limit).toPandas()
        target_df = target_spark_df.limit(row_limit).toPandas()
    except Exception:
        continue

    # Compare values
    source_data = source_df.values.tolist()
    target_data = target_df.values.tolist()

    row_mismatched = False
    diff_records = []

    for source_row, target_row in zip(source_data, target_data):
        for col_idx, (src_val, tgt_val) in enumerate(zip(source_row, target_row)):
            if (src_val is None and pd.isna(tgt_val)) or (tgt_val is None and pd.isna(src_val)):
                continue

            if str(src_val).strip() != str(tgt_val).strip():
                row_mismatched = True
                diff_records.append({
                    "Column_Name": common_columns[col_idx],
                    "Source_Value": str(src_val).strip(),
                    "Target_Value": str(tgt_val).strip(),
                    "Source_Table": source_table_name,
                    "Raw_Table": raw_table_name
                })

    if row_mismatched:
        total_mismatched_tables += 1
        mismatched_tables.append(f"{source_table_name} <-> {raw_table_name}")
        print(pd.DataFrame(diff_records).to_string(index=False))
    else:
        total_matched_tables += 1
        matched_tables.append(f"{source_table_name} <-> {raw_table_name}")

# Summary
print("\n**Comparison Summary**")
print(f"Total Matching Tables: {total_matched_tables}")
print(f"Total Mismatched Tables: {total_mismatched_tables}")
print(f"Total Missing Tables: {total_missing_tables}")
print("\nMatching Tables:", ", ".join(matched_tables) or "None")
print("\nMismatched Tables:", ", ".join(mismatched_tables) or "None")
print("\nMissing Tables:", ", ".join(missing_tables) or "None")
