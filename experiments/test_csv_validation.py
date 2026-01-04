import pyarrow as pa
import pyarrow.csv as csv

# Test with check_utf8=True
options = csv.ConvertOptions(check_utf8=True)

try:
    table = csv.read_csv(
        "input_bad.csv",
        read_options=csv.ReadOptions(encoding="utf-8"),
        convert_options=options,
    )
    print("SUCCESS: Table loaded despite invalid UTF-8")
    print(table)
    print(f"Value with bad byte: {table['name'][4].as_py()!r}")
except pa.ArrowInvalid as e:
    print(f"CAUGHT: {e}")
