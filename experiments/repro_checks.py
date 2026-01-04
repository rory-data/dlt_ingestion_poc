import polars as pl
import pyarrow as pa
import pyarrow.compute as pc


def check_binary_validation():
    print("\n--- Binary Validation ---")
    # Create binary array with invalid UTF-8
    # \xFF is invalid
    # \xED\xA0\x80 is surrogate D800 (invalid in UTF-8)
    data = [b"Valid", b"Invalid \xff", b"Surrogate \xed\xa0\x80"]
    arr = pa.array(data, type=pa.binary())

    print(f"Binary array: {arr}")

    # Try to cast to string
    try:
        str_arr = pc.cast(arr, pa.string())
        print("Cast to string succeeded (unexpected).")
        print(str_arr)
    except Exception as e:
        print(f"Cast to string failed as expected: {e}")


def check_surrogates_in_arrow():
    print("\n--- Surrogates in Arrow ---")
    # Python string with surrogate
    s = "Surrogate \ud800"
    try:
        arr = pa.array([s])
        print(f"Created Arrow array with surrogate: {arr}")
    except Exception as e:
        print(f"Failed to create Arrow array with surrogate: {e}")


def check_polars_utf8():
    print("\n--- Polars UTF-8 ---")
    data = [b"Valid", b"Invalid \xff"]
    s = pl.Series("data", data)
    print(f"Polars Binary Series: {s}")

    try:
        # Cast to String
        s_str = s.cast(pl.String)
        print("Polars cast to String succeeded.")
        print(s_str)
    except Exception as e:
        print(f"Polars cast to String failed: {e}")


if __name__ == "__main__":
    check_binary_validation()
    check_surrogates_in_arrow()
    check_polars_utf8()
