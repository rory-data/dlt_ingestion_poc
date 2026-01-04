import pyarrow as pa
import pyarrow.compute as pc


def apply_extract_regex_to_strings(table: pa.Table, pattern: str) -> pa.Table:
    new_table = table
    for i, field in enumerate(new_table.schema):
        # Only process plain string columns
        if field.type == pa.string():
            col = new_table.column(
                i
            )  # get column by index [[Table.column](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html#pyarrow.Table.column)]
            extracted = pc.extract_regex(
                col, pattern
            )  # apply to the column [[extract_regex](https://arrow.apache.org/docs/python/generated/pyarrow.compute.extract_regex.html)]
            # Update field type to the returned struct type and replace column in the table
            new_field = field.with_type(
                extracted.type
            )  # [[Field.with_type](https://arrow.apache.org/docs/python/generated/pyarrow.Field.html#pyarrow.Field.with_type)]
            new_table = new_table.set_column(
                i, new_field, extracted
            )  # [[Table.set_column](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html#pyarrow.Table.set_column)]
    return new_table


# Example usage
table = pa.table(
    {
        "s1": ["a1", "b2", "c3"],
        "s2": ["x9", "y8", "z7"],
        "n": [1, 2, 3],
    }
)

# Regex with named capture groups: letter and digit
pattern = r"(?P<letter>[a-zA-Z])(?P<digit>\d)"

result = apply_extract_regex_to_strings(table, pattern)
print(result)
