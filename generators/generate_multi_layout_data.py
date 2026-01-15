"""Generate mock multi-layout pipe-delimited data file with 100k records."""

import random
import string
from datetime import UTC, datetime, timezone
from pathlib import Path

from faker import Faker
from loguru import logger

fake = Faker("en_NZ")


def generate_random_string(max_length: int, optional: bool = True) -> str:
    """Generate random string data.

    Args:
        max_length: Maximum length of the string
        optional: If True, 30% chance of returning empty string
    """
    if optional and random.random() < 0.3:
        return ""

    # Generate string between 1 and max_length
    length = random.randint(1, max_length)
    chars = string.ascii_letters + string.digits + " "
    return "".join(random.choices(chars, k=length))


def generate_record_9001(account_id: str) -> str:
    """Generate record type 9001 (Agreement) with 96 columns."""
    if not account_id:
        raise ValueError("PK field 'account_id' cannot be null")

    record_type = "9001"
    fields = [
        account_id,
        fake.bs(),
        fake.catch_phrase(),
        fake.date_this_century().isoformat(),
        str(fake.boolean()),
    ]

    for i in range(96 - len(fields)):
        if i < 8:
            fields.append(generate_random_string(255))
        else:
            max_len = random.choice([10, 20, 50, 100])
            fields.append(generate_random_string(max_len))

    return f"{record_type}|{'|'.join(fields)}"


def generate_record_9002(account_id: str) -> str:
    """Generate record type 9002 (Account Master) with 60 columns."""
    if not account_id:
        raise ValueError("PK field 'account_id' cannot be null")

    record_type = "9002"
    fields = [
        account_id,
        fake.aba(),
        fake.currency_code(),
        str(fake.random_int(min=1000, max=1000000)),
        fake.date_this_decade().isoformat(),
        fake.company(),
    ]

    for i in range(60 - len(fields)):
        if i < 5:
            fields.append(generate_random_string(255))
        else:
            max_len = random.choice([10, 30, 50])
            fields.append(generate_random_string(max_len))

    return f"{record_type}|{'|'.join(fields)}"


def generate_record_9004(account_id: str) -> str:
    """Generate record type 9004 with 36 columns."""
    if not account_id:
        raise ValueError("PK field 'account_id' cannot be null")

    record_type = "9004"
    fields = [account_id, fake.credit_card_number(), fake.credit_card_provider()]

    for i in range(36 - len(fields)):
        if i < 3:
            fields.append(generate_random_string(255))
        else:
            max_len = random.choice([10, 25, 50])
            fields.append(generate_random_string(max_len))

    return f"{record_type}|{'|'.join(fields)}"


def generate_record_9005(account_id: str) -> str:
    """Generate record type 9005 with 16 columns."""
    if not account_id:
        raise ValueError("PK field 'account_id' cannot be null")

    record_type = "9005"
    fields = [account_id, fake.swift()]

    for i in range(16 - len(fields)):
        if i < 2:
            fields.append(generate_random_string(255))
        else:
            max_len = random.choice([10, 20, 40])
            fields.append(generate_random_string(max_len))

    return f"{record_type}|{'|'.join(fields)}"


def generate_record_9006(customer_id: str, account_id: str) -> str:
    """Generate record type 9006 (Customer-Account Bridge) with 21 columns."""
    if not customer_id:
        raise ValueError("PK field 'customer_id' cannot be null")
    if not account_id:
        raise ValueError("PK field 'account_id' cannot be null")

    record_type = "9006"
    fields = [customer_id, account_id, fake.job()]

    for i in range(21 - len(fields)):
        if i < 2:
            fields.append(generate_random_string(255))
        else:
            max_len = random.choice([10, 20, 30])
            fields.append(generate_random_string(max_len))

    return f"{record_type}|{'|'.join(fields)}"


def generate_record_9009(account_id: str) -> str:
    """Generate record type 9009 with 34 columns."""
    if not account_id:
        raise ValueError("PK field 'account_id' cannot be null")

    record_type = "9009"
    fields = [account_id, fake.phone_number()]

    for i in range(34 - len(fields)):
        if i < 3:
            fields.append(generate_random_string(255))
        else:
            max_len = random.choice([10, 25, 50])
            fields.append(generate_random_string(max_len))

    return f"{record_type}|{'|'.join(fields)}"


def generate_record_9012(account_id: str) -> str:
    """Generate record type 9012 with 37 columns."""
    if not account_id:
        raise ValueError("PK field 'account_id' cannot be null")

    record_type = "9012"
    fields = [account_id, fake.file_name(extension="pdf")]

    for i in range(37 - len(fields)):
        if i < 3:
            fields.append(generate_random_string(255))
        else:
            max_len = random.choice([10, 25, 50])
            fields.append(generate_random_string(max_len))

    return f"{record_type}|{'|'.join(fields)}"


def generate_record_9019(account_id: str) -> str:
    """Generate record type 9019 with 27 columns."""
    if not account_id:
        raise ValueError("PK field 'account_id' cannot be null")

    record_type = "9019"
    fields = [account_id, fake.city(), fake.country()]

    for i in range(27 - len(fields)):
        if i < 3:
            fields.append(generate_random_string(255))
        else:
            max_len = random.choice([10, 25, 50])
            fields.append(generate_random_string(max_len))

    return f"{record_type}|{'|'.join(fields)}"


def generate_record_9020(account_id: str) -> str:
    """Generate record type 9020 with 21 columns."""
    if not account_id:
        raise ValueError("PK field 'account_id' cannot be null")

    record_type = "9020"
    fields = [account_id, fake.color_name()]

    for i in range(21 - len(fields)):
        if i < 2:
            fields.append(generate_random_string(255))
        else:
            max_len = random.choice([10, 20, 30])
            fields.append(generate_random_string(max_len))

    return f"{record_type}|{'|'.join(fields)}"


def generate_record_9031(account_id: str) -> str:
    """Generate record type 9031 with 39 columns."""
    if not account_id:
        raise ValueError("PK field 'account_id' cannot be null")

    record_type = "9031"
    fields = [account_id, fake.email()]

    for i in range(39 - len(fields)):
        if i < 3:
            fields.append(generate_random_string(255))
        else:
            max_len = random.choice([10, 25, 50, 75])
            fields.append(generate_random_string(max_len))

    return f"{record_type}|{'|'.join(fields)}"


def generate_multi_layout_file(
    output_path: str = "multi_layout_data.txt", master_9002_count: int = 500_000
) -> None:
    """Generate multi-layout data file with proportional record counts.

    Args:
        output_path: Path to output file
        master_9002_count: Number of 9002 records (master count that others scale from)
    """
    # Define proportions relative to 9002 (based on 500k reference)
    PROPORTIONS = {
        "9001": 1.0,  # 500k / 500k
        "9002": 1.0,  # master
        "9004": 0.094,  # 47k / 500k
        "9005": 0.276,  # 138k / 500k
        "9006": 1.29,  # 645k / 500k
        "9009": 0.04,  # 20k / 500k
        "9012": 0.118,  # 59k / 500k
        "9019": 1.0,  # 500k / 500k
        "9020": 0.126,  # 63k / 500k
        "9031": 0.314,  # 157k / 500k
    }

    # Calculate target counts for each record type
    target_counts = {
        rt: int(master_9002_count * proportion)
        for rt, proportion in PROPORTIONS.items()
    }

    # Generate IDs
    account_ids = [f"{i:010d}" for i in range(1, master_9002_count + 1)]
    # Assume 80% customer-to-account ratio for realistic bridge data
    num_customers = max(1, int(master_9002_count * 0.8))
    customer_ids = [f"{i:010d}" for i in range(1, num_customers + 1)]

    # Build list of records to generate with their assigned IDs
    records_to_generate = []

    # 1. Account Masters (9002) and Agreements (9001) - 1:1 mapping
    for aid in account_ids:
        records_to_generate.append(("9002", (aid,)))
        records_to_generate.append(("9001", (aid,)))

    # 2. Customer-Account Bridge (9006)
    for _ in range(target_counts["9006"]):
        cid = random.choice(customer_ids)
        aid = random.choice(account_ids)
        records_to_generate.append(("9006", (cid, aid)))

    # 3. All other relational record types
    relational_rts = ["9004", "9005", "9009", "9012", "9019", "9020", "9031"]
    for rt in relational_rts:
        for _ in range(target_counts[rt]):
            aid = random.choice(account_ids)
            records_to_generate.append((rt, (aid,)))

    total_records = len(records_to_generate)
    logger.info(
        "Starting data generation: {:,} total records (master 9002 count: {:,})",
        total_records,
        master_9002_count,
    )

    random.shuffle(records_to_generate)

    # Record type generators mapping
    generators = {
        "9001": generate_record_9001,
        "9002": generate_record_9002,
        "9004": generate_record_9004,
        "9005": generate_record_9005,
        "9006": generate_record_9006,
        "9009": generate_record_9009,
        "9012": generate_record_9012,
        "9019": generate_record_9019,
        "9020": generate_record_9020,
        "9031": generate_record_9031,
    }

    # Track actual counts for trailer
    record_counts = dict.fromkeys(generators.keys(), 0)

    # Get file info for header
    filename = Path(output_path).name
    # ISO 8601 format for UTC
    created_datetime = datetime.now(UTC).isoformat(timespec="seconds")

    with open(output_path, "w", encoding="utf-8") as f:
        # Write header record
        header = f"H|{filename}|{created_datetime}"
        f.write(header + "\n")

        # Generate data records
        for i, (record_type, id_args) in enumerate(records_to_generate, 1):
            generator = generators[record_type]
            record = generator(*id_args)
            f.write(record + "\n")
            record_counts[record_type] += 1

            # Progress indicator
            if i % 50_000 == 0:
                progress_pct = round((i / total_records) * 100, 1)
                logger.debug(
                    "Progress update: {:,} records generated ({}%)", i, progress_pct
                )

        # Write trailer record
        trailer_parts = [
            f"{rt}-{count:010d}" for rt, count in sorted(record_counts.items())
        ]
        trailer = "T|" + "|".join(trailer_parts)
        f.write(trailer + "\n")

    logger.success(
        "File generation complete: {} ({:,} records)", output_path, total_records
    )

    logger.info("Record type breakdown:")
    for record_type, count in sorted(record_counts.items()):
        percentage = (count / total_records) * 100
        logger.info("  {}: {:,} ({:.1f}%)", record_type, count, percentage)

    # File size
    file_size = Path(output_path).stat().st_size
    file_size_mb = file_size / (1024 * 1024)
    file_size_gb = file_size / (1024**3)
    logger.info("File size: {:.2f} MB ({:.2f} GB)", file_size_mb, file_size_gb)


if __name__ == "__main__":
    # Generate the file
    # Adjust master_9002_count to scale all other record types proportionally
    generate_multi_layout_file(
        output_path="data/input/multi_layout_data_prd.txt",
        master_9002_count=200,  # Change this to scale all record types
    )
