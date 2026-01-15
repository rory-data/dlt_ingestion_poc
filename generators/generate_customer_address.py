"""Generate mock customer address pipe-delimited data file for testing the data contract.

Generates test data matching the customer_address_odcs.yaml data contract.
"""

import random
from datetime import datetime, timedelta
from pathlib import Path

from faker import Faker
from loguru import logger

fake = Faker("en_NZ")


def escape_field(value: str) -> str:
    """Escape special characters in field values for pipe-delimited format.

    Escapes: backslash, pipe, newline, carriage return, tab

    Args:
        value: Field value to escape

    Returns:
        Escaped field value
    """
    if not value:
        return ""
    # Order matters: escape backslash first
    value = value.replace("\\", "\\\\")
    value = value.replace("|", "\\|")
    value = value.replace("\n", "\\n")
    value = value.replace("\r", "\\r")
    value = value.replace("\t", "\\t")
    return value


# NZ-specific constants
NZ_CITIES = [
    "Auckland",
    "Wellington",
    "Christchurch",
    "Hamilton",
    "Tauranga",
    "Dunedin",
    "Palmerston North",
    "Rotorua",
    "Napier",
    "Hastings",
    "New Plymouth",
    "Whangarei",
    "Invercargill",
    "Wairarapa",
]

NZ_POSTAL_CODES = [
    "0610",
    "1010",
    "2012",
    "3015",
    "3082",
    "2102",
    "4410",
    "6011",
    "7010",
    "8011",
]

POSTAL_ADDR_TYPES = ["Home", "Business", "Work", "Residential"]
POSTAL_ADDRESS_CATS = ["Master", "Secondary", "Delivery"]
POSTAL_ADDR_FORMATS = ["New Zealand", "Australia", "International"]
VALIDATION_SRCS = ["System", "Manual", "External"]
VALIDATION_STS = ["Validated", "Not Validated", "Pending"]


def generate_customer_address_record(
    address_id: int,
    customer_id: str,
) -> str:
    """Generate a single customer address record.

    Args:
        address_id: Unique address identifier
        customer_id: Customer ID

    Returns:
        Pipe-delimited string of address data
    """
    # Core required fields
    fields = [
        str(address_id),  # ADDRESS_ID
        customer_id,  # CUSTOMER_ID
        random.choice(POSTAL_ADDR_TYPES),  # POSTAL_ADDR_TYPE
        str(random.randint(1, 5)),  # POSTAL_ADDR_SEQ
        random.choice(["Primary", "Secondary", "Tertiary"]),  # CONTACT_SUBTYPE
        str(random.randint(1, 10)),  # CONTACT_SEQ
        (datetime.now() - timedelta(days=random.randint(0, 1825)))
        .date()
        .isoformat(),  # START_DATE
        (
            (datetime.now() + timedelta(days=random.randint(1, 365))).date().isoformat()
            if random.random() < 0.5
            else ""
        ),  # END_DATE
        random.choice(POSTAL_ADDRESS_CATS),  # POSTAL_ADDRESS_CAT
        (
            (
                datetime.now().replace(month=1, day=1)
                + timedelta(days=random.randint(0, 364))
            )
            .date()
            .isoformat()
            if random.random() < 0.3
            else ""
        ),  # PERPETUAL_START_DATE
        (
            (
                datetime.now().replace(month=1, day=1)
                + timedelta(days=random.randint(0, 364))
            )
            .date()
            .isoformat()
            if random.random() < 0.3
            else ""
        ),  # PERPETUAL_END_DATE
        random.choice(POSTAL_ADDR_FORMATS),  # POSTAL_ADDR_FORMAT_TYPE
        random.choice(["Primary", "Secondary"])
        if random.random() < 0.8
        else "",  # RECIPIENT_LINE_1_TYPE
        random.choice(["Secondary", "Tertiary"])
        if random.random() < 0.4
        else "",  # RECIPIENT_LINE_2_TYPE
        "",  # RECIPIENT_LINE_3_TYPE
        escape_field(fake.street_address())
        if random.random() < 0.9
        else "",  # RECIPIENT_LINE_1
        escape_field(fake.address().split("\n")[0])
        if random.random() < 0.5
        else "",  # RECIPIENT_LINE_2
        "",  # RECIPIENT_LINE_3
        "",  # INFORMATION_LINE_1
        "",  # INFORMATION_LINE_2
        escape_field(fake.street_address()),  # DESTINATION_LINE_1
        escape_field(fake.city())
        if random.random() < 0.7
        else "",  # DESTINATION_LINE_2
        "",  # DESTINATION_LINE_3
        "",  # LOCATION_LINE_1
        "",  # LOCATION_LINE_2
        "",  # LOCATION_LINE_3
        "",  # LOCATION_LINE_4
        random.choice(NZ_CITIES),  # CITY
        random.choice(["Auckland", "Wellington", "Canterbury", "Otago"])
        if random.random() < 0.7
        else "",  # STATE
        random.choice(NZ_POSTAL_CODES),  # POSTAL_CODE
        "",  # POSTAL_CODE_EXTN
        "",  # DELIVERY_POINT_BARCODE
        "NZL",  # COUNTRY
        "",  # PROVINCE
        "",  # REGION
        "",  # MUNICIPALITY
        "",  # DISTRICT
        "",  # VILLAGE
        "",  # PARISH
        "",  # NEIGHBOURHOOD
        "",  # COUNTY
        random.choice(VALIDATION_SRCS)
        if random.random() < 0.7
        else "",  # VALIDATION_SRC
        (
            (datetime.now() - timedelta(days=random.randint(0, 365))).date().isoformat()
            if random.random() < 0.7
            else ""
        ),  # VALIDATION_DATE
        (
            (datetime.now() - timedelta(days=random.randint(0, 365))).date().isoformat()
            if random.random() < 0.5
            else ""
        ),  # OCCUPANCY_DATE
        (
            (datetime.now() - timedelta(days=random.randint(0, 365))).date().isoformat()
            if random.random() < 0.5
            else ""
        ),  # PURCHASE_DATE
        str(random.choice([True, False])),  # ELEMENTISATION_OVERRIDE
        str(random.choice([True, False])),  # STANDARDISATION_OVERRIDE
        str(random.choice([True, False])),  # VALIDATION_OVERRIDE
        str(random.choice([True, False])),  # FORMAT_OVERRIDE
        (
            (datetime.now() - timedelta(days=random.randint(0, 365))).date().isoformat()
            if random.random() < 0.2
            else ""
        ),  # OCCUPANCY_END_DATE
        "",  # MAIL_CODE
        escape_field(fake.text(max_nb_chars=100))
        if random.random() < 0.2
        else "",  # SPECIAL_INSTRUCTIONS
        (
            (datetime.now() - timedelta(days=random.randint(0, 365))).date().isoformat()
            if random.random() < 0.3
            else ""
        ),  # LEGAL_ADDR_START_DATE
        str(random.choice([True, False])),  # PO_BOX_IND
        str(random.choice([True, False])),  # MILITARY_ADDR_IND
        "Pacific/Auckland" if random.random() < 0.95 else "",  # TIMEZONE
        datetime.now().isoformat(timespec="microseconds"),  # HOST_TIMESTAMP
        datetime.now().isoformat(timespec="microseconds"),  # OPERATIONAL_TIMESTAMP
        datetime.now().isoformat(timespec="microseconds"),  # ACCEPTED_TIMESTAMP
        "+13:00",  # ACCEPTED_TIMESTAMP_UTC_OFFSET
        "BackOffice",  # WORKSTATION_ID
        f"E{random.randint(1000000, 9999999)}",  # USER_ID
        random.choice(["", "Firm A", "Firm B"]),  # FIRM
        escape_field(fake.building_number())
        if random.random() < 0.8
        else "",  # HOUSE_NUMBER
        "",  # PRE_DIRECTION
        escape_field(fake.street_name())
        if random.random() < 0.8
        else "",  # STREET_NAME
        random.choice(["Street", "Avenue", "Road", "Lane", "Drive"])
        if random.random() < 0.8
        else "",  # STREET_TYPE
        "",  # POST_DIRECTION
        "",  # BUILDING_NUMBER
        "",  # BUILDING_NAME
        "",  # BOX_NUMBER
        "",  # BOX_TYPE
        "",  # RURAL_ROUTE
        "",  # RURAL_ROUTE_TYPE
        "",  # PMB_NUMBER
        "",  # PMB_DESIGNATOR
        "",  # SUBADDRESS
        "",  # SUBADDRESS_TYPE
        random.choice(NZ_CITIES) if random.random() < 0.6 else "",  # LOCALITY
        "",  # VANITY_PRE_DIRECTION
        "",  # VANITY_STREET_NAME
        "",  # VANITY_STREET_TYPE
        "",  # VANITY_POST_DIRECTION
        "",  # VANITY_CITY
        random.choice(VALIDATION_STS),  # VALIDATION_STS
        "",  # VALIDATION_DETAIL
        random.choice(["Insert", "Update", "Delete", ""])
        if random.random() < 0.8
        else "",  # ACTION
    ]

    return "|".join(fields)


def generate_customer_address_file(
    output_path: str, num_records: int = 1000, num_customers: int = 100
):
    """Generate customer address data file.

    Args:
        output_path: Path to output TXT file
        num_records: Number of address records to generate
        num_customers: Number of unique customer IDs to cycle through
    """
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    # Generate header
    header = (
        "ADDRESS_ID|CUSTOMER_ID|POSTAL_ADDR_TYPE|POSTAL_ADDR_SEQ|"
        "CONTACT_SUBTYPE|CONTACT_SEQ|START_DATE|END_DATE|POSTAL_ADDRESS_CAT|"
        "PERPETUAL_START_DATE|PERPETUAL_END_DATE|POSTAL_ADDR_FORMAT_TYPE|"
        "RECIPIENT_LINE_1_TYPE|RECIPIENT_LINE_2_TYPE|RECIPIENT_LINE_3_TYPE|"
        "RECIPIENT_LINE_1|RECIPIENT_LINE_2|RECIPIENT_LINE_3|"
        "INFORMATION_LINE_1|INFORMATION_LINE_2|"
        "DESTINATION_LINE_1|DESTINATION_LINE_2|DESTINATION_LINE_3|"
        "LOCATION_LINE_1|LOCATION_LINE_2|LOCATION_LINE_3|LOCATION_LINE_4|"
        "CITY|STATE|POSTAL_CODE|POSTAL_CODE_EXTN|DELIVERY_POINT_BARCODE|"
        "COUNTRY|PROVINCE|REGION|MUNICIPALITY|DISTRICT|VILLAGE|PARISH|"
        "NEIGHBOURHOOD|COUNTY|VALIDATION_SRC|VALIDATION_DATE|OCCUPANCY_DATE|"
        "PURCHASE_DATE|ELEMENTISATION_OVERRIDE|STANDARDISATION_OVERRIDE|"
        "VALIDATION_OVERRIDE|FORMAT_OVERRIDE|OCCUPANCY_END_DATE|MAIL_CODE|"
        "SPECIAL_INSTRUCTIONS|LEGAL_ADDR_START_DATE|PO_BOX_IND|"
        "MILITARY_ADDR_IND|TIMEZONE|HOST_TIMESTAMP|OPERATIONAL_TIMESTAMP|"
        "ACCEPTED_TIMESTAMP|ACCEPTED_TIMESTAMP_UTC_OFFSET|WORKSTATION_ID|"
        "USER_ID|FIRM|HOUSE_NUMBER|PRE_DIRECTION|STREET_NAME|STREET_TYPE|"
        "POST_DIRECTION|BUILDING_NUMBER|BUILDING_NAME|BOX_NUMBER|BOX_TYPE|"
        "RURAL_ROUTE|RURAL_ROUTE_TYPE|PMB_NUMBER|PMB_DESIGNATOR|SUBADDRESS|"
        "SUBADDRESS_TYPE|LOCALITY|VANITY_PRE_DIRECTION|VANITY_STREET_NAME|"
        "VANITY_STREET_TYPE|VANITY_POST_DIRECTION|VANITY_CITY|"
        "VALIDATION_STS|VALIDATION_DETAIL|ACTION"
    )

    logger.info(f"Generating {num_records} customer address records to {output_path}")

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(header + "\n")

        # Generate customer IDs
        customer_ids = [f"{90000000 + i}" for i in range(num_customers)]

        for i in range(num_records):
            address_id = 6150 + i
            customer_id = random.choice(customer_ids)
            record = generate_customer_address_record(address_id, customer_id)
            f.write(record + "\n")

            if (i + 1) % 10000 == 0:
                logger.info(f"Generated {i + 1} records...")

    logger.info(f"Successfully generated {num_records} records to {output_path}")


if __name__ == "__main__":
    output_path = (
        Path(__file__).parent.parent / "data" / "input" / "customer_address.txt"
    )
    generate_customer_address_file(output_path, num_records=1000, num_customers=100)
