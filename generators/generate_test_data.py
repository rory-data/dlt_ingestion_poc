"""Generate test CSV data with various UTF-8 validation edge cases."""

import csv
import random

# Names with diacritics (good data)
GOOD_NAMES = [
    "José",
    "François",
    "Müller",
    "Søren",
    "Piotr",
    "Łukasz",
    "Étienne",
    "Björk",
    "Žarko",
    "Adrián",
    "Nöel",
    "Renée",
    "Škoda",
    "Ångström",
    "Çelik",
    "Øyvind",
    "Benoît",
    "Zoë",
    "Inés",
    "Andrés",
    "Chloé",
    "Günter",
    "Jürgen",
    "Māori",
    "Naïve",
    "São",
    "Tschüss",
    "Łódź",
    "Zürich",
    "Tromsø",
]

# Cities with diacritics (good data)
GOOD_CITIES = [
    "São Paulo",
    "Zürich",
    "Kraków",
    "München",
    "Göteborg",
    "Malmö",
    "Łódź",
    "Tromsø",
    "Köln",
    "Montréal",
    "Québec",
    "Bogotá",
    "Medellín",
    "Córdoba",
    "México",
    "Brasília",
    "Øresund",
    "Ålesund",
    "Bodø",
    "København",
    "Brünn",
    "Beograd",
    "Česká",
    "Łomża",
]

# Problematic characters to inject
SURROGATE_CHAR = "\ud800"  # Surrogate character (should error)
REPLACEMENT_CHAR = "\ufffd"  # Replacement character (should warn)
NONCHAR_1 = "\ufdd0"  # Non-character U+FDD0 (should warn)
NONCHAR_2 = "\ufffe"  # Non-character U+FFFE (should warn)
BINARY_CHAR = "\x01"  # Control character (should warn)
NULL_CHAR = "\x00"  # Null byte (should warn)


def generate_csv(filename: str, num_rows: int = 50_000):
    """Generate CSV with mostly good data and handful of problematic characters."""
    # Indices where we'll inject problems
    problem_rows = {
        100: ("Eve" + REPLACEMENT_CHAR, "Phoenix"),  # Replacement char
        500: ("Frank", "Phil" + NONCHAR_1 + "adelphia"),  # Non-character
        1000: ("Grace" + BINARY_CHAR + "Test", "Dallas"),  # Binary control char
        2500: ("Henry", "San " + NONCHAR_2 + "Antonio"),  # Non-character U+FFFE
        5000: ("Ivy", "San\x07Diego"),  # Bell control character
        7500: ("Jack" + NULL_CHAR + "son", "Austin"),  # Null byte
        10000: ("Karen" + REPLACEMENT_CHAR + "Smith", "Seattle"),  # Replacement
        15000: ("Leo", "Portland" + BINARY_CHAR),  # Binary at end
        20000: ("Maya" + NONCHAR_1, "Denver"),  # Non-character in name
        25000: ("Noah", "\x1bBoston"),  # ESC character at start
        30000: ("Olivia", "Chicago" + REPLACEMENT_CHAR),  # Replacement in city
        35000: ("Paul\x0c", "Miami"),  # Form feed character
        40000: ("Quinn", REPLACEMENT_CHAR + "Tampa"),  # Replacement at start
        45000: ("Ruby" + NONCHAR_2 + "Lee", "Atlanta"),  # Non-character U+FFFE
    }

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "name", "city"])

        for i in range(1, num_rows + 1):
            if i in problem_rows:
                # Use the problematic data
                name, city = problem_rows[i]
                writer.writerow([i, name, city])
            else:
                # Use good data with diacritics
                name = random.choice(GOOD_NAMES)
                city = random.choice(GOOD_CITIES)

                # Occasionally add more complex good names
                if i % 100 == 0:
                    name = f"{random.choice(GOOD_NAMES)}-{random.choice(GOOD_NAMES)}"

                writer.writerow([i, name, city])

    print(f"Generated {num_rows} rows in {filename}")
    print(f"Injected {len(problem_rows)} rows with problematic characters")
    print(f"Problem row indices: {sorted(problem_rows.keys())}")


if __name__ == "__main__":
    generate_csv("input_bad.csv", 50_000)
