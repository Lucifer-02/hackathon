from typing import Set, List, Tuple, Optional
import re
from dataclasses import dataclass
import unicodedata
from pprint import pprint
import logging

import polars as pl
import ahocorasick  # Import the library

import variant
import sample


@dataclass
class Area:
    code: str
    name: str
    level: str
    variants: Set[str]


@dataclass
class Ward(Area):
    ...


@dataclass
class District(Area):
    ...


@dataclass
class Province(Area):
    ...


@dataclass
class AddrMatch:
    index: int  # Index of the original address in sample.ADDR
    raw_addr: str  # The original address string
    normalized_addr: str  # The normalized address string used for matching
    area: Area
    start_idx: int  # Start index in the normalized_addr (inclusive)
    end_idx: int  # End index in the normalized_addr (exclusive)


def normalize(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        [
            (
                pl.col(col)
                .str.strip_chars()
                .str.strip_chars_start()
                .str.strip_chars_end()
                .str.to_lowercase()
                if df.schema[col] == pl.String
                else pl.col(col)
            )
            for col in df.columns
        ]
    )


def match_pattern(text: str, pattern: str, ignore_case: bool = True) -> str | None:
    # Keep match_pattern as it's used in prepare_map
    match = None
    if ignore_case:
        match = re.search(pattern, text, re.IGNORECASE)
    else:
        match = re.search(pattern, text)
    if match:
        return match.group(0)
    return None


def prepare_map() -> List[Area]:
    """
    Prepares area data from the Excel file, normalizes it,
    and generates variants. Returns a combined list of all areas.
    """
    df = pl.read_excel("./dataset/Danh sách cấp xã ___25_05_2025.xls")

    df = df.drop("Tên Tiếng Anh").rename(
        {
            "Mã": "ward code",
            "Mã QH": "district code",
            "Mã TP": "province code",
            "Cấp": "ward level",
            "Tên": "ward",
            "Tỉnh / Thành Phố": "province",
            "Quận Huyện": "district",
        }
    )

    df = normalize(df).with_columns(
        pl.col("ward level"),
        pl.struct(["province"])
        .map_elements(
            lambda x: match_pattern(x["province"], r"(^tỉnh)|(^thành phố)"),
            return_dtype=pl.String,
        )
        .alias("province level"),
        pl.struct(["district"])
        .map_elements(
            lambda x: match_pattern(
                x["district"],
                r"(^quận)|(^huyện)|(^thị xã)|(^thành phố)",
                ignore_case=True,
            ),
            return_dtype=pl.String,
        )
        .alias("district level"),
    )

    # remove prefix
    df = df.with_columns(
        pl.col("ward").str.replace(r"(^xã )|(^thị trấn )|(^phường )", "").alias("ward"),
        pl.col("district")
        .str.replace(r"(^quận )|(^huyện )|(^thị xã )|(^thành phố )", "")
        .str.strip_chars_start()
        .alias("district"),
        pl.col("province").str.replace(r"(^tỉnh )|(^thành phố )", "").alias("province"),
    )

    # Get unique areas
    wards_df = df.select(pl.col("ward", "ward level", "ward code")).unique()
    districts_df = df.select(
        pl.col("district", "district level", "district code")
    ).unique()
    provinces_df = df.select(
        pl.col("province", "province level", "province code")
    ).unique()

    # Create Area objects with variants (lowercase, accent-removed)
    wards = [
        Ward(
            code=str(row["ward code"]), # Ensure code is string
            name=str(row["ward"]),
            level=str(row["ward level"]),
            variants=variant.generate_variants(
                name=remove_accents(str(row["ward"])),
                level=remove_accents(str(row["ward level"])),
                is_shorten=False, # Keep this False for wards as in original code
            ),
        )
        for row in wards_df.iter_rows(named=True)
    ]

    districts = [
        District(
            code=str(row["district code"]), # Ensure code is string
            name=str(row["district"]),
            level=str(row["district level"]),
            variants=variant.generate_variants(
                name=remove_accents(str(row["district"])),
                level=remove_accents(str(row["district level"])),
            ),
        )
        for row in districts_df.iter_rows(named=True)
    ]

    provinces = [
        Province(
            code=str(row["province code"]), # Ensure code is string
            name=str(row["province"]),
            level=str(row["province level"]),
            variants=variant.generate_variants(
                name=remove_accents(str(row["province"])),
                level=remove_accents(str(row["province level"])),
            ),
        )
        for row in provinces_df.iter_rows(named=True)
    ]

    logging.info(f"number of ward variants: {size_areas(wards)}")
    logging.info(f"number of district variants: {size_areas(districts)}")
    logging.info(f"number of province variants: {size_areas(provinces)}")

    # Combine all areas into a single list
    all_areas: List[Area] = []
    all_areas.extend(wards)
    all_areas.extend(districts)
    all_areas.extend(provinces)

    return all_areas


def size_areas(areas: List[Area]) -> int:
    count = 0
    for area in areas:
        count += len(area.variants)
    return count


def remove_accents(input_str):
    # This function remains the same
    if not isinstance(input_str, str):
        return "" # Handle non-string input gracefully
    nfkd_form = unicodedata.normalize(
        "NFKD", input_str.replace("đ", "d").replace("Đ", "D")
    )
    return "".join([c for c in nfkd_form if not unicodedata.combining(c)])


# Removed match_word_string_multiple and match_word_string


def build_automaton(areas: List[Area]):
    """Builds an Aho-Corasick automaton from all area variants."""
    A = ahocorasick.Automaton()
    # A temporary dictionary to group areas by variant string
    variant_to_areas: dict[str, List[Area]] = {}
    for area in areas:
        for variant in area.variants:
            # Ensure variant is a non-empty string before processing
            if not isinstance(variant, str) or not variant:
                continue

            if variant not in variant_to_areas:
                variant_to_areas[variant] = []
            variant_to_areas[variant].append(area)

    # Add variants to automaton, storing variant string and list of areas as value
    for variant_string, area_list in variant_to_areas.items():
        A.add_word(variant_string, (variant_string, area_list))

    # Finalize the automaton
    A.make_automaton()
    return A


def is_whole_word_match(text: str, start_idx: int, end_idx_inclusive: int) -> bool:
    """Checks if text[start_idx:end_idx_inclusive+1] is a whole word in text."""
    # Check left boundary: character before the match must be non-word or it's the start of the string
    if start_idx > 0:
        if re.match(r"\w", text[start_idx - 1]):
            return False

    # Check right boundary: character after the match must be non-word or it's the end of the string
    if end_idx_inclusive < len(text) - 1:
        if re.match(r"\w", text[end_idx_inclusive + 1]):
            return False

    return True


def main():
    logging.basicConfig(level="INFO")

    # Prepare areas with variants (lowercase, accent-removed)
    areas = prepare_map()
    logging.info(f"number of areas: {len(areas)}")

    # Build Aho-Corasick automaton from all area variants
    logging.info("Building Aho-Corasick automaton...")
    automaton = build_automaton(areas)
    logging.info("Automaton built.")
    logging.info(f"Automaton size: {automaton.get_size()}")

    # Load and potentially normalize addresses (using sample.ADDR for now)
    # addrs_df = normalize(
    #     pl.read_excel("./dataset/Advance - Sao chép.xlsx").select("ADDR")
    # ).filter(~pl.col("ADDR").str.contains(","))
    # addrs = addrs_df.select("ADDR").to_series().to_list()
    addrs = sample.ADDR

    add_result: List[AddrMatch] = []
    from time import time

    start = time()

    # Process each address using the automaton
    for addr_index, raw_addr in enumerate(addrs):
        if not isinstance(raw_addr, str):
            logging.warning(f"Skipping non-string address at index {addr_index}: {raw_addr}")
            continue

        # Normalize the address for matching (lowercase, accent-removed)
        # This ensures consistency with area variants
        normalized_addr = remove_accents(raw_addr).lower()

        # Use the automaton to find all occurrences of variants in the normalized address
        # iter() yields (end_index_inclusive, value)
        # Our value is (matched_variant_string, list_of_areas)
        for end_index_inclusive, (matched_variant, area_list) in automaton.iter(
            normalized_addr
        ):
            # Calculate the start index
            start_index = end_index_inclusive - len(matched_variant) + 1

            # Check if the match is a whole word match in the normalized string
            if is_whole_word_match(normalized_addr, start_index, end_index_inclusive):
                # If it's a whole word match, add AddrMatch for each area associated with this variant
                for area in area_list:
                    add_result.append(
                        AddrMatch(
                            index=addr_index,
                            raw_addr=raw_addr,
                            normalized_addr=normalized_addr,  # Store the normalized address
                            area=area,
                            start_idx=start_index,  # Indices are relative to normalized_addr
                            end_idx=end_index_inclusive + 1,  # End index (exclusive)
                        )
                    )

    end = time()
    logging.info(f"Matching took {end - start:.4f} seconds")

    logging.info("Matching results:")
    # Only print the first few results and their count for large outputs
    if len(add_result) > 20:
        logging.info(f"Found {len(add_result)} matches. Showing first 20.")
        logging.info(matches_to_df(add_result[:20]))
    else:
         logging.info(f"Found {len(add_result)} matches.")
         logging.info(matches_to_df(add_result))


def matches_to_df(matches: List[AddrMatch]) -> pl.DataFrame:
    """Converts a list of AddrMatch objects into a Polars DataFrame."""
    if not matches:
        return pl.DataFrame({ # Return empty DataFrame with correct schema
             "index": pl.Int64,
             "raw_addr": pl.String,
             "normalized_addr": pl.String,
             "area_name": pl.String,
             "area_level": pl.String,
             "area_code": pl.String,
             "match_start_idx_normalized": pl.Int64,
             "match_end_idx_normalized": pl.Int64,
             "matched_string_normalized": pl.String
        })

    return pl.DataFrame(
        {
            "index": [m.index for m in matches],
            "raw_addr": [m.raw_addr for m in matches],
            "normalized_addr": [m.normalized_addr for m in matches],  # Include normalized address
            "area_name": [m.area.name for m in matches],
            "area_level": [m.area.level for m in matches],
            "area_code": [m.area.code for m in matches],
            "match_start_idx_normalized": [m.start_idx for m in matches],  # Indices relative to normalized
            "match_end_idx_normalized": [m.end_idx for m in matches],  # Indices relative to normalized
            # Add the matched substring from the normalized address for easy verification
            "matched_string_normalized": [
                m.normalized_addr[m.start_idx : m.end_idx]
                for m in matches
            ],
        }
    )


if __name__ == "__main__":
    main()

```