from typing import Set, List, Tuple, Optional
import re
from dataclasses import dataclass
import unicodedata
from pprint import pprint
import logging
from time import time

import polars as pl

import variant
import sample


@dataclass
class Area:
    code: str
    name: str
    level: str
    variants: Set[str]


@dataclass
class Ward(Area): ...


@dataclass
class District(Area): ...


@dataclass
class Province(Area): ...


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
    match = None

    if ignore_case:
        match = re.search(pattern, text, re.IGNORECASE)
    else:
        match = re.search(pattern, text)

    if match:
        return match.group(0)

    return None


def prepare_map():
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
    # df = df.filter(pl.col("province").str.contains("Đồng Nai"))

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

    # print(df.select(pl.col("district level").unique()))
    # print(df.select(pl.col("district level")).to_series().to_list())
    # print(df.filter(pl.col("district level").is_null()).select(pl.col("district")))
    # print(df.select(pl.col("district")))
    provinces_df = df.select(
        pl.col("province", "province level", "province code")
    ).unique()
    districts_df = df.select(
        pl.col("district", "district level", "district code")
    ).unique()
    wards_df = df.select(pl.col("ward", "ward level", "ward code")).unique()
    # print(districts_df.filter(pl.col("district").eq("10")))

    # print(wards_df.select(pl.col("ward level")).unique())
    # print(districts_df.select(pl.col("district level")).unique())
    # print(provinces_df.select(pl.col("province level")).unique())

    wards = [
        Ward(
            code=row["ward code"],
            name=row["ward"],
            level=row["ward level"],
            variants=variant.generate_variants(
                name=remove_accents(row["ward"]),
                level=remove_accents(row["ward level"]),
                is_shorten=False,
            ),
        )
        for row in wards_df.iter_rows(named=True)
    ]

    districts = [
        District(
            code=row["district code"],
            name=remove_accents(row["district"]),
            level=remove_accents(row["district level"]),
            variants=variant.generate_variants(
                name=remove_accents(row["district"]),
                level=remove_accents(row["district level"]),
            ),
        )
        for row in districts_df.iter_rows(named=True)
    ]
    # print(districts)

    provinces = [
        Province(
            code=row["province code"],
            name=remove_accents(row["province"]),
            level=remove_accents(row["province level"]),
            variants=variant.generate_variants(
                name=remove_accents(row["province"]),
                level=remove_accents(row["province level"]),
            ),
        )
        for row in provinces_df.iter_rows(named=True)
    ]
    logging.info(f"number of ward variants: {size_areas(wards)}")
    logging.info(f"number of district variants: {size_areas(districts)}")
    logging.info(f"number of province variants: {size_areas(provinces)}")
    return provinces
    # return districts
    # return wards


def size_areas(areas: List[Area]) -> int:
    count = 0
    for area in areas:
        count += len(area.variants)

    return count


def remove_accents(input_str):
    nfkd_form = unicodedata.normalize(
        "NFKD", input_str.replace("đ", "d").replace("Đ", "D")
    )
    return "".join([c for c in nfkd_form if not unicodedata.combining(c)])


def match_word_string_multiple(
    text: str, words: Set[str], case_sensitive: bool = False
) -> Tuple[bool, Optional[int], Optional[int]]:
    """
    Checks if any of the given 'words' exist as whole words within 'text' and
    returns the start and end indices of the first match found.

    A 'whole word' is defined by word boundaries (\b), meaning it's either at
    the beginning/end of the string or surrounded by non-word characters (like spaces, punctuation, etc.).

    Args:
        text (str): The input string to search within. words (List[str]): A list of words to search for.
        case_sensitive (bool): If True, the search is case-sensitive.
                                If False (default), it ignores case.

    Returns:
        Tuple[bool, Optional[int], Optional[int]]:
            A tuple containing:
            - bool: True if any of the words are found, False otherwise.
            - Optional[int]: The start index of the first match if found, else None.
            - Optional[int]: The end index of the first match if found, else None.
    """
    if not words:
        return False, None, None  # No words to search for

    # Escape each word in the list to treat any special regex characters within it
    # literally. Then join them with '|' (OR) to create a single pattern.
    # Use a non-capturing group (?:...) for the alternation to ensure \b applies
    # to the entire set of words.
    escaped_words = [re.escape(word) for word in words]
    words_pattern = "|".join(escaped_words)

    # Construct the regex pattern using word boundaries '\b'.
    # '\b' matches the position between a word character (\w) and a non-word
    # character (\W), or at the start/end of the string.
    pattern = r"\b(?:" + words_pattern + r")\b"

    # Set regex flags. By default, we ignore case.
    flags = 0
    if not case_sensitive:
        flags |= re.IGNORECASE  # Add the IGNORECASE flag

    # Use re.search() to find the pattern anywhere in the text.
    # If a match is found, re.search returns a match object. Otherwise, it returns None.
    match_object = re.search(pattern, text, flags)

    if match_object:
        # If a match is found, return True and its start/end indices
        return True, match_object.start(), match_object.end()
    else:
        # If no match is found, return False and None for indices
        return False, None, None


def match_word_string(
    text: str, word: str, case_sensitive: bool = False
) -> Tuple[bool, Optional[int], Optional[int]]:
    # Escape the 'word' string to treat any special regex characters within it
    # literally. For example, if 'word' is "a.b", re.escape makes it "a\.b".
    escaped_word = re.escape(word)

    # Construct the regex pattern using word boundaries '\b'.
    # '\b' matches the position between a word character (\w) and a non-word
    # character (\W), or at the start/end of the string.
    pattern = r"\b" + escaped_word + r"\b"

    # Set regex flags. By default, we ignore case.
    flags = 0
    if not case_sensitive:
        flags |= re.IGNORECASE  # Add the IGNORECASE flag

    match_object = re.search(pattern, text, flags)

    if match_object:
        # If a match is found, return True and its start/end indices
        return True, match_object.start(), match_object.end()
    else:
        # If no match is found, return False and None for indices
        return False, None, None


@dataclass
class AddrMatch:
    index: int
    raw_addr: str
    area: Area
    start_idx: int
    end_idx: int


def main():
    logging.basicConfig(level="INFO")
    areas = prepare_map()
    logging.info(f"number of areas: {len(areas)}")

    # addrs_df = normalize(
    #     pl.read_excel("./dataset/Advance - Sao chép.xlsx").select("ADDR")
    # ).filter(~pl.col("ADDR").str.contains(","))
    # print(len(addrs_df))
    # print(addrs_df)

    addrs = sample.ADDR
    logging.info(f"number of addresses: {len(addrs)}")

    add_result: List[AddrMatch] = []
    start = time()
    for addr in addrs:
        for area in areas:
            is_match, start_idx, end_idx = match_word_string_multiple(
                text=addr, words=area.variants
            )
            if is_match and start_idx is not None and end_idx is not None:
                add_result.append(
                    AddrMatch(
                        index=0,
                        raw_addr=addr,
                        area=area,
                        start_idx=start_idx,
                        end_idx=end_idx,
                    )
                )
            # for variant in area.variants:
            #     if match_word_string(text=addr, word=variant):
            #         add_result.append(f"{addr}|{variant}|{area.name}")
        # if len(add_result) == 0:
        #     pprint(f"Not found {addr}")
        # if len(add_result) >= 5:
        #     pprint(add_result)
    end = time()
    logging.info(f"Take {end - start}")

    match_df = matches_to_df(add_result)
    match_df.write_csv("match_provinces.csv")
    logging.info(match_df)


def matches_to_df(matches: List[AddrMatch]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "index": [m.index for m in matches],
            "raw_addr": [m.raw_addr for m in matches],
            "area": [m.area.name for m in matches],
            "start_idx": [m.start_idx for m in matches],
            "end_idx": [m.end_idx for m in matches],
        }
    )


if __name__ == "__main__":
    main()
