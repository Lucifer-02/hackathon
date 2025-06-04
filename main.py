from typing import Set, List, Tuple, Optional
import re
import re2
from dataclasses import dataclass
import unicodedata
from pprint import pprint
import logging
from time import time
from multiprocessing import Pool

import polars as pl
from tqdm import tqdm

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
    # re2 uses embedded flags like (?i) for case-insensitivity
    if ignore_case:
        pattern = "(?i)" + pattern

    # Use re2.search without the flags argument
    match = re2.search(pattern, text)

    if match:
        return match.group(0)

    return None


def prepare_map() -> Tuple[List[Ward], List[District], List[Province]]:
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

    return (wards, districts, provinces)


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
) -> List[Tuple[int, int]]:
    """
    Checks if any of the given 'words' exist as whole words within 'text' and
    returns a list of start and end indices for all matches found.

    A 'whole word' is defined by word boundaries (\b), meaning it's either at
    the beginning/end of the string or surrounded by non-word characters (like spaces, punctuation, etc.).

    Args:
        text (str): The input string to search within. words (List[str]): A list of words to search for.
        case_sensitive (bool): If True, the search is case-sensitive.
                                If False (default), it ignores case.

    Returns:
        List[Tuple[int, int]]: A list of tuples, where each tuple is (start_index, end_index)
                                for a found match. Returns an empty list if no matches are found.
    """
    if not words:
        return []  # No words to search for

    # Escape each word in the list to treat any special regex characters within it
    # literally. Then join them with '|' (OR) to create a single pattern.
    # Use a non-capturing group (?:...) for the alternation to ensure \b applies
    # to the entire set of words.
    escaped_words = [re.escape(word) for word in words]
    words_pattern = "|".join(escaped_words)

    # Construct the regex pattern using word boundaries '\b'.
    pattern = r"\b(?:" + words_pattern + r")\b"

    # re2 uses embedded flags like (?i) for case-insensitivity
    if not case_sensitive:
        pattern = "(?i)" + pattern

    # Use re2.finditer() to get an iterator over all matches
    matches = []
    for match_object in re2.finditer(pattern, text):
        matches.append(
            (match_object.start(), match_object.end() - 1)
        )  # -1 at end to take actual index

    return matches


@dataclass
class RawAddr:
    index: int
    content: str


@dataclass
class AddrMatch:
    raw_addr: RawAddr
    area: Area
    start_idx: int
    end_idx: int


@dataclass
class SubRawAddr:
    raw_addr: RawAddr
    start_idx: int
    end_idx: int


@dataclass
class CombinedRawAddr:
    content: str
    schema: List[SubRawAddr]


def extract_batch(
    batch: CombinedRawAddr, matches: List[Tuple[int, int]], area: Area
) -> List[AddrMatch]:

    result = []

    for start_idx, end_idx in matches:
        check = False
        for sub in batch.schema:
            if start_idx >= sub.start_idx and end_idx <= sub.end_idx:
                result.append(
                    AddrMatch(
                        raw_addr=sub.raw_addr,
                        area=area,
                        start_idx=start_idx,
                        end_idx=end_idx,
                    )
                )
                check = True

        if check != True:
            logging.info(batch)
            logging.info(matches)
            logging.info(f"Index: {start_idx}, {end_idx}.")
            logging.info(f"Match substring: {batch.content[start_idx : end_idx]}")
            logging.info(area)
            logging.info("================================")
    return result


def batch_process_address(
    addrs: List[RawAddr], areas: List[Area], batch_size: int
) -> List[AddrMatch]:

    batchs: List[CombinedRawAddr] = []

    for i in tqdm(range(0, len(addrs), batch_size)):
        batch_addr = CombinedRawAddr(content="", schema=[])
        start_idx = 0
        for addr in addrs[i : i + batch_size]:
            batch_addr.content += (
                addr.content + ";"
            )  # add ";" to avoid mis regex match with word
            batch_addr.schema.append(
                SubRawAddr(
                    raw_addr=addr,
                    start_idx=start_idx,
                    end_idx=start_idx + len(addr.content) - 1,
                )
            )

            start_idx += len(addr.content) + 1

        assert (
            len(batch_addr.schema) <= batch_size
        ), f"{batch_addr},{len(batch_addr.schema)} not equal {batch_size}"
        assert (
            len(batch_addr.content) == batch_addr.schema[-1].end_idx + 2
        ), f"{batch_addr}\n len: {len(batch_addr.content)}"
        batchs.append(batch_addr)

    assert len(addrs) == sum([len(batch.schema) for batch in batchs])

    results = []
    for batch in tqdm(batchs):
        for area in areas:
            # match_word_string_multiple now returns a list of (start, end) tuples
            matches = match_word_string_multiple(
                text=batch.content, words=area.variants
            )
            results.extend(extract_batch(batch=batch, matches=matches, area=area))

    return results


def process_address(addr: RawAddr, areas: List[Area]) -> List[AddrMatch]:
    results = []
    for area in areas:
        # match_word_string_multiple now returns a list of (start, end) tuples
        matches = match_word_string_multiple(text=addr.content, words=area.variants)
        for start_idx, end_idx in matches:
            results.append(
                AddrMatch(
                    raw_addr=addr,
                    area=area,
                    start_idx=start_idx,
                    end_idx=end_idx,
                )
            )
    return results


def main():
    logging.basicConfig(level="INFO")
    wards, districts, provinces = prepare_map()
    areas = wards
    logging.info(f"number of areas: {len(areas)}")

    sample_addrs = (
        normalize(pl.read_excel("./dataset/Advance - Sao chép.xlsx").select("ADDR"))
        .select("ADDR")
        .to_series()
        .to_list()
    )
    addrs: List[RawAddr] = [RawAddr(index=0, content=addr) for addr in sample_addrs]

    # addrs: List[RawAddr] = [RawAddr(index=0, content=addr) for addr in sample.ADDR]
    logging.info(f"number of addresses: {len(addrs)}")

    add_result: List[AddrMatch] = []

    start = time()

    # for addr in addrs:
    #     add_result.extend(process_address(addr, areas))

    add_result.extend(batch_process_address(addrs=addrs, areas=areas, batch_size=5000))

    # # Prepare arguments for starmap: a list of tuples (addr, areas)
    # # tasks = [(addr, areas) for addr in addrs]
    # tasks = [(addr, areas) for addr in addrs]
    #
    # # Use multiprocessing Pool to parallelize the process_address function
    # add_result: List[AddrMatch] = []
    # with Pool() as pool:
    #     results = pool.starmap(process_address, tasks)
    #
    # # Flatten the list of lists into a single list of AddrMatch objects
    # for res_list in results:
    #     add_result.extend(res_list)

    end = time()

    logging.info(f"Take {end - start}")

    match_df = matches_to_df(add_result)
    match_df.write_csv("match_provinces2.csv")
    logging.info(match_df)


def matches_to_df(matches: List[AddrMatch]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "index": [m.raw_addr.index for m in matches],
            "addr": [m.raw_addr.content for m in matches],
            "area": [m.area.name for m in matches],
            "start_idx": [m.start_idx for m in matches],
            "end_idx": [m.end_idx for m in matches],
        }
    )


if __name__ == "__main__":
    main()
