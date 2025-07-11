import logging
import re
from time import time
from typing import List, Sequence, Set, Tuple

import polars as pl
import re2
from tqdm import tqdm

import inference
from model import (
    AddrMatch,
    Area,
    CombinedRawAddr,
    District,
    Province,
    RawAddr,
    SubRawAddr,
    Ward,
)
from prepare import normalize, prepare_areas


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
                        start_idx=start_idx - sub.start_idx,
                        end_idx=end_idx - sub.start_idx,
                    )
                )
                check = True

        if not check:
            logging.info(batch)
            logging.info(matches)
            logging.info(f"Index: {start_idx}, {end_idx}.")
            logging.info(f"Match substring: {batch.content[start_idx:end_idx]}")
            logging.info(area)
            logging.info("================================")

    return result


def batch_address_match_process(
    batchs: List[CombinedRawAddr], areas: Sequence[Area]
) -> List[AddrMatch]:
    results = []
    for batch in tqdm(batchs):
        for area in areas:
            # match_word_string_multiple now returns a list of (start, end) tuples
            matches = match_word_string_multiple(
                text=batch.content, words=area.variants
            )
            results.extend(extract_batch(batch=batch, matches=matches, area=area))

    return results


def batch_address_match(
    addrs: Sequence[RawAddr], batch_size: int
) -> List[CombinedRawAddr]:
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

        assert len(batch_addr.schema) <= batch_size, (
            f"{batch_addr},{len(batch_addr.schema)} not equal {batch_size}"
        )
        assert len(batch_addr.content) == batch_addr.schema[-1].end_idx + 2, (
            f"{batch_addr}\n len: {len(batch_addr.content)}"
        )
        batchs.append(batch_addr)

    assert len(addrs) == sum([len(batch.schema) for batch in batchs])

    return batchs


def address_match(addr: RawAddr, areas: Sequence[Area]) -> List[AddrMatch]:
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


def matches_to_df(matches: List[AddrMatch]) -> pl.DataFrame:
    match matches[0].area:
        case Ward():
            return pl.DataFrame(
                {
                    "index": [m.raw_addr.index for m in matches],
                    "addr": [m.raw_addr.content for m in matches],
                    "ward": [m.area.name for m in matches],
                    "ward code": [m.area.code for m in matches],
                    "start_idx": [m.start_idx for m in matches],
                    "end_idx": [m.end_idx for m in matches],
                }
            )
        case District():
            return pl.DataFrame(
                {
                    "index": [m.raw_addr.index for m in matches],
                    "addr": [m.raw_addr.content for m in matches],
                    "district": [m.area.name for m in matches],
                    "district code": [m.area.code for m in matches],
                    "start_idx": [m.start_idx for m in matches],
                    "end_idx": [m.end_idx for m in matches],
                }
            )
        case Province():
            return pl.DataFrame(
                {
                    "index": [m.raw_addr.index for m in matches],
                    "addr": [m.raw_addr.content for m in matches],
                    "province": [m.area.name for m in matches],
                    "province code": [m.area.code for m in matches],
                    "start_idx": [m.start_idx for m in matches],
                    "end_idx": [m.end_idx for m in matches],
                }
            )
        case _:
            raise


def process_address(
    addrs: List[RawAddr],
    areas: Sequence[Area],
    file_name: str,
    batch_size: int = 5000,
) -> pl.DataFrame:
    # addrs: List[RawAddr] = [RawAddr(index=0, content=addr) for addr in sample.ADDR]
    logging.info(f"number of addresses: {len(addrs)}")
    logging.info(f"number of areas: {len(areas)}")

    areas_result: List[AddrMatch] = []

    # for addr in tqdm(addrs):
    #     areas_result.extend(address_match(addr, areas))
    # pprint(address_match(addrs[49], areas))

    batchs = batch_address_match(addrs=addrs, batch_size=batch_size)
    areas_result.extend(batch_address_match_process(batchs=batchs, areas=areas))

    # # Prepare arguments for starmap: a list of tuples (addr, areas)
    # # tasks = [(addr, areas) for addr in addrs]
    # tasks = [(addr, provinces) for addr in addrs]
    # #
    # # Use multiprocessing Pool to parallelize the process_address function
    # with Pool() as pool:
    #     results = pool.starmap(process_address, tasks)
    # # Flatten the list of lists into a single list of AddrMatch objects
    # for res_list in results:
    #     province_result.extend(res_list)

    match_df = matches_to_df(areas_result)
    match_df.write_csv(f"{file_name}.csv")
    match_df.write_parquet(f"{file_name}.parquet")

    return match_df


def main():
    logging.basicConfig(level="INFO")

    start = time()
    wards, districts, provinces = prepare_areas()
    # print(provinces)
    areas = provinces
    logging.info(f"number of areas: {len(areas)}")

    sample_addrs = normalize(pl.read_excel("./dataset/Advance - Sao chép.xlsx"))
    # print(sample_addrs)
    # sample_addrs = normalize(pl.read_excel("./dataset/sample.xlsx"))
    # sample_addrs = normalize(pl.read_excel("./dataset/hackathon_result.xlsx"))

    addrs: List[RawAddr] = [
        RawAddr(index=addr["ID"], content=addr["ADDR"])
        for addr in sample_addrs.to_dicts()
    ]

    match_provinces_df = process_address(
        addrs=addrs, areas=provinces, file_name="province_match"
    )
    # print(match_provinces_df.filter(pl.col("index").eq(72)))
    match_districts_df = process_address(
        addrs=addrs, areas=districts, file_name="district_match"
    )
    # print(match_districts_df.filter(pl.col("index").eq(72)))
    match_wards_df = process_address(addrs=addrs, areas=wards, file_name="ward_match")
    # print(match_wards_df.filter(pl.col("index").eq(72)))

    official_areas = pl.read_parquet("./dataset/param_c06_distilled.parquet")

    result = inference.address_infer(
        official_areas=official_areas,
        match_wards_df=match_wards_df,
        match_districts_df=match_districts_df,
        match_provinces_df=match_provinces_df,
    )

    end = time()

    logging.info(f"Take {(end - start)}seconds")

    # print(sample_addrs)
    # print(result)
    # print(sample_addrs.join(result, left_on="ID", right_on="index", how="anti"))


if __name__ == "__main__":
    main()
