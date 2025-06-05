from typing import List, Tuple, Sequence
import logging
import unicodedata

import polars as pl
import re2

import variant
from model import *


def size_areas(areas: Sequence[Area]) -> int:
    count = 0
    for area in areas:
        count += len(area.variants)

    return count


def remove_accents(input_str):
    nfkd_form = unicodedata.normalize(
        "NFKD", input_str.replace("đ", "d").replace("Đ", "D")
    )
    return "".join([c for c in nfkd_form if not unicodedata.combining(c)])


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


def standadize_areas1() -> pl.DataFrame:
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

    return df


def prepare_areas() -> Tuple[List[Ward], List[District], List[Province]]:

    # print(df.select(pl.col("district level").unique()))
    # print(df.select(pl.col("district level")).to_series().to_list())
    # print(df.filter(pl.col("district level").is_null()).select(pl.col("district")))
    # print(df.select(pl.col("district")))
    df = standadize_areas1()
    # print(df)
    # df = pl.read_parquet("./dataset/param_c06_distilled.parquet")
    # print(df.filter(pl.col("province code").eq("87")).select(pl.col("ward")).to_series().to_list())
    # print(df)
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
            name=row["district"],
            level=row["district level"],
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
            name=row["province"],
            level=row["province level"],
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
