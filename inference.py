import polars as pl

from prepare import standadize_areas1


def ward_district(
    official_areas: pl.DataFrame,
    wards: pl.DataFrame,
    districts: pl.DataFrame,
    factor: float = 2.0,
) -> pl.DataFrame:
    ward_district_df = wards.join(
        districts.drop("addr"), on="index", how="left", suffix="_district"
    ).rename({"start_idx": "start_idx_ward", "end_idx": "end_idx_ward"})

    # print(ward_district_df)
    ward_district_df.write_csv("ward_district_df.csv")
    # print(ward_district_df.columns)
    #
    filter1 = ward_district_df.filter(
        pl.col("end_idx_ward") < pl.col("start_idx_district")
    )

    result = (
        (
            official_areas.join(
                filter1,
                on=[
                    pl.col("ward code"),
                    pl.col("district code"),
                    pl.col("ward"),
                    pl.col("district"),
                ],
                how="left",
            )
            .with_columns(
                (
                    (
                        (
                            (
                                pl.col("end_idx_district")
                                - pl.col("start_idx_district")
                                + 1
                            )
                            + (pl.col("end_idx_ward") - pl.col("start_idx_ward") + 1)
                        )
                        * factor
                    ).alias("score")
                )
            )
            .filter(pl.col("addr").is_not_null())
        )
        .unique()
        .select(pl.col("index", "addr", "ward", "district", "province", "score"))
        .sort("index")
    )

    # print(result)
    result.write_csv("ward_district.csv", separator=";")
    return result


def ward_province(
    official_areas: pl.DataFrame,
    wards: pl.DataFrame,
    provinces: pl.DataFrame,
    factor: float = 2.0,
) -> pl.DataFrame:

    ward_district_province_df = wards.join(
        provinces.drop("addr"), on="index", how="left", suffix="_province"
    ).rename({"start_idx": "start_idx_ward", "end_idx": "end_idx_ward"})

    filter1 = ward_district_province_df.filter(
        pl.col("end_idx_ward") < pl.col("start_idx_province"),
    )

    result = (
        (
            official_areas.join(
                filter1,
                on=[
                    pl.col("ward code"),
                    pl.col("province code"),
                    pl.col("ward"),
                    pl.col("province"),
                ],
                how="left",
            )
            .filter(pl.col("start_idx_province").is_not_null())
            .with_columns(
                (
                    (
                        (
                            (
                                pl.col("end_idx_province")
                                - pl.col("start_idx_province")
                                + 1
                            )
                            + (pl.col("end_idx_ward") - pl.col("start_idx_ward") + 1)
                        )
                        * factor
                    ).alias("score")
                )
            )
        )
        .unique()
        .select(pl.col("index", "addr", "ward", "district", "province", "score"))
    )

    # print(result)
    result.write_csv("ward_province.csv", separator=";")
    return result


def ward_district_province(
    official_areas: pl.DataFrame,
    wards: pl.DataFrame,
    districts: pl.DataFrame,
    provinces: pl.DataFrame,
    factor: float = 3.0,
) -> pl.DataFrame:

    ward_district_province_df = (
        wards.join(districts.drop("addr"), on="index", how="left", suffix="_district")
        .join(provinces.drop("addr"), on="index", how="left", suffix="_province")
        .rename({"start_idx": "start_idx_ward", "end_idx": "end_idx_ward"})
    )

    filter1 = ward_district_province_df.filter(
        pl.col("end_idx_ward") < pl.col("start_idx_district"),
        pl.col("end_idx_district") < pl.col("start_idx_province"),
    )

    result = (
        (
            official_areas.join(
                filter1,
                on=[
                    pl.col("ward code"),
                    pl.col("district code"),
                    pl.col("ward"),
                    pl.col("district"),
                    pl.col("province code"),
                    pl.col("province"),
                ],
                how="left",
            ).filter(pl.col("start_idx_district").is_not_null())
            # scoring
            .with_columns(
                (
                    (
                        (
                            (
                                pl.col("end_idx_province")
                                - pl.col("start_idx_province")
                                + 1
                            )
                            + (
                                pl.col("end_idx_district")
                                - pl.col("start_idx_district")
                                + 1
                            )
                            + (pl.col("end_idx_ward") - pl.col("start_idx_ward") + 1)
                        )
                        * factor
                    ).alias("score")
                )
            )
        )
        .unique()
        .select(pl.col("index", "addr", "ward", "district", "province", "score"))
        .sort("index")
    )

    result.write_csv("ward_district_province.csv", separator=";")
    return result


def district_province(
    official_areas: pl.DataFrame,
    districts: pl.DataFrame,
    provinces: pl.DataFrame,
    factor: float = 2.0,
) -> pl.DataFrame:

    district_province_df = districts.join(
        provinces.drop("addr"), on="index", how="left", suffix="_province"
    ).rename({"start_idx": "start_idx_district", "end_idx": "end_idx_district"})

    filter1 = district_province_df.filter(
        pl.col("end_idx_district") < pl.col("start_idx_province"),
    )

    result = (
        (
            official_areas.join(
                filter1,
                on=[
                    pl.col("district code"),
                    pl.col("district"),
                    pl.col("province code"),
                    pl.col("province"),
                ],
                how="left",
            ).filter(pl.col("start_idx_province").is_not_null())
        )
        .with_columns(
            (
                (
                    (
                        (pl.col("end_idx_province") - pl.col("start_idx_province") + 1)
                        + (
                            pl.col("end_idx_district")
                            - pl.col("start_idx_district")
                            + 1
                        )
                    )
                    * factor
                ).alias("score")
            )
        )
        .unique()
        .select(pl.col("index", "addr", "ward", "district", "province", "score"))
    )

    result.write_csv("district_province.csv", separator=";")
    return result


def main():
    official_areas = pl.read_parquet("./dataset/param_c06_distilled.parquet")
    # print(official_areas)
    # official_areas.write_csv("official.csv")

    match_wards_df = pl.read_parquet("./ward_match.parquet")
    match_districts_df = pl.read_parquet("./district_match.parquet")
    match_provinces_df = pl.read_parquet("./province_match.parquet")
    # print(match_wards_df)
    # print(match_districts_df)
    # print(match_provinces_df)

    ward_district_province_df = ward_district_province(
        official_areas=official_areas,
        wards=match_wards_df,
        districts=match_districts_df,
        provinces=match_provinces_df,
        factor=3,
    )

    ward_district_df = ward_district(
        official_areas=official_areas,
        wards=match_wards_df,
        districts=match_districts_df,
        factor=2,
    )

    ward_province_df = ward_province(
        official_areas=official_areas,
        wards=match_wards_df,
        provinces=match_provinces_df,
        factor=2,
    )

    district_province_df = district_province(
        official_areas=official_areas,
        districts=match_districts_df,
        provinces=match_provinces_df,
        factor=2,
    )

    combine = pl.concat(
        [
            ward_district_province_df,
            ward_district_df,
            ward_province_df,
            district_province_df,
        ]
    )
    print(combine)
    # result_agg = combine.group_by("index", "addr").agg(pl.col("score").max())
    result_agg = combine.sort(["index", "score"], descending=[False, True]).unique(
        "index", keep="first"
    )
    print(result_agg)
    result_agg.write_csv("test.csv", separator=";")


if __name__ == "__main__":
    main()
