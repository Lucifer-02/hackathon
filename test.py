import polars as pl


province = pl.read_excel("./dataset/param_c06.xlsx", sheet_name="province")
district = pl.read_excel("./dataset/param_c06.xlsx", sheet_name="district")
ward = pl.read_excel("./dataset/param_c06.xlsx", sheet_name="ward")

# print(ward)
# print(district)
# print(province)
# print(pl.read_excel("./dataset/Danh sách cấp xã ___25_05_2025.xls"))

combine = ward.join(district, on="district_code").join(province, on="province_code")

add_level = (
    (
        combine.with_columns(
            pl.when(pl.col("ward_name").str.contains("(?i)phường"))
            .then(pl.lit("phường"))
            .when(pl.col("ward_name").str.contains("(?i)xã"))
            .then(pl.lit("xã"))
            .when(pl.col("ward_name").str.contains("(?i)thị trấn"))
            .then(pl.lit("thị trấn"))
            .otherwise(pl.lit(None))
            .alias("ward level")
        )
        .with_columns(
            pl.when(pl.col("district_name").str.contains("(?i)thị xã"))
            .then(pl.lit("thị xã"))
            .when(pl.col("district_name").str.contains("(?i)thành phố"))
            .then(pl.lit("thành phố"))
            .when(pl.col("district_name").str.contains("(?i)huyện"))
            .then(pl.lit("huyện"))
            .when(pl.col("district_name").str.contains("(?i)quận"))
            .then(pl.lit("quận"))
            .otherwise(pl.lit(None))
            .alias("district level"),
        )
        .with_columns(
            pl.when(pl.col("province_name").str.contains("(?i)thành phố"))
            .then(pl.lit("thành phố"))
            .when(pl.col("province_name").str.contains("(?i)tỉnh"))
            .then(pl.lit("tỉnh"))
            .otherwise(pl.lit(None))
            .alias("province level"),
        )
    )
    .rename(
        {
            "ward_code": "ward code",
            "district_code": "district code",
            "province_code": "province code",
            "province_name": "province",
            "district_name": "district",
            "ward_name": "ward",
        }
    )
    .with_columns(
        pl.col("ward")
        .str.replace(r"(?i)(^xã )|(^thị trấn )|(^phường )", "")
        .str.to_lowercase()
        .alias("ward"),
        pl.col("district")
        .str.replace(r"(?i)(^quận )|(^huyện )|(^thị xã )|(^thành phố )", "")
        .str.to_lowercase()
        .str.strip_chars_start()
        .alias("district"),
        pl.col("province")
        .str.replace(r"(?i)(^tỉnh )|(^thành phố )", "")
        .str.to_lowercase()
        .alias("province"),
    )
)

print(add_level)
add_level.write_parquet("./dataset/param_c06_distilled.parquet")
add_level.write_excel("./dataset/param_c06_distilled.xlsx")
