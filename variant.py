from typing import Set
import itertools
import logging


def variant_names(name: str, is_shorten: bool = True) -> Set[str]:
    words = name.lower().split(" ")
    for word in words:
        assert word != " "
        assert word != "", f"{words}"

    # logging.info(f"Shorten: {is_shorten}")
    # print(f"Shorten: {is_shorten}")

    result = []
    result.append(name)

    match len(words):
        case 1:
            result.append(words[0])

        case 2:
            result.append(words[0] + words[1])
            result.append(words[0] + " " + words[1])
            if is_shorten:
                result.append(words[0][0] + words[1][0])
                result.append(words[0][0] + words[1])
                result.append(words[0][0] + words[1][:2])
                result.append(words[0][0] + "." + words[1])
                result.append(words[0][0] + "." + words[1][:2])

                if len(words[0]) > 2:
                    result.append(words[0][:2] + "." + words[1])
                    result.append(words[0][:2] + " " + words[1])

        case 3:
            result.append(words[0] + " " + words[1] + " " + words[2])
            if is_shorten:
                result.append(words[0][0] + words[1][0] + words[2][0])
        case 4:
            result.append(name)
        case 5:
            result.append(name)
        case _:
            logging.error(ValueError(name))

    return set(result)


def combine_variants(lhs: Set[str], rhs: Set[str]) -> Set[str]:
    return {"".join(pair) for pair in itertools.product(lhs, rhs)}


def variant_level(level: str) -> Set[str]:
    result = set()

    if level == "phuong":
        result.add("p.")
        result.add("p ")
        result.add("p")
        result.add("phuong ")

    if level == "xa":
        result.add("xa ")
        result.add("x.")
        result.add("x ")

    if level == "thi tran":
        result.add("thi tran ")
        result.add("tt.")
        result.add("tt ")

    if level == "quan":
        result.add("quan ")
        result.add("q.")
        result.add("q ")

    if level == "huyen":
        result.add("huyen ")
        result.add("h.")
        result.add("h ")

    if level == "thi xa":
        result.add("thi xa ")
        result.add("tx.")
        result.add("tx ")

    if level == "thanh pho":
        result.add("tp ")
        result.add("tp.")
        result.add("thanh pho ")

    if level == "tinh":
        result.add("tinh ")
        result.add("t.")
        result.add("t ")

    return set(result)


def generate_variants(name: str, level: str, is_shorten: bool = True) -> Set[str]:
    variants = combine_variants(
        variant_level(level), variant_names(name, is_shorten)
    ).union(variant_names(name, is_shorten))
    return variants


def main():
    # Example usage
    base = "2"
    level = "phuong"
    print(generate_variants(base, level, is_shorten=False))


if __name__ == "__main__":
    main()
