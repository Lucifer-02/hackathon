from typing import Set


def find_matching_substrings(
    text_string: str, word_count_limit: int, pattern: str
) -> list[str]:
    """
    Finds substrings in a given text that meet specific criteria related to word count and pattern matching.

    A substring is considered a match if it satisfies:
    1. Its word count is less than or equal to the word_count_limit.
    2. All characters in the substring (ignoring spaces) are present in the pattern (ignoring spaces).
    3. The relative order of characters in the substring (ignoring spaces) is preserved
       as they appear in the pattern (ignoring spaces) (i.e., it's a subsequence).
    4. **Refined Pattern Matching Condition:**
        a. The cleaned substring is an exact character-for-character match of the cleaned pattern.
        OR
        b. The cleaned substring is a proper subsequence of the cleaned pattern, AND the characters
           in the substring originate from at least two different words of the original pattern string.

    Args:
        text_string: The input string to search within.
        word_count_limit: The maximum number of words allowed in a matching substring.
        pattern: The pattern string to match characters and their relative order.

    Returns:
        A list of unique substrings that satisfy all conditions, sorted alphabetically.
    """

    # --- Preprocessing the pattern to understand character origin ---
    pattern_words = pattern.split()

    # cleaned_pattern_builder: Used to build the final cleaned_pattern string.
    # pattern_char_to_orig_word_idx_map: Stores the original word index for each character
    #                                     in the cleaned_pattern string.
    cleaned_pattern_builder = []
    pattern_char_to_orig_word_idx_map = []

    for word_idx, word in enumerate(pattern_words):
        for char in word:
            cleaned_pattern_builder.append(char)
            pattern_char_to_orig_word_idx_map.append(word_idx)

    cleaned_pattern = "".join(cleaned_pattern_builder)

    if not cleaned_pattern:
        return []  # An empty pattern after cleaning cannot match anything meaningfully.

    # pattern_chars_set: Set of characters from cleaned_pattern for efficient O(1) existence checks.
    pattern_chars_set = set(cleaned_pattern)

    # --- Main Substring Search Logic ---
    matching_substrings_set = set()  # Use a set to store unique matches
    words_in_text = text_string.split()
    num_words_in_text = len(words_in_text)

    # Iterate through all possible contiguous sequences of words to form substrings.
    for i in range(num_words_in_text):
        for j in range(i, num_words_in_text):
            current_substring_words = words_in_text[i : j + 1]
            current_word_count = len(current_substring_words)

            # Condition 1: Check word count limit.
            if current_word_count > word_count_limit:
                continue

            current_substring = " ".join(current_substring_words)
            cleaned_substring = current_substring.replace(" ", "")

            if not cleaned_substring:  # Skip empty cleaned substrings
                continue

            # --- Pattern Matching Criteria Checks (Conditions 2 & 3) ---

            # Condition 2: All characters in cleaned_substring must be present in pattern_chars_set.
            is_char_subset_of_pattern = True
            for char_sub in cleaned_substring:
                if char_sub not in pattern_chars_set:
                    is_char_subset_of_pattern = False
                    break
            if not is_char_subset_of_pattern:
                continue

            # Condition 3 & 4 (part b): Check order preservation (subsequence)
            # and track original pattern word sources.
            is_order_preserved = True
            last_found_index_in_pattern = (
                -1
            )  # Tracks the last matched character's index in cleaned_pattern.
            seen_pattern_word_indices = (
                set()
            )  # Stores unique original word indices from the pattern.

            for char_sub in cleaned_substring:
                # Find char_sub in cleaned_pattern starting from the position *after* last_found_index_in_pattern.
                # This ensures relative order is maintained.
                found_index = cleaned_pattern.find(
                    char_sub, last_found_index_in_pattern + 1
                )

                if (
                    found_index == -1
                ):  # Character not found or not in correct relative order.
                    is_order_preserved = False
                    break

                last_found_index_in_pattern = found_index

                # Retrieve the original word index from the preprocessed map for this character.
                original_pattern_word_idx = pattern_char_to_orig_word_idx_map[
                    found_index
                ]
                seen_pattern_word_indices.add(original_pattern_word_idx)

            if not is_order_preserved:
                continue  # Skip if the relative order of characters is not preserved.

            # --- Apply Refined Pattern Matching Condition (Condition 4) ---
            # Condition 4a: Exact match of cleaned strings
            if cleaned_substring == cleaned_pattern:
                matching_substrings_set.add(current_substring)
            # Condition 4b: Proper subsequence AND characters come from >= 2 original pattern words
            elif len(seen_pattern_word_indices) >= 2:
                matching_substrings_set.add(current_substring)

    # Convert the set of unique matches to a list and sort it for a consistent output.
    return sorted(list(matching_substrings_set))


def main():
    addrs = [
        "SO 9,NGO 12,NGACH 85 DAO TAN,BD,HN",
        "SO 9,NGO 402 NGO GIA TU, DUC GIANG,LBIEN",
        "SO 9 NGO 402 NGO GIA TU  DUC GIANG LBIEN",
        "SO 91 PHO CHUA LANG HN",
        "SO 91,NGO 73,NGUYEN LUONG BANG,DONG DA",
        "SO 92 NGO 1137 DE LA THANH HN",
        "SO 93 LO 26D LE HONG PHONG DONG KHE HP",
        "so 95 to 4 p.mai dong hbt hn",
        "SO 953 D. HONG HA CHUONG DUONG HN",
        "SO 96 PHO HANG BAC HN",
        "SO 97 NGO 180 NGUYEN LUONG BANG",
        "SO 98 NGO YEN NINH HN",
        "SO 99 NGO 161A DUONG NUOC PHAN LAN HN",
        "SO 99 NGO 580 TRUONG CHINH,DDA, HN",
        "SO 99 NGO 580 TRUONG CHINH DDA  HN",
        "SO 99 NGUYEN CHI THANH DD HN",
        "SO 99-22/17-LENH CU-KHAM THIEN HN",
        "SO 9A5-HV THANH THIEU NIEN-CHUA LANG-HN",
        "SO 9B DUONG G KHU 2 TRUONG DH NN1 HN",
        "SO 9B HOE NHAI",
        "SO 9B NGACH 25/100 NGO CHO KHAM THIEN HN",
        "SO 9D NGO 1 BUI XUONG TRACH TX HN",
        "SO 9D NGO 1 BUI XUONG TRACH TX HANOI",
    ]

    # for addr in addrs:
    #     print(addr)
    #     result = find_matching_substrings(
    #         text_string=addr,
    #         word_count_limit=2,
    #         pattern="HAI BA TRUNG",
    #     )
    #     print(result)


if __name__ == "__main__":
    main()
