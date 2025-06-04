import unicodedata

def remove_accents(input_str):
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    return ''.join([c for c in nfkd_form if not unicodedata.combining(c)])

# Example usage
original = "nguyễn"
converted = remove_accents(original)
print(converted)  # Output: nguyen
