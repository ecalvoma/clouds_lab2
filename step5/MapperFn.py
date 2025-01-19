def main(file_content: str) -> list:
    # Tokenize the file content into words
    words = file_content.split()
    # Emit (word, 1) pairs
    return [(word.lower(), 1) for word in words]