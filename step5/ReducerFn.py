def main(shuffle_result: dict) -> dict:
    # Sum the counts for each word
    return {word: sum(counts) for word, counts in shuffle_result.items()}