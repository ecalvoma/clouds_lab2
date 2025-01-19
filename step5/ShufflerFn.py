from collections import defaultdict

def main(map_results: list) -> dict:
    # Group (word, 1) pairs by word
    shuffle_result = defaultdict(list)
    for result in map_results:
        for word, count in result:
            shuffle_result[word].append(count)
    return shuffle_result