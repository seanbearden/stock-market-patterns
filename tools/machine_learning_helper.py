def find_first_above_threshold(lst, threshold):
    for index, value in enumerate(lst):
        if value > threshold:
            return index
    return None  # or an appropriate value indicating no such element was found


def find_first_below_threshold(lst, threshold):
    for index, value in enumerate(lst):
        if value < threshold:
            return index
    return None  # or an appropriate value indicating no such element was found

