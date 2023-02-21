def MapFunction(records: list):
    groups = {}
    for record in records:
        key, value = record["item_type"], record["quantity"]
        if key not in groups:
            groups[key] = []
        groups[key].append(value)

    return groups


def ReduceFunction(values: list):
    return sum(values)
