def clean(old_tuples):
    new_tuples = []
    for tuple in old_tuples:
        new_entry = []
        for entry in tuple:
            if type(entry) == str:
                new_entry.append(entry.strip())
            else:
                new_entry.append(entry)
        new_tuples.append(new_entry)
    return new_tuples
