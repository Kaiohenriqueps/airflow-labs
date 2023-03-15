def split(my_list, n):
    k, m = divmod(len(my_list), n)
    return list(
        (my_list[i * k + min(i, m) : (i + 1) * k + min(i + 1, m)] for i in range(n))
    )
