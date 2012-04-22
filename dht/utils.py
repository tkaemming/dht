def last(iterator):
    """Returns the last item in an iterator."""
    for item in iterator:
        last = item

    try:
        return last
    except NameError:
        raise ValueError('Cannot fetch the last item from an empty iterator')
