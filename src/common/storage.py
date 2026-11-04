# Saving and loading the two json files under data/.
# Rule: write to a .tmp file, fsync it, then rename it over the real one.
# Never open the real file and start typing into it — if we crash halfway,
# the log still has to be readable when we come back.
