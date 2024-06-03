import hashlib


def create_hash_id(df, columns):
    # Create Columns ID
    df["Id"] = df[columns].astype(str).apply(lambda x: "".join(x), axis=1)
    df["Id"] = df["Id"].apply(lambda x: hashlib.sha256(x.encode()).hexdigest())

    return df
