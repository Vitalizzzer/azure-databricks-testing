import base64


def encode_to_base64(file):
    with open(file, "rb") as f:
        encoded = base64.b64encode(f.read())
    return encoded


def base64_to_string(base):
    return base.decode('utf-8')
