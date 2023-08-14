import re

from konlpy.tag import Okt


PATTERN = re.compile(r"[^%s]" % "가-힣a-zA-Z")
URL_PATTERN = re.compile(
    r"((http|https)\:\/\/)?[a-zA-Z0-9\.\/\?\:@\-_=#]+\.([a-zA-Z]){2," r"6}([a-zA-Z0-9\.\&\/\?\:@\-_=#])*"
)
SPACE_PATTERN = re.compile(r"\s+")

tokenizer = Okt()

def cleansing(sent):
    res = URL_PATTERN.sub(" ", sent)
    res = PATTERN.sub(" ", res)
    res = SPACE_PATTERN.sub(" ", res)
    return res


def tokenize(sent, stopwords=None):
    tokens = tokenizer.nouns(sent.strip())
    if stopwords:
        tokens = [tok for tok in tokens if len(tok) > 1 and tok not in stopwords]
    else:
        tokens = [tok for tok in tokens if len(tok) > 1]
    return tokens
