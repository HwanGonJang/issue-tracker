from dataclasses import dataclass

from LDA import LDA

from konlpy.tag import Okt
import re

with open("./stopwords-ko.txt", "r", encoding="utf-8") as f:
    stopwords = [word.strip() for word in f.readlines()]

PATTERN = re.compile(r"[^%s]" % "가-힣a-zA-Z")
URL_PATTERN = re.compile(
    r"((http|https)\:\/\/)?[a-zA-Z0-9\.\/\?\:@\-_=#]+\.([a-zA-Z]){2," r"6}([a-zA-Z0-9\.\&\/\?\:@\-_=#])*"
)
SPACE_PATTERN = re.compile(r"\s+")

@dataclass
class Model:
    okt: Okt
    def __init__(self):
        self.okt = Okt()

    def _preprocessed_data(self, df):
        def remove_bracket_text(title):
            return re.sub(r'\[.*?\]', '', title)

        # '정치' 카테고리에 해당하는 모든 제목을 합친 후 명사 추출 작업 수행
        category_politics = df[df['category'] == '정치']
        category_politics['title'] = category_politics['title'].apply(remove_bracket_text)  # 대괄호 안 신문사 이름 삭제

        return category_politics['title']

    def _cleansing(self, sent):
        res = URL_PATTERN.sub(" ", sent)
        res = PATTERN.sub(" ", res)
        res = SPACE_PATTERN.sub(" ", res)
        return res

    def _tokenize(self, sent, stopwords=None):
        tokens = self.okt.nouns(sent.strip())
        if stopwords:
            tokens = [tok for tok in tokens if len(tok) > 1 and tok not in stopwords]
        else:
            tokens = [tok for tok in tokens if len(tok) > 1]
        return tokens

    def get_issue_keyword(self, df):
        titles = self._preprocessed_data(df)

        all_titles = ' '.join(titles)

        tokens_const = self.okt.nouns(all_titles)

        const_cnt = {}
        max_words = 20
        for word in tokens_const:
            const_cnt[word] = const_cnt.get(word, 0) + 1
        sorted_w = sorted(const_cnt.items(), key=lambda kv: kv[1])
        result = sorted_w[-max_words:]

        return result[::-1]

    def get_issue_keyword_LDA(self, df):
        lda = LDA()
        titles = self._preprocessed_data(df)
        issues = titles.values.tolist()

        processed = []
        for issue in issues:
            cleaned = self._cleansing(issue)
            tokens = self._tokenize(cleaned, stopwords)
            processed.append(" ".join(tokens))

        return lda.lda_model(processed)
