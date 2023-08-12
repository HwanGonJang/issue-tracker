from dataclasses import dataclass

from konlpy.tag import Okt
import re

@dataclass()
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

        all_titles = ' '.join(category_politics['title'])

        return all_titles

    def get_issue_keyword(self, df):
        titles = self._preprocessed_data(df)

        tokens_const = self.okt.nouns(titles)

        const_cnt = {}
        max_words = 20
        for word in tokens_const:
            const_cnt[word] = const_cnt.get(word, 0) + 1
        sorted_w = sorted(const_cnt.items(), key=lambda kv: kv[1])
        result = sorted_w[-max_words:]

        return result[::-1]