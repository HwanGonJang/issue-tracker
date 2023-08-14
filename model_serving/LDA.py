from dataclasses import dataclass

from sklearn.decomposition import LatentDirichletAllocation
from sklearn.feature_extraction.text import TfidfVectorizer

from collections import defaultdict

@dataclass
class LDA:
    def _print_topics(self, components, feature_names, n=5):
        for idx, topic in enumerate(components):
            print(f"Topic {idx + 1}:{[(feature_names[i], topic[i].round(2)) for i in topic.argsort()[:-n - 1:-1]]}")

    def _get_topics(self, components, feature_names, n=5):
        topics = []
        for idx, topic in enumerate(components):
            topics.append([(feature_names[i], topic[i].round(2)) for i in topic.argsort()[:-n - 1:-1]])

        # 딕셔너리 생성 및 단어별 점수 합산
        word_scores = defaultdict(float)
        for topic in topics:
            for word, score in topic:
                word_scores[word] += score

        sorted_word_scores = sorted(word_scores.items(), key=lambda item: item[1], reverse=True)

        return sorted_word_scores[:20]

    def lda_model(self, processed):
        vect = TfidfVectorizer(ngram_range=(1, 1), lowercase=True, tokenizer=lambda x: x.split(), max_features=500)
        input_matrix = vect.fit_transform(processed)
        terms = vect.get_feature_names_out()

        model = LatentDirichletAllocation()
        model.fit_transform(input_matrix)

        return self._get_topics(model.components_, terms)

        # topic = []
        # for comp in model.components_:
        #     idx = comp.argsort()[:-2:-1]
        #     topic.append(terms[idx].item())
        # return topic
