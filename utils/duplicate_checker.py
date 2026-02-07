import threading
from collections import deque, Counter

from .tokenizer import tokenize_string


def _to_vector(tokens):
    return dict(Counter(tokens))


def _cosine_sim(vec_a, vec_b):
    if not vec_a or not vec_b:
        return 0.0
    dot = sum(vec_a.get(k, 0) * vec_b.get(k, 0) for k in set(vec_a) & set(vec_b))
    norm_a = sum(v * v for v in vec_a.values()) ** 0.5
    norm_b = sum(v * v for v in vec_b.values()) ** 0.5
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


class DuplicateChecker(object):
    def __init__(self, n=1000, similarity_threshold=0.9):
        self.n = n
        self.threshold = similarity_threshold
        self._vectors = deque(maxlen=n)
        self._lock = threading.Lock()

    def _vectorize(self, text):
        tokens = tokenize_string(text)
        return _to_vector(tokens) if tokens else {}

    def is_duplicate(self, doc_text):
        """Check and add atomically. Returns True if duplicate."""
        vec = self._vectorize(doc_text)
        if not vec:
            return False
        with self._lock:
            for stored in self._vectors:
                if _cosine_sim(vec, stored) >= self.threshold:
                    return True
            self._vectors.append(vec)
            return False
