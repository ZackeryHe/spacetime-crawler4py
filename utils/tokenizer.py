from collections import Counter


def tokenize_string(text):
    if not text or not isinstance(text, str):
        return []
    normalized = ''.join(
        c if c.isalnum() and c.isascii() else (' ' if c.isascii() else '')
        for c in text
    )
    tokens = []
    for word in normalized.split():
        if word and all(c.isalnum() for c in word):
            tokens.append(word.lower())
    return tokens


class TokenEncoder:
    def __init__(self, file_path=None, text=None):
        self._file_path = file_path
        self._text = text
        self._tokens = []
        self._frequencies = Counter()

    def _tokenize_input(self):
        if self._file_path:
            with open(self._file_path, 'r') as f:
                return tokenize_string(f.read())
        if self._text:
            return tokenize_string(self._text)
        return []

    def tokenize(self):
        self._tokens = self._tokenize_input()
        return self._tokens

    def compute_word_frequency(self):
        if not self._tokens:
            self.tokenize()
        self._frequencies = Counter(self._tokens)
        return dict(self._frequencies)
