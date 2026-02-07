class ContentFilter:
    def __init__(self, text_ratio_threshold=0.5):
        self._text_ratio_threshold = text_ratio_threshold

    def should_skip(self, text):
        if not text or not text.strip():
            return True
        total = len(text)
        text_chars = sum(1 for c in text if c.isalnum() or c.isspace())
        if total == 0:
            return True
        return (text_chars / total) < self._text_ratio_threshold
