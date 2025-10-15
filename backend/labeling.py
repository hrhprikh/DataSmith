import pandas as pd

def simple_sentiment_label(df: pd.DataFrame, text_col: str) -> pd.DataFrame:
    """
    Adds a 'label' column using simple keyword-based sentiment detection.
    """
    positive_words = ["good", "great", "excellent", "awesome", "amazing", "love"]
    negative_words = ["bad", "poor", "worst", "terrible", "awful", "hate"]

    def detect_sentiment(text):
        text = str(text).lower()
        if any(word in text for word in positive_words):
            return "Positive"
        elif any(word in text for word in negative_words):
            return "Negative"
        else:
            return "Neutral"

    df = df.copy()
    df["label"] = df[text_col].apply(detect_sentiment)
    return df
