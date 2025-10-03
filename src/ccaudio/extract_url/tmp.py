import requests
from bs4 import BeautifulSoup
from cc_audio.url2xml_audio import extract_audio_entries_from_rss
from lingua import LanguageDetectorBuilder

detector = LanguageDetectorBuilder.from_all_spoken_languages().build()


def is_japanese(text: str) -> bool:
    language = detector.detect_language_of(text)
    if language is None:
        return False
    return language.iso_code_639_1.name == "JA"


url = "https://anchor.fm/s/76596fc/podcast/rss"
myagent = "cc-get-started/1.0 (Example data retrieval script; yourname@example.com)"
response = requests.get(url, stream=True, headers={"user-agent": myagent})
xml_content = response.text
soup = BeautifulSoup(xml_content, "xml")
# lang
channel = soup.find("channel")
language_tag = channel.find("language") if channel else None
language = language_tag.text.strip() if language_tag else None
print(language)
print(extract_audio_entries_from_rss(xml_content))
