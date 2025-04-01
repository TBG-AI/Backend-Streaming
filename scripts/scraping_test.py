from playwright.sync_api import sync_playwright
from requests_html import HTMLSession


# playwright
with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()
    page.goto("https://www.whoscored.com/matches/1821159/live/england-premier-league-2024-2025-manchester-united-arsenal")
    content = page.content()
    browser.close()

print(content)