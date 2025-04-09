import json
import os
import re
import time
from typing import List, Set, Tuple, Dict, Optional

from crawl4ai import (
    AsyncWebCrawler,
    BrowserConfig,
    CacheMode,
    CrawlerRunConfig,
    LLMExtractionStrategy,
)

from models.venue import BlogPost
from utils.data_utils import is_complete_post, is_duplicate_post


def get_browser_config() -> BrowserConfig:
    """
    Returns the browser configuration for the crawler.

    Returns:
        BrowserConfig: The configuration settings for the browser.
    """
    # https://docs.crawl4ai.com/core/browser-crawler-config/
    return BrowserConfig(
        browser_type="chromium",  # Type of browser to simulate
        headless=False,  # Whether to run in headless mode (no GUI)
        verbose=True,  # Enable verbose logging
    )


def get_content_extraction_strategy(max_tokens: int = 4000) -> LLMExtractionStrategy:
    """
    Returns the configuration for extracting content from individual blog posts.

    Args:
        max_tokens (int): Maximum token limit to use for content extraction.

    Returns:
        LLMExtractionStrategy: The settings for how to extract blog post content using LLM.
    """
    # https://docs.crawl4ai.com/api/strategies/#llmextractionstrategy
    return LLMExtractionStrategy(
        # Using a smaller model to avoid rate limits
        provider="groq/llama3-8b-8192",  # Name of the LLM provider
        api_token=os.getenv("GROQ_API_KEY"),  # API token for authentication
        schema=BlogPost.model_json_schema(),  # JSON schema of the data model
        extraction_type="schema",  # Type of extraction to perform
        instruction=(
            "Extract the blog post content with:\n"
            "1. title: The title of the blog post\n"
            "2. body: A brief summary of the main points (1 paragraph only)\n"
            "3. link: The current URL of the blog post\n"
            "Be very concise. Focus only on extracting the main article content, ignoring navigation, headers, footers, etc."
        ),
        input_format="markdown",  # Format of the input content
        max_tokens=max_tokens,  # Limit token usage
        verbose=True,  # Enable verbose logging
    )


def extract_main_content(html_content: str, max_chars: int = 8000) -> str:
    """
    Extracts just the main content of a blog post to reduce text size.
    
    Args:
        html_content (str): The full HTML content
        max_chars (int): Maximum characters to extract
        
    Returns:
        str: Extracted main content
    """
    # Try to find the main article content
    main_content_patterns = [
        r'<article[^>]*>(.*?)</article>',
        r'<div[^>]*class="[^"]*post-content[^"]*"[^>]*>(.*?)</div>',
        r'<div[^>]*class="[^"]*blog-content[^"]*"[^>]*>(.*?)</div>',
        r'<div[^>]*class="[^"]*content[^"]*"[^>]*>(.*?)</div>'
    ]
    
    for pattern in main_content_patterns:
        matches = re.findall(pattern, html_content, re.DOTALL)
        if matches:
            # Take the first match and strip HTML tags
            content = re.sub(r'<[^>]+>', ' ', matches[0])
            # Remove extra whitespace
            content = re.sub(r'\s+', ' ', content).strip()
            # Truncate if necessary
            if len(content) > max_chars:
                content = content[:max_chars] + "..."
            return content
    
    # If no main content found, extract the title and take some content from the page
    title_match = re.search(r'<title[^>]*>(.*?)</title>', html_content)
    title = title_match.group(1) if title_match else "Blog Post"
    
    # Strip all HTML tags and take a portion
    content = re.sub(r'<[^>]+>', ' ', html_content)
    content = re.sub(r'\s+', ' ', content).strip()
    
    # Limit content length
    if len(content) > max_chars:
        content = content[:max_chars] + "..."
    
    return f"{title}\n\n{content}"


async def extract_blog_post_links(
    crawler: AsyncWebCrawler,
    url: str,
    session_id: str,
    max_links: int = 15  # Extract more links than needed in case some fail
) -> List[Dict[str, str]]:
    """
    Extracts links to blog posts from the main blog page using direct HTML parsing.

    Args:
        crawler (AsyncWebCrawler): The web crawler instance.
        url (str): The URL of the main blog page.
        session_id (str): The session identifier.
        max_links (int): Maximum number of links to extract.

    Returns:
        List[Dict[str, str]]: List of dictionaries with blog post titles and links.
    """
    print(f"Extracting blog post links from {url}...")
    
    # Fetch the main page without any extraction strategy
    result = await crawler.arun(
        url=url,
        config=CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS,
            session_id=session_id,
        ),
    )
    
    if not result.success:
        print(f"Error fetching the main page: {result.error_message}")
        return []
        
    # Use regex to find blog post links and titles from the HTML
    links_data = []
    
    # Pattern for blog post links - looking for blog post links specifically
    blog_link_pattern = r'<a\s+[^>]*href="([^"]*(?:\/blog\/[^"]*|\/blog\/[^\/]+\/[^"]*))"\s*[^>]*>(.*?)<\/a>'
    
    matches = re.findall(blog_link_pattern, result.cleaned_html)
    
    for link, title_html in matches:
        # Clean up the title - remove HTML tags
        title = re.sub(r'<[^>]+>', '', title_html).strip()
        if title and link and "/blog/" in link:
            # Skip obvious navigation links
            if any(x in title.lower() for x in ['next', 'previous', 'all posts', 'view all']):
                continue
                
            # Make sure link is absolute
            if not link.startswith("http"):
                if link.startswith("/"):
                    link = f"https://www.upguard.com{link}"
                else:
                    link = f"https://www.upguard.com/{link}"
            
            # Only add links that seem to be actual blog posts (not category pages)
            if len(title) > 10 and link not in [item["link"] for item in links_data]:
                links_data.append({"title": title, "link": link})
    
    # Sort links to prioritize actual blog posts over category pages
    links_data.sort(key=lambda x: 0 if x["link"].count("/") > 4 else 1)
    
    # Limit the number of links
    links_data = links_data[:max_links]
    
    print(f"Found {len(links_data)} blog post links using direct HTML parsing")
    return links_data


async def scrape_blog_post(
    crawler: AsyncWebCrawler,
    url: str,
    title: str,
    session_id: str,
) -> Optional[Dict]:
    """
    Scrapes the content of an individual blog post.

    Args:
        crawler (AsyncWebCrawler): The web crawler instance.
        url (str): The URL of the blog post.
        title (str): The title of the blog post (from the link extraction).
        session_id (str): The session identifier.

    Returns:
        Optional[Dict]: Blog post data if successful, None otherwise.
    """
    print(f"Scraping blog post: {title} at {url}")
    
    try:
        # First fetch the page without extraction strategy to get content
        result = await crawler.arun(
            url=url,
            config=CrawlerRunConfig(
                cache_mode=CacheMode.BYPASS,
                session_id=session_id,
            ),
        )
        
        if not result.success:
            print(f"Error fetching blog post page: {result.error_message}")
            return fallback_post(title, url)
            
        # Extract and preprocess main content to reduce size
        main_content = extract_main_content(result.cleaned_html)
        
        # Create a more targeted config for content extraction
        content_strategy = get_content_extraction_strategy()
        
        # Extract content from the preprocessed HTML
        result = await crawler.process_content(
            content=main_content,
            url=url,
            config=CrawlerRunConfig(
                extraction_strategy=content_strategy,
                session_id=session_id,
            )
        )
        
        if not (result.success and result.extracted_content):
            print(f"Error extracting content: {result.error_message}")
            return fallback_post(title, url, content_snippet=main_content[:500])
            
        # Parse extracted content
        post_data = json.loads(result.extracted_content)
        
        # In case the title wasn't extracted properly, use the one from the link
        if not post_data.get("title"):
            post_data["title"] = title
                
        # Make sure the link is included
        if not post_data.get("link"):
            post_data["link"] = url
                
        return post_data
    except Exception as e:
        print(f"Error when scraping blog post {title}: {str(e)}")
        return fallback_post(title, url)


def fallback_post(title: str, url: str, content_snippet: str = "") -> Dict:
    """
    Creates a fallback blog post when extraction fails.
    
    Args:
        title (str): The title of the blog post
        url (str): The URL of the blog post
        content_snippet (str): Optional snippet of content if available
        
    Returns:
        Dict: A basic blog post with available information
    """
    body = "Content extraction failed due to API limitations."
    if content_snippet:
        # Clean up the snippet
        snippet = re.sub(r'\s+', ' ', content_snippet).strip()
        body = f"{body} Partial content: {snippet[:300]}..."
        
    return {
        "title": title,
        "body": body,
        "link": url
    }


async def check_no_results(
    crawler: AsyncWebCrawler,
    url: str,
    session_id: str,
) -> bool:
    """
    Checks if there are no blog posts on the page.

    Args:
        crawler (AsyncWebCrawler): The web crawler instance.
        url (str): The URL to check.
        session_id (str): The session identifier.

    Returns:
        bool: True if no blog posts are found, False otherwise.
    """
    # Fetch the page without any CSS selector or extraction strategy
    result = await crawler.arun(
        url=url,
        config=CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS,
            session_id=session_id,
        ),
    )

    if result.success:
        # Check if there are any blog post cards on the page
        if "blog-post-card" not in result.cleaned_html:
            return True
    else:
        print(
            f"Error fetching page for blog post check: {result.error_message}"
        )

    return False


async def fetch_and_process_page(
    crawler: AsyncWebCrawler,
    page_number: int,
    base_url: str,
    css_selector: str,
    llm_strategy: LLMExtractionStrategy,
    session_id: str,
    required_keys: List[str],
    seen_titles: Set[str],
) -> Tuple[List[dict], bool]:
    """
    Fetches and processes a single page of blog posts.

    Args:
        crawler (AsyncWebCrawler): The web crawler instance.
        page_number (int): The page number to fetch.
        base_url (str): The base URL of the website.
        css_selector (str): The CSS selector to target the content.
        llm_strategy (LLMExtractionStrategy): The LLM extraction strategy.
        session_id (str): The session identifier.
        required_keys (List[str]): List of required keys in the blog post data.
        seen_titles (Set[str]): Set of blog post titles that have already been seen.

    Returns:
        Tuple[List[dict], bool]:
            - List[dict]: A list of processed blog posts from the page.
            - bool: A flag indicating if no more blog posts were found.
    """
    # For UpGuard blog, the URL format might be different
    if page_number == 1:
        url = base_url
    else:
        url = f"{base_url}?page={page_number}"
    
    print(f"Loading page {page_number}...")

    # Check if there are no blog posts on the page
    no_results = await check_no_results(crawler, url, session_id)
    if no_results:
        return [], True  # No more results, signal to stop crawling

    # Fetch page content with the extraction strategy
    result = await crawler.arun(
        url=url,
        config=CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS,  # Do not use cached data
            extraction_strategy=llm_strategy,  # Strategy for data extraction
            css_selector=css_selector,  # Target specific content on the page
            session_id=session_id,  # Unique session ID for the crawl
        ),
    )

    if not (result.success and result.extracted_content):
        print(f"Error fetching page {page_number}: {result.error_message}")
        return [], False

    # Parse extracted content
    extracted_data = json.loads(result.extracted_content)
    if not extracted_data:
        print(f"No blog posts found on page {page_number}.")
        return [], False

    # After parsing extracted content
    print("Extracted data:", extracted_data)

    # Process blog posts
    complete_posts = []
    for post in extracted_data:
        # Debugging: Print each post to understand its structure
        print("Processing blog post:", post)

        # Ignore the 'error' key if it's False
        if post.get("error") is False:
            post.pop("error", None)  # Remove the 'error' key if it's False

        if not is_complete_post(post, required_keys):
            print(f"Incomplete post: {post}")
            continue  # Skip incomplete posts

        if is_duplicate_post(post["title"], seen_titles):
            print(f"Duplicate post '{post['title']}' found. Skipping.")
            continue  # Skip duplicate posts

        # Make sure the link is a complete URL
        if post["link"] and not post["link"].startswith("http"):
            if post["link"].startswith("/"):
                post["link"] = f"https://www.upguard.com{post['link']}"
            else:
                post["link"] = f"https://www.upguard.com/{post['link']}"

        # Add post to the list
        seen_titles.add(post["title"])
        complete_posts.append(post)

    if not complete_posts:
        print(f"No complete blog posts found on page {page_number}.")
        return [], False

    print(f"Extracted {len(complete_posts)} blog posts from page {page_number}.")
    return complete_posts, False  # Continue crawling
