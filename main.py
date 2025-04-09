import asyncio
import argparse
import time

from crawl4ai import AsyncWebCrawler
from dotenv import load_dotenv

from config import BASE_URL, REQUIRED_KEYS
from utils.data_utils import (
    save_posts_to_csv,
)
from utils.scraper_utils import (
    extract_blog_post_links,
    scrape_blog_post,
    get_browser_config,
)

load_dotenv()


async def crawl_blog_posts(max_posts: int = 10, delay_seconds: int = 5):
    """
    Main function to crawl blog post data from the UpGuard blog.
    
    Args:
        max_posts (int): Maximum number of blog posts to crawl
        delay_seconds (int): Delay between API calls to avoid rate limits
    """
    # Initialize configurations
    browser_config = get_browser_config()
    session_id = "blog_post_crawl_session"

    # Initialize state variables
    all_posts = []
    seen_titles = set()
    successful_posts = 0
    failed_posts = 0

    print(f"Starting blog crawler. Will scrape up to {max_posts} posts with {delay_seconds}s delay between requests.")

    # Start the web crawler context
    async with AsyncWebCrawler(config=browser_config) as crawler:
        try:
            # Step 1: Extract links to blog posts from the main page
            links = await extract_blog_post_links(crawler, BASE_URL, session_id)
            
            if not links:
                print("No blog post links found on the main page.")
                return
                
            print(f"Found {len(links)} blog post links. Will scrape up to {max_posts} posts.")
            
            # Step 2: Visit each link and scrape the full blog post content
            for i, link_data in enumerate(links):
                # Stop after reaching the maximum number of posts
                if successful_posts >= max_posts:
                    print(f"Reached the maximum of {max_posts} blog posts. Stopping.")
                    break
                    
                title = link_data.get("title", "Unknown Title")
                link = link_data.get("link", "")
                
                if not link:
                    print(f"Skipping post with missing link: {title}")
                    continue
                    
                # Skip duplicates
                if title in seen_titles:
                    print(f"Skipping duplicate post: {title}")
                    continue
                
                # Delay between requests to avoid rate limits
                if i > 0:
                    print(f"Waiting {delay_seconds} seconds before next request...")
                    await asyncio.sleep(delay_seconds)
                    
                # Scrape the individual blog post
                post_data = await scrape_blog_post(crawler, link, title, session_id)
                
                if post_data and all(key in post_data for key in REQUIRED_KEYS):
                    all_posts.append(post_data)
                    seen_titles.add(title)
                    successful_posts += 1
                    print(f"Successfully scraped post {successful_posts}/{max_posts}: {title}")
                    
                    # Save after each successful post to preserve progress
                    if successful_posts % 2 == 0:
                        save_posts_to_csv(all_posts, "upguard_blog_posts.csv")
                        print(f"Saved {len(all_posts)} blog posts to 'upguard_blog_posts.csv' (intermediate save)")
                else:
                    failed_posts += 1
                    print(f"Failed to scrape post or missing required data: {title}")
        
        except Exception as e:
            print(f"Error during crawling: {str(e)}")
            # Save what we have so far
            if all_posts:
                save_posts_to_csv(all_posts, "upguard_blog_posts.csv")
                print(f"Saved {len(all_posts)} blog posts to 'upguard_blog_posts.csv' after error.")

    # Final save of the collected posts to a CSV file
    if all_posts:
        save_posts_to_csv(all_posts, "upguard_blog_posts.csv")
        print(f"Saved {len(all_posts)} blog posts to 'upguard_blog_posts.csv'.")
        print(f"Summary: {successful_posts} posts successfully scraped, {failed_posts} posts failed.")
    else:
        print("No blog posts were found during the crawl.")


async def main():
    """
    Entry point of the script.
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Scrape blog posts from UpGuard blog')
    parser.add_argument('--max-posts', type=int, default=10, help='Maximum number of posts to scrape (default: 10)')
    parser.add_argument('--delay', type=int, default=5, help='Delay between requests in seconds (default: 5)')
    
    # Parse args only if running from command line
    import sys
    if len(sys.argv) > 1:
        args = parser.parse_args()
        max_posts = args.max_posts
        delay = args.delay
    else:
        # Default values when run without arguments
        max_posts = 10
        delay = 5
        
    await crawl_blog_posts(max_posts=max_posts, delay_seconds=delay)


if __name__ == "__main__":
    asyncio.run(main())
