import asyncio
import argparse
import time
import os
import csv
import random

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


def load_existing_posts(csv_filename: str) -> set:
    """
    Load already scraped posts from the CSV file.
    
    Args:
        csv_filename (str): Path to the CSV file
        
    Returns:
        set: Set of links that have already been scraped
    """
    scraped_links = set()
    
    if not os.path.exists(csv_filename):
        print(f"No existing CSV file found at {csv_filename}")
        return scraped_links
        
    try:
        with open(csv_filename, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                if 'link' in row and row['link']:
                    scraped_links.add(row['link'])
                    
        print(f"Loaded {len(scraped_links)} already scraped posts from {csv_filename}")
    except Exception as e:
        print(f"Error loading existing posts: {str(e)}")
        
    return scraped_links


async def crawl_blog_posts(max_posts: int = 10, delay_seconds: int = 5, csv_filename: str = "upguard_blog_posts.csv", random_factor: float = 0.3):
    """
    Main function to crawl blog post data from the UpGuard blog.
    
    Args:
        max_posts (int): Maximum number of blog posts to crawl
        delay_seconds (int): Delay between API calls to avoid rate limits
        csv_filename (str): Path to the CSV file to save posts
        random_factor (float): Probability (0-1) of selecting an already scraped post to refresh
    """
    # Load already scraped posts
    already_scraped_links = load_existing_posts(csv_filename)
    
    # Initialize configurations
    browser_config = get_browser_config()
    session_id = "blog_post_crawl_session"

    # Initialize state variables
    all_posts = []
    seen_titles = set()
    seen_links = set()
    processed_posts = 0  # Total posts processed (both new and refreshed)
    successful_posts = 0
    failed_posts = 0
    skipped_posts = 0
    refreshed_posts = 0

    print(f"Starting blog crawler. Will scrape up to {max_posts} posts with {delay_seconds}s delay between requests.")
    print(f"Random factor: {random_factor:.2f} - Approximately {int(random_factor*100)}% chance to refresh already scraped posts")

    # Start the web crawler context
    async with AsyncWebCrawler(config=browser_config) as crawler:
        try:
            # Step 1: Extract links to blog posts from the main page
            links = await extract_blog_post_links(crawler, BASE_URL, session_id)
            
            if not links:
                print("No blog post links found on the main page.")
                return
                
            total_new_posts = sum(1 for link_data in links if link_data.get("link") and link_data["link"] not in already_scraped_links)
            print(f"Found {len(links)} blog post links. {total_new_posts} are new. Will scrape up to {max_posts} posts.")
            
            # Shuffle the links to get random selection
            random.shuffle(links)
            
            # Step 2: Visit each link and scrape the full blog post content
            for i, link_data in enumerate(links):
                # Stop after reaching the maximum number of posts
                if processed_posts >= max_posts:
                    print(f"Reached the maximum of {max_posts} posts processed. Stopping.")
                    break
                    
                title = link_data.get("title", "Unknown Title")
                link = link_data.get("link", "")
                
                if not link:
                    print(f"Skipping post with missing link: {title}")
                    continue
                    
                # Skip duplicates within current run
                if link in seen_links:
                    print(f"Skipping duplicate link in this run: {title}")
                    continue
                
                seen_links.add(link)
                
                # For already scraped posts, decide randomly whether to refresh them
                if link in already_scraped_links:
                    if random.random() < random_factor:
                        print(f"Randomly selected to refresh already scraped post: {title}")
                        # Continue with refreshing this post
                    else:
                        print(f"Skipping already scraped post: {title}")
                        skipped_posts += 1
                        continue
                
                # Delay between requests to avoid rate limits
                if i > 0:
                    # Add slight randomness to delay to appear more natural
                    current_delay = delay_seconds * (0.8 + 0.4 * random.random())
                    print(f"Waiting {current_delay:.1f} seconds before next request...")
                    await asyncio.sleep(current_delay)
                    
                # Scrape the individual blog post
                post_data = await scrape_blog_post(crawler, link, title, session_id)
                
                # Increment the processed counter regardless of success
                processed_posts += 1
                
                if post_data and all(key in post_data for key in REQUIRED_KEYS):
                    all_posts.append(post_data)
                    if link in already_scraped_links:
                        refreshed_posts += 1
                        print(f"Successfully refreshed post {processed_posts}/{max_posts}: {title}")
                    else:
                        successful_posts += 1
                        print(f"Successfully scraped new post {processed_posts}/{max_posts}: {title}")
                    
                    # Save after each successful post to preserve progress
                    if len(all_posts) >= 2 or processed_posts >= max_posts:
                        save_posts_to_csv(all_posts, csv_filename, append=True)
                        print(f"Saved {len(all_posts)} blog posts to '{csv_filename}' (intermediate save)")
                        # Clear the list since we've saved them
                        all_posts = []
                else:
                    failed_posts += 1
                    print(f"Failed to scrape post or missing required data: {title}")
                    
                # Debug output to track progress
                print(f"Progress: {processed_posts}/{max_posts} posts processed ({successful_posts} new, {refreshed_posts} refreshed, {failed_posts} failed)")
        
        except Exception as e:
            print(f"Error during crawling: {str(e)}")
            # Save what we have so far
            if all_posts:
                save_posts_to_csv(all_posts, csv_filename, append=True)
                print(f"Saved {len(all_posts)} blog posts to '{csv_filename}' after error.")

    # Final save of any remaining posts
    if all_posts:
        save_posts_to_csv(all_posts, csv_filename, append=True)
        print(f"Saved {len(all_posts)} blog posts to '{csv_filename}'.")
        
    print(f"Summary: Processed {processed_posts} posts - {successful_posts} new posts successfully scraped, {refreshed_posts} posts refreshed, {skipped_posts} posts skipped, {failed_posts} posts failed.")


async def main():
    """
    Entry point of the script.
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Scrape blog posts from UpGuard blog')
    parser.add_argument('--max-posts', type=int, default=10, help='Maximum number of posts to scrape (default: 10)')
    parser.add_argument('--delay', type=int, default=5, help='Delay between requests in seconds (default: 5)')
    parser.add_argument('--output', type=str, default='upguard_blog_posts.csv', help='Output CSV file (default: upguard_blog_posts.csv)')
    parser.add_argument('--random', type=float, default=0.3, help='Random factor for refreshing already scraped posts (0-1, default: 0.3)')
    
    # Parse args only if running from command line
    import sys
    if len(sys.argv) > 1:
        args = parser.parse_args()
        max_posts = args.max_posts
        delay = args.delay
        csv_filename = args.output
        random_factor = min(max(args.random, 0.0), 1.0)  # Clamp between 0 and 1
    else:
        # Default values when run without arguments
        max_posts = 10
        delay = 5
        csv_filename = 'upguard_blog_posts.csv'
        random_factor = 0.3
        
    await crawl_blog_posts(max_posts=max_posts, delay_seconds=delay, csv_filename=csv_filename, random_factor=random_factor)


if __name__ == "__main__":
    asyncio.run(main())
