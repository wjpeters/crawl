import csv
import os
import tempfile
import shutil

from models.venue import BlogPost


def is_duplicate_post(post_title: str, seen_titles: set) -> bool:
    """
    Check if a blog post with the given title has already been seen.
    
    Args:
        post_title (str): The title of the blog post.
        seen_titles (set): Set of post titles that have already been seen.
        
    Returns:
        bool: True if the post title is in the set of seen titles, False otherwise.
    """
    return post_title in seen_titles


def is_complete_post(post: dict, required_keys: list) -> bool:
    """
    Check if a blog post has all the required keys.
    
    Args:
        post (dict): The blog post data.
        required_keys (list): List of required keys.
        
    Returns:
        bool: True if the post has all required keys, False otherwise.
    """
    return all(key in post and post[key] for key in required_keys)


def save_posts_to_csv(posts: list, filename: str, append: bool = False):
    """
    Save a list of blog posts to a CSV file.
    
    Args:
        posts (list): List of blog post dictionaries.
        filename (str): The name of the file to save to.
        append (bool): Whether to append to an existing file (True) or overwrite (False).
    """
    if not posts:
        print("No posts to save.")
        return

    # Use field names from the BlogPost model
    fieldnames = BlogPost.model_fields.keys()
    
    # Check if the file exists and we should append
    file_exists = os.path.exists(filename)
    
    # If not appending or file doesn't exist, just write directly
    if not append or not file_exists:
        with open(filename, mode="w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(posts)
        print(f"Saved {len(posts)} blog posts to '{filename}'.")
        return
    
    # Handle appending with deduplication
    # Load existing posts and track by link
    existing_posts = {}
    try:
        with open(filename, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                if 'link' in row and row['link']:
                    existing_posts[row['link']] = row
    except Exception as e:
        print(f"Error reading existing posts: {str(e)}")
        # Fall back to simple append if there's an error
        with open(filename, mode="a", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writerows(posts)
        print(f"Appended {len(posts)} blog posts to '{filename}'.")
        return
    
    # Process new posts - update existing entries or add new ones
    updated_count = 0
    added_count = 0
    
    for post in posts:
        if post.get('link') in existing_posts:
            # Update the existing entry
            existing_posts[post['link']] = post
            updated_count += 1
        else:
            # Add new entry
            existing_posts[post['link']] = post
            added_count += 1
    
    # Write back all posts
    with open(filename, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(existing_posts.values())
    
    print(f"Updated CSV file '{filename}': {added_count} posts added, {updated_count} posts updated.")
