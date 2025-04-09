import csv

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


def save_posts_to_csv(posts: list, filename: str):
    """
    Save a list of blog posts to a CSV file.
    
    Args:
        posts (list): List of blog post dictionaries.
        filename (str): The name of the file to save to.
    """
    if not posts:
        print("No posts to save.")
        return

    # Use field names from the BlogPost model
    fieldnames = BlogPost.model_fields.keys()

    with open(filename, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(posts)
    print(f"Saved {len(posts)} blog posts to '{filename}'.")
