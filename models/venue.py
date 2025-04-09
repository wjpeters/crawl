from pydantic import BaseModel


class BlogPost(BaseModel):
    """
    Represents the data structure of a blog post.
    """

    title: str
    body: str
    link: str
