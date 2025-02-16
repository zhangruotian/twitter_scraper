import asyncio
import os
import random
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm
from twikit import Client


class TwitterScraper:
    def __init__(self):
        load_dotenv()
        self.client = Client()
        self.cookies_file = "cookies.json"

        # Rate limiting parameters
        self.requests_count = 0
        self.reset_time = time.time() + 900  # 15 minutes in seconds
        self.max_requests = 45  # Keep it under 50 to be safe

        # Create output directory
        self.output_dir = Path("output")
        self.output_dir.mkdir(exist_ok=True)

        # Initialize CSV file
        self.current_file = None
        self.df = None

    async def setup_client(self):
        """Setup client with login or cookies"""
        if os.path.exists(self.cookies_file):
            try:
                self.client.load_cookies(self.cookies_file)
                print("Loaded existing cookies")
                return
            except Exception as e:
                print(f"Error loading cookies: {e}")

        # If no cookies or loading failed, login with credentials
        auth_info_1 = os.getenv("TWITTER_AUTH_INFO_1", "")
        auth_info_2 = os.getenv("TWITTER_AUTH_INFO_2", "")
        password = os.getenv("TWITTER_PASSWORD", "")

        if not all([auth_info_1, auth_info_2, password]):
            raise ValueError("Twitter credentials not found in .env file")

        await self.client.login(
            auth_info_1=auth_info_1, auth_info_2=auth_info_2, password=password
        )
        self.client.save_cookies(self.cookies_file)
        print("Logged in and saved cookies")

    def check_rate_limit(self):
        """Check and handle rate limiting with optimized delays"""
        current_time = time.time()

        # Reset counter if 15 minutes have passed
        if current_time > self.reset_time:
            self.requests_count = 0
            self.reset_time = current_time + 900

        # If we've hit the limit, wait until reset
        if self.requests_count >= self.max_requests:
            wait_time = self.reset_time - current_time
            print(f"\nRate limit reached. Waiting {wait_time:.2f} seconds...")

            # Show progress bar during wait
            with tqdm(total=int(wait_time), desc="Rate limit cooldown") as pbar:
                while time.time() < self.reset_time:
                    time.sleep(1)
                    pbar.update(1)

            self.requests_count = 0
            self.reset_time = time.time() + 900

        # Add randomized cooldown between requests (10-20 seconds)
        # This helps avoid hitting rate limits too quickly
        cooldown = random.uniform(10, 17)
        time.sleep(cooldown)
        self.requests_count += 1

    def init_csv(self, keyword):
        """Initialize CSV file for incremental saving"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.current_file = self.output_dir / f"tweets_{keyword}_{timestamp}.csv"
        # Create empty DataFrame with correct columns
        self.df = pd.DataFrame(
            columns=[
                "id",
                "text",
                "created_at",
                "author_id",
                "author_username",
                "retweet_count",
                "like_count",
                "reply_count",
                "media_urls",  # List of media URLs (images, videos)
                "urls",  # List of URLs mentioned in tweet
                "has_media",  # Boolean indicating if tweet has media
                "media_types",  # List of media types (photo, video, etc.)
            ]
        )
        # Save empty DataFrame to create file with headers
        self.df.to_csv(self.current_file, index=False)
        print(f"Initialized output file: {self.current_file}")

    def save_tweets_batch(self, tweets_batch):
        """Save a batch of tweets to CSV"""
        if not tweets_batch:
            return

        df_batch = pd.DataFrame(tweets_batch)
        df_batch.to_csv(self.current_file, mode="a", header=False, index=False)

    async def search_tweets(self, keyword, max_tweets=100):
        """Search for tweets containing the keyword"""
        print(f"Searching for tweets containing '{keyword}'...")
        self.init_csv(keyword)

        tweets_batch = []
        total_tweets = 0

        try:
            # Initial search
            self.check_rate_limit()
            result = await self.client.search_tweet(keyword, product="Top")

            while result and total_tweets < max_tweets:
                # Process current batch of tweets
                for tweet in result:
                    # Extract media information
                    media_urls = []
                    media_types = []
                    has_media = False

                    # Check media attribute directly
                    if hasattr(tweet, "media"):
                        for media in tweet.media:
                            media_type = getattr(media, "type", "")
                            media_types.append(media_type)
                            has_media = True

                            # Handle different media types
                            if media_type == "photo":
                                # Get the photo URL
                                media_url = getattr(media, "media_url", "None")
                                if media_url:
                                    media_urls.append(
                                        f"{media_url}?format=jpg&name=large"
                                    )
                            elif media_type in ["video", "animated_gif"]:
                                # For videos and GIFs, get the stream
                                if hasattr(media, "streams"):
                                    # Get the last stream (usually highest quality)
                                    media_urls.append(media.streams[-1].url) # type:ignore

                    # Extract URLs from tweet
                    urls = []
                    if hasattr(tweet, "urls"):
                        for url_entity in tweet.urls:
                            expanded_url = getattr(url_entity, "expanded_url", "")
                            if expanded_url:
                                urls.append(expanded_url)

                    tweet_data = {
                        "id": tweet.id,
                        "text": tweet.text,
                        "created_at": tweet.created_at,
                        "author_id": tweet.user.id,
                        "author_username": tweet.user.name,
                        "retweet_count": tweet.retweet_count,
                        "like_count": tweet.favorite_count,
                        "reply_count": tweet.reply_count,
                        "media_urls": "|".join(media_urls) if media_urls else "",
                        "urls": "|".join(urls) if urls else "",
                        "has_media": has_media,
                        "media_types": "|".join(media_types) if media_types else "",
                    }
                    tweets_batch.append(tweet_data)
                    total_tweets += 1

                    # Save in batches of 20 tweets
                    if len(tweets_batch) >= 20:
                        self.save_tweets_batch(tweets_batch)
                        tweets_batch = []
                        print(f"Collected and saved {total_tweets} tweets...")

                    if total_tweets >= max_tweets:
                        break

                # If we haven't reached max_tweets, get next page
                if total_tweets < max_tweets:
                    self.check_rate_limit()
                    result = await result.next()  # Get next page of results
                    if not result:
                        print("No more tweets available")
                        break

            # Save any remaining tweets
            if tweets_batch:
                self.save_tweets_batch(tweets_batch)
                print(f"Collected and saved {total_tweets} tweets...")

        except Exception as e:
            print(f"Error during search: {e}")
            # Save any remaining tweets in case of error
            if tweets_batch:
                self.save_tweets_batch(tweets_batch)

        print(f"\nCompleted! Total tweets collected: {total_tweets}")
        print(f"Results saved to: {self.current_file}")


async def main():
    scraper = TwitterScraper()
    await scraper.setup_client()
    keyword = "disaster"
    max_tweets = 1000
    await scraper.search_tweets(keyword, max_tweets)


if __name__ == "__main__":
    asyncio.run(main())
