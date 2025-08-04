#!/usr/bin/env python3
"""
Test Bluesky API to see what's available
"""

import requests
import json

def test_bluesky_api():
    base_url = "https://bsky.social/xrpc"
    
    # Test public timeline
    print("Testing public timeline...")
    try:
        url = f"{base_url}/app.bsky.feed.getTimeline"
        response = requests.get(url, params={'limit': 10})
        print(f"Status: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"Found {len(data.get('feed', []))} posts")
            for i, item in enumerate(data.get('feed', [])[:3]):
                post = item.get('post', {})
                record = post.get('record', {})
                print(f"Post {i+1}: {record.get('text', '')[:100]}...")
        else:
            print(f"Error: {response.text}")
    except Exception as e:
        print(f"Timeline error: {e}")
    
    # Test popular feed
    print("\nTesting popular feed...")
    try:
        url = f"{base_url}/app.bsky.feed.getFeed"
        response = requests.get(url, params={'feed': 'at://did:plc:z72i7hdynmk6r22z27h6tvur/app.bsky.feed.generator/whats-hot', 'limit': 10})
        print(f"Status: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"Found {len(data.get('feed', []))} posts")
        else:
            print(f"Error: {response.text}")
    except Exception as e:
        print(f"Popular feed error: {e}")

if __name__ == "__main__":
    test_bluesky_api()