import praw
import socket
import json

# Define Reddit API credentials
client_id = "zqRTjPqgLq6yqzJPBcYM8w" # YOUR_CLIENT_ID
client_secret = "wLc64W4xQqtu05OuHyVDkhiWKIZGHQ" # YOUR_CLIENT_SECRET
# username = "YOUR_USERNAME" not necessary for read-oly access
# password = "YOUR_PASSWORD"
user_agent = "web:mySparkProject:v1.0.0"# YOUR_USER_AGENT

# Define socket host and port
host = "localhost"
port = 12345

# Create a Reddit instance and authenticate
reddit = praw.Reddit(
    client_id=client_id,
    client_secret=client_secret,
    # username=username,
    # password=password,
    user_agent=user_agent,
)

# Create a socket object
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Bind the socket to a specific host and port
server_socket.bind((host, port))

# Listen for incoming connections
server_socket.listen(1)

# Wait for a client to connect
print("Waiting for a client to connect...")
client_socket, client_address = server_socket.accept()
print(f"Client connected from {client_address}")

# Iterate over the "hot" posts in the "python" subreddit
for post in reddit.subreddit("python").hot(limit=10):
    # Convert the post data to a JSON string
    post_data = json.dumps({
        "title": post.title,
        "url": post.url,
        "author": post.author.name,
        "created_utc": post.created_utc,
    })
    # Send the post data to the client socket
    client_socket.sendall(post_data.encode("utf-8"))
    print(f"Sent data to client: {post_data}")

# Close the client socket and server socket
client_socket.close()
server_socket.close()