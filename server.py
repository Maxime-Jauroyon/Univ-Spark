import praw
import socket
import json
import sys

if len(sys.argv) != 3:
    print("Usage: server.py <hostname> <port>", file=sys.stderr)
    sys.exit(-1)

# Define Reddit API credentials
client_id = "zqRTjPqgLq6yqzJPBcYM8w" # OUR_CLIENT_ID
client_secret = "wLc64W4xQqtu05OuHyVDkhiWKIZGHQ" # OUR_CLIENT_SECRET
# username = "OUR_USERNAME" not necessary for read-only access
# password = "OUR_PASSWORD"
user_agent = "web:RedditStreamingSentimentAnalysis:v1.0.0"# OUR_USER_AGENT

# Define socket host and port
host = sys.argv[1] # "localhost"
port = int(sys.argv[2]) # 8888

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


subreddit = reddit.subreddit("python")
for submission in subreddit.stream.submissions():
    # message = ""
    submission.comments.replace_more(limit=None)
    for comment in submission.comments.list():
        message = comment.body + " "
        client_socket.send(message.encode('utf-8'))
        # print(f"Sent data to client: {message}")
 

# # Iterate over the "hot" posts in the "python" subreddit
# for post in reddit.subreddit("python").hot(limit=10):
#     # Convert the post data to a JSON string
#     post_data = json.dumps({
#         "title": post.title,
#         "url": post.url,
#         "author": post.author.name,
#         "created_utc": post.created_utc,
#     })
#     # Send the post data to the client socket
#     client_socket.sendall(post_data.encode("utf-8"))
#     print(f"Sent data to client: {post_data}")

# Close the client socket and server socket
client_socket.close()
server_socket.close()