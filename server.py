import praw
import socket
import json
import sys


if __name__=="__main__":
    # Check if the correct number of arguments were passed
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

    try:
        # Bind the socket to a specific host and port
        server_socket.bind((host, port))
    except socket.error as e:
        print(f"Failed to bind socket: {e}")
        sys.exit(-1)

    # Listen for incoming connections
    server_socket.listen(1)

    try:
        # Wait for a client to connect
        print("Waiting for a client to connect...")
        client_socket, client_address = server_socket.accept()
        print(f"Client connected from {client_address}")
    except KeyboardInterrupt:
        print("Shutting down server...")
        server_socket.close()
        sys.exit(-1)
    except Exception as e:
        print(f"Failed to accept client connection: {e}")
        server_socket.close()
        sys.exit(-1)

    try:
        # Send data to client
        subreddit = reddit.subreddit('all')
        for submission in subreddit.stream.submissions():
            submission.comments.replace_more(limit=None)
            for comment in submission.comments.list():
                message = {
                    "text": comment.body,
                    "subreddit": comment.subreddit.display_name
                }
                with open("messages.json", "a") as f:
                    f.write(json.dumps(message) + "\n")
                message_str = json.dumps(message) + "\n"
                client_socket.send(message_str.encode("utf-8"))

                #message = comment.body + "\n"
                #client_socket.send(message.encode('utf-8'))
                # print(f"Sent data to client: {message}")
    except KeyboardInterrupt:
        print("Shutting down server...")
        client_socket.close()
        server_socket.close()
        sys.exit(-1)
    except Exception as e:
        print("Client disconnected")
        print(f"Error received: {e}")
        client_socket.close()
        server_socket.close()
        sys.exit(-1)

    client_socket.close()
    server_socket.close()