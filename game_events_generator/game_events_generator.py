from random import randint
from datetime import datetime
import time

from google.cloud import pubsub_v1


def main():
    # Set your Google Cloud project ID and topic name
    project_id = "alk-big-data-processing"
    topic_name = "game-events"

    # Create a publisher client
    publisher = pubsub_v1.PublisherClient()

    # Create the topic path
    topic_path = publisher.topic_path(project_id, topic_name)

    # Define the teams and users
    teams = [
        "The Invincibles",
        "The Mavericks",
        "The Dream Team",
        "The Rising Phoenix",
        "The Trailblazers",
        "The Dominators",
        "The Visionaries",
        "The Elite Squad",
        "The Fireballs",
        "The Thunderbolts"
    ]
    users_per_team = 10

    # Publish messages indefinitely
    while True:
        # Generate random message data
        team = teams[randint(0, len(teams) - 1)]
        random_number = randint(1, users_per_team)
        user = f"{team.replace(' ', '')}{random_number}"
        random_number = randint(0, 20)
        date = datetime.now()
        timestamp = int(date.timestamp() * 1000)
        formatted_date = date.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        message_data = f"{user},{team},{random_number},{timestamp},{formatted_date}".encode()

        # Publish the message to the topic
        future = publisher.publish(topic_path, data=message_data)
        message_id = future.result()

        print(f"Message published. Message ID: {message_id}")

        # Sleep for 1 second before publishing the next message
        time.sleep(1)


if __name__ == '__main__':
    main()
