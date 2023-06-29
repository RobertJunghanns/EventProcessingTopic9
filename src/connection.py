import stomp


class LogListener(stomp.ConnectionListener):
    def on_error(self, message):
        print('received an error "%s"' % message)

    def on_message(self, message):
        print('received a message "%s"' % message)


class ExceptionListener(stomp.ConnectionListener):
    def on_error(self, message):
        raise Exception('received an error "%s"' % message)

    def on_message(self, message):
        raise Exception('received a message "%s"' % message)


class ActiveMQNode:
    def __init__(self, connection, id_, query_topic, input_topics):
        self.conn = connection
        self.id = id_
        self.query_topic = query_topic
        self.input_topics = input_topics

        for topic in input_topics:
            self.conn.subscribe(
                destination=f"/topic/{topic}",
                id=f"{topic}_{id_}",
                ack="auto",
            )

    def send(self, message):
        self.conn.send(body=message, destination=f"/topic/{self.query_topic}")

    def disconnect(self):
        self.conn.disconnect()

    def __str__(self):
        return f"Connection(conn={self.conn}, id='{self.id}', query_topic='{self.query_topic}', input_topics={self.input_topics})"
