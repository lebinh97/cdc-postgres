FROM debezium/connect:latest
COPY connector.json /connector.json
COPY startup.sh /startup.sh
ENTRYPOINT ["/bin/sh", "/startup.sh"]
