if [ $# -lt 1 ]; then
  echo "USAGE: $0 <config>"
  exit 1
fi

KAFKA_CONFIG=$1

# Ensure KAFKA_HOME is set
if [ -z "$KAFKA_HOME" ]; then
  echo "KAFKA_HOME is not set. Please set it to the Kafka installation directory."
  exit 1
fi

# Start Kafka server
exec $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_CONFIG