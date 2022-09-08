KAFKA_BIN_VERSION="2.13"
KAFKA_VERSION="3.2.1"

echo "kafka install: ${KAFKA_INSTALL}"
if [ -z $KAFKA_INSTALL ]
then
    if [ ! -d $1 ]
    then
        curl https://dlcdn.apache.org/kafka/${KAFKA_VERSION}/kafka_${KAFKA_BIN_VERSION}-${KAFKA_VERSION}.tgz -o kafka.tgz
        tar -xzvf kafka.tgz
        mv kafka_${KAFKA_BIN_VERSION}-${KAFKA_VERSION} $1
        rm kafka.tgz
    fi
fi