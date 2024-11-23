package org.apache.pulsar.ecosystem.io.paimon.sink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.pulsar.client.api.schema.GenericRecord;

@Slf4j
public class PaimonWriter {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static synchronized void write(GenericRecord message, StreamTableWrite write) throws Exception {
        // 转换 GenericRecord 到 GenericRow
        GenericRow row = convertToRow(message);
        log.info("record = {}, write a message to paimon",message);
        write.write(row,1);
    }

    //GenericRecord 到 GenericRow 的转换逻辑
    private static GenericRow convertToRow(GenericRecord record) throws JsonProcessingException {
        GenericRow row = new GenericRow(10);

        row.setField(0, BinaryString.fromString((String) record.getField("messageId")));
        log.info("messageId is  {}", record.getField("messageId"));

        row.setField(1, BinaryString.fromString((String) record.getField("address")));
        log.info("address is  {}", record.getField("address"));

        row.setField(2, BinaryString.fromString((String) record.getField("subscription")));
        log.info("subscription is  {}", record.getField("subscription"));

        row.setField(3, BinaryString.fromString((String) record.getField("topic")));
        log.info("topic is  {}", record.getField("topic"));

        row.setField(4, BinaryString.fromString((String) record.getField("partition")));
        log.info("partition is  {}", record.getField("partition"));

        row.setField(5, BinaryString.fromString((String) record.getField("event")));
        log.info("event is  {}", record.getField("event"));

        row.setField(6, BinaryString.fromString((String) record.getField("producerName")));
        log.info("producerName is  {}", record.getField("producerName"));

        row.setField(7, BinaryString.fromString((String) record.getField("consumerName")));
        log.info("consumerName is  {}", record.getField("consumerName"));

        row.setField(8, BinaryString.fromString((String) record.getField("producerId")));
        log.info("producerId is  {}", record.getField("producerId"));

        row.setField(9, BinaryString.fromString((String) record.getField("consumerId")));
        log.info("consumerId is  {}", record.getField("consumerId"));

        return row;
    }
}