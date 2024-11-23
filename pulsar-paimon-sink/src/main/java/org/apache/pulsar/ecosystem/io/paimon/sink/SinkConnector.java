package org.apache.pulsar.ecosystem.io.paimon.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import java.util.List;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.client.api.schema.GenericRecord;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

@Slf4j
public class SinkConnector implements Sink<GenericRecord> {

    private final PaimonWriter paimonWriter = new PaimonWriter();
    StreamTableWrite write;
    Table table;
    StreamWriteBuilder writeBuilder;
    int count = 0;
    long commitIdentifier = 0;

    @Override
    public void open(Map<String, Object> configMap, SinkContext sinkContext) throws Exception {
        // 1. Create a WriteBuilder (Serializable)
        table = GetTable.getTable();
        writeBuilder = table.newStreamWriteBuilder();

        // 2. Write records in distributed tasks
        this.write = writeBuilder.newWrite();

    }

    @Override
    public void write(Record<GenericRecord> record) throws Exception {
        count++;
        GenericRecord value = record.getMessage().get().getValue();
        paimonWriter.write(value,write);

        List<CommitMessage> messages = write.prepareCommit(false, commitIdentifier);
        commitIdentifier++;

        // 3. Collect all CommitMessages to a global node and commit
        StreamTableCommit commit = writeBuilder.newCommit();
        commit.commit(commitIdentifier, messages);

        if(commitIdentifier % 1000 == 0){
            //调用flink api从paimon中读取数据，验证数据是否写入成功
            PredicateBuilder builder =
                    new PredicateBuilder(RowType.of(DataTypes.STRING(), DataTypes.STRING(),
                            DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING(),
                            DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING(),
                            DataTypes.STRING(), DataTypes.STRING()));
            Predicate notNull = builder.isNotNull(0);

            int[] projection = new int[] {0, 1, 2, 3, 4, 5, 6};

            ReadBuilder readBuilder =
                    table.newReadBuilder()
                            .withProjection(projection)
                            .withFilter(Lists.newArrayList(notNull));

            // 2. Plan splits in 'Coordinator' (or named 'Driver')
            List<Split> splits = readBuilder.newScan().plan().splits();

            // 3. Distribute these splits to different tasks

            // 4. Read a split in task
            TableRead read = readBuilder.newRead();
            RecordReader<InternalRow> reader = read.createReader(splits);
            reader.forEachRemaining(row -> {
                String msg = row.getString(0) + ":" + row.getString(1)+ ":" + row.getString(2)+
                        ":" + row.getString(3)
                        + ":" + row.getString(4)+ ":" + row.getString(5)+ ":" + row.getString(6);
                log.info("msg: " + msg);
            });
        }
    }

    @Override
    public void close() throws Exception {
        log.info("Paimon Sink Connector closed.");
    }
}