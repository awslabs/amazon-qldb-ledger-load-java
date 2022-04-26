/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package software.amazon.qldb.load.receiver;

import com.amazon.ion.*;
import com.amazon.ion.system.IonSystemBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.amazonaws.services.lambda.runtime.events.models.kinesis.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.model.EncryptionType;
import software.amazon.kinesis.retrieval.AggregatorUtil;
import software.amazon.kinesis.retrieval.KinesisClientRecord;
import software.amazon.qldb.load.LoadEvent;
import software.amazon.qldb.load.Operation;
import software.amazon.qldb.load.writer.RevisionWriter;
import software.amazon.qldb.load.writer.RevisionWriterFactory;
import software.amazon.qldb.load.writer.ValidationResult;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;


/**
 * AWS Lambda function that consumes revision details from a QLDB ledger stream writes them into a target QLDB
 * ledger.  The behavior of this function with respect to writing events into the ledger is primarily controlled by the
 * RevisionWriter, whose type is determined through an environment variable.
 *
 * @see software.amazon.qldb.load.writer.RevisionWriter
 * @see software.amazon.qldb.load.writer.RevisionWriterFactory
 */
public class QldbLedgerStreamEventReceiver implements RequestHandler<KinesisEvent, Void> {
    private static final Logger logger = LoggerFactory.getLogger(QldbLedgerStreamEventReceiver.class);

    private final static IonSystem ionSystem = IonSystemBuilder.standard().build();
    protected RevisionWriter writer = RevisionWriterFactory.buildFromEnvironment();
    protected final AggregatorUtil aggregatorUtil = new AggregatorUtil();

    @Override
    public Void handleRequest(KinesisEvent kinesisEvent, Context context) {
        boolean failBatch = false;

        List<KinesisClientRecord> recList = new ArrayList<>();
        for (KinesisEvent.KinesisEventRecord ker : kinesisEvent.getRecords()) {
            Record record = ker.getKinesis();
            KinesisClientRecord kcr = KinesisClientRecord.builder()
                    .data(record.getData())
                    .approximateArrivalTimestamp(record.getApproximateArrivalTimestamp().toInstant())
                    .sequenceNumber(record.getSequenceNumber())
                    .partitionKey(record.getPartitionKey())
                    .encryptionType(EncryptionType.fromValue(record.getEncryptionType()))
                    .build();

            recList.add(kcr);
        }

        List<KinesisClientRecord> userRecords = aggregatorUtil.deaggregate(recList);
        for (KinesisClientRecord kcr : userRecords)  {
            ByteBuffer buf = kcr.data();
            byte[] bytes = new byte[buf.remaining()];
            buf.get(bytes);

            IonDatagram datagram = ionSystem.getLoader().load(bytes);
            for (IonValue gramValue : datagram) {
                if (gramValue.getType() != IonType.STRUCT) {
                    context.getLogger().log("Unexpected non-struct Ion value received in Kinesis event payload: " + gramValue.toPrettyString());
                    continue;
                }

                IonStruct streamRecord = (IonStruct) gramValue;
                IonStruct payload = (IonStruct) streamRecord.get("payload");
                String recordType = ((IonString) streamRecord.get("recordType")).stringValue();
                if (!"REVISION_DETAILS".equals(recordType))
                    continue;

                IonStruct tableInfo = (IonStruct) payload.get("tableInfo");
                IonStruct revision = (IonStruct) payload.get("revision");
                IonStruct metadata = (IonStruct) revision.get("metadata");

                int version = ((IonInt) metadata.get("version")).intValue();

                Operation op;
                if (!revision.containsKey("data"))
                    op = Operation.DELETE;
                else if (version == 0)
                    op = Operation.INSERT;
                else
                    op = Operation.UPDATE;

                final LoadEvent event = new LoadEvent();

                event.setOperation(op);
                event.setId(metadata.get("id"));
                event.setTableName(((IonString) tableInfo.get("tableName")).stringValue());
                event.setVersion(version);

                if (revision.containsKey("data")) {
                    IonStruct data = (IonStruct) revision.get("data");
                    event.setRevision(data.clone());
                }

                ValidationResult result = writer.writeEvent(event);
                if (result.message != null) {
                    logger.warn(result.message);
                    logger.warn(gramValue.toPrettyString());
                }

                failBatch = failBatch || result.fail;
            }
        }

        //
        // Instead of erroring-out when we encounter our first failed event load, process all of the load events in the
        // batch.  An event that occurs in the stream after the failing event may fix the condition that the first event
        // failed on (for example, if events are in the stream out-of-order).  Otherwise, the failure event may end up
        // being a logjam that stalls the stream.
        //
        if (failBatch)
            throw new RuntimeException("Batch contained failures.");

        return null;
    }
}