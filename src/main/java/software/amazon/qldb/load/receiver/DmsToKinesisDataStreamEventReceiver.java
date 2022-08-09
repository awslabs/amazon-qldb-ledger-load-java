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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.qldb.load.LoadEvent;
import software.amazon.qldb.load.Operation;
import software.amazon.qldb.load.eventmap.LoadEventMapper;
import software.amazon.qldb.load.eventmap.LoadEventMapperFactory;
import software.amazon.qldb.load.writer.RevisionWriter;
import software.amazon.qldb.load.writer.RevisionWriterFactory;


/**
 * AWS Lambda function that consumes AWS Database Migration Service (DMS) events from a Kinesis Data Stream and writes
 * them into a QLDB ledger, enabling automated migration and CDC replication of data from relational databases,
 * mainframes, and any other data source that DMS supports.  The behavior of this function with respect to writing events
 * into the ledger is primarily controlled by the RevisionWriter, whose type is determined through an environment variable.
 *
 * @see software.amazon.qldb.load.writer.RevisionWriter
 * @see software.amazon.qldb.load.writer.RevisionWriterFactory
 */
public class DmsToKinesisDataStreamEventReceiver implements RequestHandler<KinesisEvent, Void> {
    private static final Logger logger = LoggerFactory.getLogger(KinesisDataStreamEventReceiver.class);

    private final static IonSystem ionSystem = IonSystemBuilder.standard().build();
    protected RevisionWriter writer = RevisionWriterFactory.buildFromEnvironment();
    protected LoadEventMapper mapper = LoadEventMapperFactory.buildFromEnvironment();


    @Override
    public Void handleRequest(KinesisEvent kinesisEvent, Context context) {
        if (kinesisEvent == null || kinesisEvent.getRecords() == null) {
            logger.warn("Input is not a valid Kinesis event.  Ignoring event.");
            return null;
        }

        boolean failBatch = false;

        for (KinesisEvent.KinesisEventRecord rec : kinesisEvent.getRecords()) {
            IonDatagram datagram = ionSystem.getLoader().load(rec.getKinesis().getData().array());
            for (IonValue gramValue : datagram) {
                if (gramValue.getType() != IonType.STRUCT) {
                    continue;
                }

                IonStruct streamRecord = (IonStruct) gramValue;
                if (!(streamRecord.containsKey("metadata") && streamRecord.containsKey("data"))) {
                    continue;
                }

                if (!((IonString) streamRecord.get("record-type")).toString().equals("data")) {
                    continue; // Skip control records
                }

                IonStruct metadata = (IonStruct) streamRecord.get("metadata");
                IonStruct data = (IonStruct) streamRecord.get("data");

                Operation op = null;
                String opString = ((IonString) metadata.get("operation")).stringValue();
                switch (opString) {
                    case "load":
                    case "insert":
                        op = Operation.INSERT;
                        break;
                    case "update":
                        op = Operation.UPDATE;
                        break;
                    case "delete":
                        op = Operation.DELETE;
                        break;
                    default:
                        context.getLogger().log("Unexpected data operation \"" + opString + "\".  Skipping.");
                        continue;
                }

                String sourceTableName = ((IonString) metadata.get("table-name")).stringValue();
                IonValue id = mapper.mapPrimaryKey(data, sourceTableName);
                if (id == null) {
                    context.getLogger().log("Unable to determine primary key for record.  Skipping.  " + streamRecord.toPrettyString());
                    continue;
                }

                final LoadEvent event = new LoadEvent();
                event.setRevision(mapper.mapDataRecord(data, sourceTableName));
                event.setOperation(op);
                event.setTableName(mapper.mapTableName(sourceTableName));
                event.setId(id);
                if (op == Operation.INSERT) {
                    event.setVersion(0);
                }

                context.getLogger().log(event.toPrettyString());
//
//                ValidationResult result = writer.writeEvent(event);
//                if (result.message != null) {
//                    logger.warn(result.message);
//                    logger.warn(gramValue.toPrettyString());
//                }
//
//                failBatch = failBatch || result.fail;
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
