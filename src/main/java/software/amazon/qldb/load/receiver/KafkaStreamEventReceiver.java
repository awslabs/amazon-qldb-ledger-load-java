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
import com.amazonaws.services.lambda.runtime.events.KafkaEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.qldb.load.LoadEvent;
import software.amazon.qldb.load.writer.RevisionWriter;
import software.amazon.qldb.load.writer.RevisionWriterFactory;
import software.amazon.qldb.load.writer.ValidationResult;

import java.util.Base64;


/**
 * AWS Lambda function that consumes document load events from a Kafka Stream and writes them into a QLDB
 * ledger.  The behavior of this function with respect to writing events into the ledger is primarily controlled by the
 * RevisionWriter, whose type is determined through an environment variable.
 *
 * @see software.amazon.qldb.load.writer.RevisionWriter
 * @see software.amazon.qldb.load.writer.RevisionWriterFactory
 */
public class KafkaStreamEventReceiver implements RequestHandler<KafkaEvent, Void> {
    private static final Logger logger = LoggerFactory.getLogger(KafkaStreamEventReceiver.class);

    private final static IonSystem ionSystem = IonSystemBuilder.standard().build();
    protected RevisionWriter writer = RevisionWriterFactory.buildFromEnvironment();


    @Override
    public Void handleRequest(KafkaEvent kafkaEvent, Context context) {
        boolean failBatch = false;

        if (kafkaEvent == null || kafkaEvent.getRecords() == null) {
            logger.warn("Input is not a valid Kafka event.  Ignoring event.");
            return null;
        }

        for (String key : kafkaEvent.getRecords().keySet()) {
            for (KafkaEvent.KafkaEventRecord rec : kafkaEvent.getRecords().get(key)) {
                byte[] bytes = Base64.getDecoder().decode(rec.getValue());
                IonDatagram datagram = ionSystem.getLoader().load(bytes);
                for (IonValue gramValue : datagram) {
                    if (gramValue.getType() != IonType.STRUCT) {
                        context.getLogger().log("Unexpected non-struct Ion value received in Kafka event payload: " + gramValue.toPrettyString());
                        continue;
                    }

                    IonStruct streamRecord = (IonStruct) gramValue;
                    final LoadEvent event = LoadEvent.fromIon(streamRecord);

                    ValidationResult result = writer.writeEvent(event);
                    if (result.message != null) {
                        logger.warn(result.message);
                        logger.warn(gramValue.toPrettyString());
                    }

                    failBatch = failBatch || result.fail;
                }
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
