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
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.qldb.load.LoadEvent;
import software.amazon.qldb.load.writer.RevisionWriter;
import software.amazon.qldb.load.writer.RevisionWriterFactory;
import software.amazon.qldb.load.writer.ValidationResult;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 * AWS Lambda function that consumes document load events from an SQS queue and writes them into a QLDB ledger.  The
 * behavior of this function with respect to writing events into the ledger is primarily controlled by the
 * RevisionWriter, whose type is determined through an environment variable.
 *
 * @see software.amazon.qldb.load.writer.RevisionWriter
 * @see software.amazon.qldb.load.writer.RevisionWriterFactory
 */
public class SQSEventReceiver implements RequestHandler<SQSEvent, SQSBatchResponse> {
    private static final Logger logger = LoggerFactory.getLogger(SQSEventReceiver.class);

    protected final IonSystem ionSystem = IonSystemBuilder.standard().build();
    protected RevisionWriter writer = RevisionWriterFactory.buildFromEnvironment();


    @Override
    public SQSBatchResponse handleRequest(SQSEvent sqsEvent, Context context) {
        if (writer == null)
            throw new RuntimeException("No loader set for this receiver");

        List<SQSBatchResponse.BatchItemFailure> batchItemFailures = new ArrayList<>();
        if (sqsEvent == null || sqsEvent.getRecords() == null) {
            logger.warn("Input is not a valid SQS event.  Ignoring event.");
            return new SQSBatchResponse(batchItemFailures);
        }

        for (SQSEvent.SQSMessage message : sqsEvent.getRecords()) {
            final String messageId = message.getMessageId();

            try {
                IonDatagram datagram = ionSystem.getLoader().load(message.getBody());
                Iterator<IonValue> iter = datagram.iterator();
                if (iter.hasNext()) {
                    IonValue sqsItem = iter.next();
                    if (sqsItem.getType() != IonType.STRUCT) {
                        logger.warn("Unexpected non-struct Ion value received in SQS event payload: " + sqsItem.toPrettyString());
                        continue;
                    }

                    IonStruct struct = (IonStruct) sqsItem;
                    final LoadEvent event;

                    // Did this event come to SQS through SNS?
                    if (struct.containsKey("TopicArn")) {
                        if (struct.containsKey("Message")) {
                            event = LoadEvent.fromString(((IonString) struct.get("Message")).stringValue());
                        } else {
                            logger.info("Poorly formatted SNS-originated event.  Skipping. \n" + sqsItem.toPrettyString());
                            continue;
                        }
                    } else {
                        event = LoadEvent.fromIon(struct);
                    }

                    ValidationResult result = writer.writeEvent(event);
                    if (result.message != null) {
                        logger.warn(result.message);
                        logger.warn(sqsItem.toPrettyString());
                    }

                    if (result.fail) {
                        batchItemFailures.add(new SQSBatchResponse.BatchItemFailure(messageId));
                    }
                }
            } catch (Exception e) {
                logger.error("Error handling message: " + message, e);
                batchItemFailures.add(new SQSBatchResponse.BatchItemFailure(messageId));
            }
        }

        return new SQSBatchResponse(batchItemFailures);
    }
}