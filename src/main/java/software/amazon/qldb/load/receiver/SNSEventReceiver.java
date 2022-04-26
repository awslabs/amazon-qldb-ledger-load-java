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
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.qldb.load.LoadEvent;
import software.amazon.qldb.load.writer.RevisionWriter;
import software.amazon.qldb.load.writer.RevisionWriterFactory;
import software.amazon.qldb.load.writer.ValidationResult;

import java.util.Iterator;


/**
 * AWS Lambda function that consumes document load events from an SNS topic and writes them into a QLDB ledger.The
 * behavior of this function with respect to writing events into the ledger is primarily controlled by the
 * RevisionWriter, whose type is determined through an environment variable.
 *
 * @see software.amazon.qldb.load.writer.RevisionWriter
 * @see software.amazon.qldb.load.writer.RevisionWriterFactory
 */
public class SNSEventReceiver implements RequestHandler<SNSEvent, Object> {
    private static final Logger logger = LoggerFactory.getLogger(SNSEventReceiver.class);

    protected final IonSystem ionSystem = IonSystemBuilder.standard().build();
    protected RevisionWriter writer = RevisionWriterFactory.buildFromEnvironment();


    public Object handleRequest(SNSEvent request, Context context) {

        for (SNSEvent.SNSRecord record : request.getRecords()) {
            IonDatagram datagram = ionSystem.getLoader().load(record.getSNS().getMessage());
            Iterator<IonValue> iter = datagram.iterator();
            if (iter.hasNext()) {
                IonValue snsItem = iter.next();
                if (snsItem.getType() != IonType.STRUCT) {
                    logger.warn("Unexpected non-struct Ion value received in SNS event payload: " + record.getSNS().getMessage());
                    continue;
                }

                IonStruct struct = (IonStruct) snsItem;
                final LoadEvent event = LoadEvent.fromIon(struct);

                ValidationResult result = writer.writeEvent(event);
                if (result.message != null) {
                    logger.warn(result.message);
                    logger.warn(snsItem.toPrettyString());
                }

                if (result.fail) {
                    throw new RuntimeException("Load failure: " + result.message + ":  " + record.getSNS().getMessage());
                }
            }
        }

        return null;
    }
}
