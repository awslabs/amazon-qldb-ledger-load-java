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
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.qldb.load.LoadEvent;
import software.amazon.qldb.load.writer.RevisionWriter;
import software.amazon.qldb.load.writer.RevisionWriterFactory;
import software.amazon.qldb.load.writer.ValidationResult;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;


/**
 * AWS Lambda function that consumes document load events from an EventBridge bus and writes them into a QLDB ledger.  The
 * behavior of this function with respect to writing events into the ledger is primarily controlled by the
 * RevisionWriter, whose type is determined through an environment variable.
 *
 * @see software.amazon.qldb.load.writer.RevisionWriter
 * @see software.amazon.qldb.load.writer.RevisionWriterFactory
 */
public class EventBridgeEventReceiver implements RequestStreamHandler {
    private static final Logger logger = LoggerFactory.getLogger(EventBridgeEventReceiver.class);

    private final static IonSystem ionSystem = IonSystemBuilder.standard().build();
    protected RevisionWriter writer = RevisionWriterFactory.buildFromEnvironment();

    @Override
    public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context) throws IOException {

        try (outputStream; IonReader reader = ionSystem.newReader(inputStream)) {
            IonDatagram datagram = ionSystem.getLoader().load(reader);
            for (IonValue gramValue : datagram) {
                if (gramValue.getType() != IonType.STRUCT) {
                    context.getLogger().log("Unexpected non-struct Ion value received in event payload: " + gramValue.toPrettyString());
                    continue;
                }

                IonStruct eventBusRecord = (IonStruct) gramValue;
                if (!eventBusRecord.containsKey("detail"))
                    continue;

                IonStruct payload = ((IonStruct) eventBusRecord.get("detail"));
                final LoadEvent event = LoadEvent.fromIon(payload);

                ValidationResult result = writer.writeEvent(event);
                if (result.message != null) {
                    logger.warn(result.message);
                    logger.warn(gramValue.toPrettyString());
                }

                if (result.fail) {
                    throw new RuntimeException("Event failed validation. " + result.message + ":  " + payload.toPrettyString());
                }
            }
        }
    }
}
