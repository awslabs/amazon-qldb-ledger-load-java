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
package software.amazon.qldb.load.writer;

import com.amazon.ion.IonStruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.qldb.QldbDriver;
import software.amazon.qldb.TransactionExecutor;
import software.amazon.qldb.load.LoadEvent;

import java.util.ArrayList;
import java.util.List;


/**
 * Defines generic operations of verifying and writing a document revision received through a loading mechanism into a
 * ledger.
 */
public abstract class RevisionWriter {
    private static final Logger logger = LoggerFactory.getLogger(RevisionWriter.class);

    protected QldbDriver driver;

    /**
     * Reads the current revision of a given document, if it exists, based on the given identifier and ledger table.
     * The meaning of the identifier (e.g. whether it is a document ID or indexed unique value) is left to
     * implementations.
     *
     * @param txn An open ledger transactions
     * @param event The load event containing the new document revision, its ledger table name, and its identifier
     * @return  The current document revision in the given table with the given identifier or null if it does not exist
     */
    protected abstract IonStruct readCurrentRevision(TransactionExecutor txn, LoadEvent event);

    /**
     * Performs any validations on the event prior to interacting with the ledger.  This is useful for making sure
     * the table specified in the event is valid and active, for example.
     *
     * @param event The load event containing the new document revision, its ledger table name, and its identifier
     * @return The result of the validation, including any validation messages
     */
    protected abstract ValidationResult preValidate(LoadEvent event);

    /**
     * Perform any validations prior to writing the new revision into the ledger.
     *
     * @param txn An open ledger transaction
     * @param event The load event containing the new document revision, its ledger table name, and its identifier
     * @param currentRevision The current revision, if it exists, of the document to be written
     * @return The result of the validation, including any validation messages
     */
    protected abstract ValidationResult validate(TransactionExecutor txn, LoadEvent event, IonStruct currentRevision);

    /**
     * Provides a means of making adjustments to the new revision prior to committing it to the ledger using information
     * from the current revision.  For example, adding a reference to an old document ID if the data is being loaded
     * from a ledger backup.
     *
     * @param txn An open ledger transaction
     * @param event The load event containing the new document revision, its ledger table name, and its identifier
     * @param currentRevision The current revision, if it exists, of the document to be written
     */
    protected abstract void adjustRevision(TransactionExecutor txn, LoadEvent event, IonStruct currentRevision);

    /**
     * Writes the new revision from the load event into the ledger.
     *
     * @param txn An open ledger transaction
     * @param event The load event containing the new document revision, its ledger table name, and its identifier
     * @param currentRevision The current revision, if it exists, of the document to be written
     */
    protected abstract void writeDocument(TransactionExecutor txn, LoadEvent event, IonStruct currentRevision);


    /**
     * Writes the load event into the ledger.
     *
     * @param event The event to write to the ledger
     * @return The result of the validation and write operation, including error message.
     */
    public ValidationResult writeEvent(LoadEvent event) {
        if (event == null || !event.isValid()) {
            return ValidationResult.skip("Event is not complete enough to process. Skipping.");
        }

        ValidationResult result = preValidate(event);
        if (result.skip || result.fail)
            return result;

        return driver.execute(txn -> {
            return writeEvent(txn, event);
        });
    }


    /**
     * Writes multiple load events to the ledger in one atomic transaction.  Poorly-formed events and events that belong
     * to inactive tables are skipped.
     *
     * @param events The events to write to the ledger.
     */
    public void writeEvents(List<LoadEvent> events) {
        if (events == null || events.size() == 0)
            return;

        List<LoadEvent> loadEvents = new ArrayList<>();
        for (LoadEvent event : events) {
            if (!event.isValid()) {
                logger.warn("Invalid event ignore: " + event.toPrettyString());
                continue;
            }

            ValidationResult result = preValidate(event);
            if (result.fail || result.skip) {
                logger.warn("Event failed pre-validation.  " + result.message + ": " + event.toPrettyString());
                continue;
            }

            loadEvents.add(event);
        }

        if (loadEvents.size() == 0)
            return;

        driver.execute(txn -> {
            for (LoadEvent event : loadEvents) {
                ValidationResult result = writeEvent(txn, event);

                if (result.fail || result.skip) {
                    throw new RuntimeException(result.message);
                }
            }
        });
    }


    /**
     * Writes a load event to the ledger in the provided transaction.
     *
     * @param txn   The transaction to use to write the event
     * @param event The event to write to the ledger
     * @return  The result of event validation
     */
    private ValidationResult writeEvent(TransactionExecutor txn, LoadEvent event) {
        IonStruct currentRevision = readCurrentRevision(txn, event);
        ValidationResult result = validate(txn, event, currentRevision);

        if (!result.fail && !result.skip) {
            adjustRevision(txn, event, currentRevision);
            writeDocument(txn, event, currentRevision);
        }

        return result;
    }
}
