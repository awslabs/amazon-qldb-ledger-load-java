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

import com.amazon.ion.IonInt;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.qldb.QldbDriver;
import software.amazon.qldb.Result;
import software.amazon.qldb.TransactionExecutor;
import software.amazon.qldb.load.LoadEvent;
import software.amazon.qldb.load.Operation;
import software.amazon.qldb.load.util.LoaderUtils;

import java.util.Iterator;
import java.util.List;


/**
 * Provides a base revision writer with the most common functionality.  This implementation creates an "oldDocumentId"
 * field in written revisions to track the ID in the LoadEvent, assuming that it represents a document ID for the data
 * in another ledger.
 *
 * In strict mode, validation will report an error if an existing revision doesn't exist for delete or update operations,
 * if an existing revision exists for an insert operation, or if the new revision's version number is more than one
 * greater than the version number of the existing revision for an update operation. This is the default mode for this
 * loader and is recommended for an initial load of a data set.
 *
 * If strict mode is false, validation will ignore if records have already been inserted or deleted or if there's
 * already a later version of the document in the table.  This mode is useful for re-processing an interrupted data load
 * operation because it will quietly ignore work that has already been done.  One warning to this mode is that update
 * operations will be skipped if the document does not already exist, assuming that a subsequent delete has already
 * been processed.
 */
public class BaseRevisionWriter extends RevisionWriter {
    private static final Logger logger = LoggerFactory.getLogger(BaseRevisionWriter.class);

    private boolean strictMode = true;
    private List<String> activeTables;


    protected BaseRevisionWriter(QldbDriver driver, boolean strictMode) {
        if (driver == null)
            throw new IllegalArgumentException("QLDB driver required");

        this.driver = driver;
        this.strictMode = strictMode;
    }

    public boolean isStrictMode() {
        return strictMode;
    }

    protected void initialize() {
        this.activeTables = LoaderUtils.fetchActiveLedgerTables(driver);
    }


    /**
     * Reads the current revision of a given document, if it exists, based on the given identifier and ledger table. This
     * implementation assumes the ID is the document metadata ID from another ledger and that documents in
     * the target ledger can be identified with this ID using an indexed field called "oldDocumentId".
     *
     * NOTE:  This implementation reads from the committed view of the table, so the returned IonStruct will contain
     *        metadata, hash, blockSequence, and data fields.
     *
     * @param txn An open ledger transactions
     * @param event The load event containing the new document revision, its ledger table name, and its identifier
     * @return  The committed view of the current document revision in the given table with the given identifier or null if
     * it does not exist
     */
    @Override
    protected IonStruct readCurrentRevision(TransactionExecutor txn, LoadEvent event) {
        if (txn == null || event == null || event.getTableName() == null || event.getId() == null)
            return null;

        Result result = txn.execute("SELECT * FROM _ql_committed_" + event.getTableName() + " WHERE data.oldDocumentId = ?", event.getId());

        Iterator<IonValue> iter = result.iterator();
        return iter.hasNext() ? (IonStruct) iter.next() : null;
    }


    /**
     * Adds an "oldDocumentId" field to the new revision containing the ID from the event.
     *
     * This implementation adds an "oldDocumentId" field to the event to be written to the ledger.
     *
     * @param txn An open ledger transaction
     * @param event The load event containing the new document revision, its ledger table name, and its identifier
     * @param currentRevision The current revision, if it exists, of the document to be written
     */
    @Override
    protected void adjustRevision(TransactionExecutor txn, LoadEvent event, IonStruct currentRevision) {
        if (event != null && event.getRevision() != null && event.getId() != null)
            event.getRevision().put("oldDocumentId", event.getId().clone());
    }


    /**
     * Verifies that the event's table is valid and active in the target ledger.
     *
     * @param event The load event containing the new document revision, its ledger table name, and its identifier
     * @return The result of the validation, including any validation messages
     */
    @Override
    protected ValidationResult preValidate(LoadEvent event) {
        return activeTables.contains(event.getTableName()) ? ValidationResult.pass() :
                ValidationResult.skip(event.getTableName() + " is not an active table");
    }


    /**
     * Performs validations prior to writing the new revision into the ledger.  Validation behavior depends on the
     * strict mode setting.
     *
     * @param txn An open ledger transaction
     * @param event The load event containing the new document revision, its ledger table name, and its identifier
     * @param currentRevision The current revision, if it exists, of the document to be written
     */
    @Override
    protected ValidationResult validate(TransactionExecutor txn, LoadEvent event, IonStruct currentRevision) {
        Operation op = event.getOperation();

        if (op == Operation.INSERT && currentRevision != null) {
            logger.info("Record already exists for INSERT operation:  \n" + event.toPrettyString());
            return ValidationResult.skip();
        }

        if (op == Operation.DELETE && currentRevision == null) {
            logger.info("Revision does not exist to DELETE:  \n" + event.toPrettyString());
            return strictMode ? ValidationResult.fail("Revision does not exist to DELETE:  \n" + event.toPrettyString()) : ValidationResult.skip();
        }

        if (op == Operation.UPDATE && currentRevision == null) {
            logger.info("Revision does not exist for UPDATE:  \n" + event.toPrettyString());
            return strictMode ? ValidationResult.fail("Revision does not exist for UPDATE:  \n" + event.toPrettyString()) : ValidationResult.skip();
        }

        /*
         * Regardless of the operation, we want to make sure we're processing revisions in the correct
         * order to preserve the event ordering.  We wouldn't want to process a delete if there are more updates for
         * that document coming, for example.
         */
        if (event.getVersion() > -1 && currentRevision != null) {
            // This depends on readCurrentVersion() reading from the committed view
            IonStruct metadata = (IonStruct) currentRevision.get("metadata");
            int currentVersion = ((IonInt) metadata.get("version")).intValue();

            if (event.getVersion() <= currentVersion) {
                logger.info("Revision " + event.getVersion() + " is not greater than current version " + currentVersion + ":  \n" + event.toPrettyString());
                return ValidationResult.skip();
            }

            if (event.getVersion() != (currentVersion + 1))
                return ValidationResult.fail("Out of order revision received.  Current version = " + currentVersion + "\n" + event.toPrettyString());
        }

        return ValidationResult.pass();
    }


    /**
     * Writes the new revision from the load event into the ledger.  This method assumes the event has already been
     * validated.
     *
     * @param txn An open ledger transaction
     * @param event The load event containing the new document revision, its ledger table name, and its identifier
     * @param currentRevision The current revision, if it exists, of the document to be written
     */
    protected void writeDocument(TransactionExecutor txn, LoadEvent event, IonStruct currentRevision) {
        if (txn == null || event == null || event.getTableName() == null)
            return;

        switch (event.getOperation()) {
            case INSERT:
                txn.execute("INSERT INTO " + event.getTableName() + " VALUE ?", event.getRevision());
                break;
            case UPDATE:
                if (currentRevision != null)
                    txn.execute("UPDATE " + event.getTableName() + " AS doc BY rid SET doc = ? WHERE rid = ?",
                        event.getRevision(), getDocumentId(currentRevision));
                break;
            case DELETE:
                if (currentRevision != null)
                    txn.execute("DELETE FROM " + event.getTableName() + " BY rid WHERE rid = ?", getDocumentId(currentRevision));
                break;
            case ANY:
                if (currentRevision == null)
                    txn.execute("INSERT INTO " + event.getTableName() + " VALUE ?", event.getRevision());
                else if (event.getRevision() == null)
                    txn.execute("DELETE FROM " + event.getTableName() + " BY rid WHERE rid = ?", getDocumentId(currentRevision));
                else
                    txn.execute("UPDATE " + event.getTableName() + " AS doc BY rid SET doc = ? WHERE rid = ?",
                            event.getRevision(), getDocumentId(currentRevision));
                break;
        }
    }


    private IonValue getDocumentId(IonStruct revision) {
        IonStruct metadata = (IonStruct) revision.get("metadata");
        return metadata.get("id");
    }


    public static RevisionWriterBuilder builder() {
        return BaseRevisionWriterBuilder.builder();
    }


    public static class BaseRevisionWriterBuilder implements RevisionWriterBuilder {
        private QldbDriver driver;
        private boolean strictMode = true;


        @Override
        public BaseRevisionWriterBuilder qldbDriver(QldbDriver driver) {
            this.driver = driver;
            return this;
        }


        public BaseRevisionWriterBuilder strictMode(boolean strictMode) {
            this.strictMode = strictMode;
            return this;
        }


        @Override
        public BaseRevisionWriterBuilder configureFromEnvironment() {
            if (System.getenv().containsKey("STRICT_MODE")) {
                strictMode = Boolean.parseBoolean(System.getenv("STRICT_MODE"));
            }

            return this;
        }


        @Override
        public RevisionWriter build() {
            BaseRevisionWriter writer = new BaseRevisionWriter(driver, strictMode);
            writer.initialize();
            return writer;
        }


        public static BaseRevisionWriterBuilder builder() {
            return new BaseRevisionWriterBuilder();
        }
    }
}
