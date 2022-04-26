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
import com.amazon.ion.IonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.qldb.QldbDriver;
import software.amazon.qldb.Result;
import software.amazon.qldb.TransactionExecutor;
import software.amazon.qldb.load.LoadEvent;

import java.io.InputStream;
import java.util.Iterator;
import java.util.Properties;


/**
 * A specialization of {@link software.amazon.qldb.load.writer.BaseRevisionWriter} that performs idempotence checks
 * against the target ledger using fields in the document data using a mapping of the table name in the load event
 * to a document field for loading the current revision.  The mapping of table names to ID field is done in a
 * properties-style file called table-map.properties in the root of the classpath.
 */
public class TableMapperRevisionWriter extends BaseRevisionWriter {
    private static final Logger logger = LoggerFactory.getLogger(TableMapperRevisionWriter.class);
    private final Properties mapping = new Properties();


    public TableMapperRevisionWriter(QldbDriver driver, boolean strictMode) {
        super(driver, strictMode);

        try (InputStream in = getClass().getResourceAsStream("/table-map.properties")) {
            mapping.load(in);
        } catch (Exception e) {
            throw new RuntimeException("Unable to load table map file", e);
        }
    }



    /**
     * Looks for a current record in the target ledger for the given event using an identifier field in the
     * document itself based on the table the event is for.
     */
    @Override
    protected IonStruct readCurrentRevision(TransactionExecutor txn, LoadEvent event) {
        if (txn == null || event == null || event.getTableName() == null || event.getId() == null)
            return null;

        String idField = mapping.getProperty(event.getTableName());
        if (idField == null)
            idField = mapping.getProperty("*");

        if (idField == null) {
            logger.warn("No mapping for table " + event.getTableName() + ".  Skipping event " + event.toPrettyString());
            return null;
        }

        Result result = txn.execute("SELECT * FROM _ql_committed_" + event.getTableName() + " WHERE data." + idField + " = ?", event.getId());

        Iterator<IonValue> iter = result.iterator();
        return iter.hasNext() ? (IonStruct) iter.next() : null;
    }


    @Override
    protected void adjustRevision(TransactionExecutor txn, LoadEvent event, IonStruct currentRevision) {
        // NO-OP
    }


    @Override
    protected ValidationResult validate(TransactionExecutor txn, LoadEvent event, IonStruct currentRevision) {
        if (!(mapping.containsKey(event.getTableName()) || mapping.containsKey("*")))
            return ValidationResult.skip("Unknown table " + event.getTableName() + ".  Skipping");

        return super.validate(txn, event, currentRevision);
    }
}