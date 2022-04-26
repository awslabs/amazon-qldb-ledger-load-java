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
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;
import com.amazon.ion.system.IonSystemBuilder;
import org.junit.jupiter.api.Test;
import software.amazon.qldb.*;
import software.amazon.qldb.load.LoadEvent;
import software.amazon.qldb.load.Operation;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class BaseRevisionWriterTest {
    private final static IonSystem ION_SYSTEM = IonSystemBuilder.standard().build();

    @Test
    public void testStrictModeIsDefault() {
        BaseRevisionWriter writer = BaseRevisionWriter.builder().qldbDriver(new MockNoOpQldbDriver()).build();
        assertTrue(writer.isStrictMode());
    }

    @Test
    public void testReadCurrentVersionNullTransaction() {
        LoadEvent event = new LoadEvent();
        event.setOperation(Operation.INSERT);
        event.setVersion(200);
        event.setId(ION_SYSTEM.newString("12345"));
        event.setTableName("Person");

        IonStruct revision = ION_SYSTEM.newEmptyStruct();
        revision.put("foo").newString("bar");
        revision.put("test").newInt(10);
        event.setRevision(revision);

        BaseRevisionWriter writer = BaseRevisionWriter.builder().qldbDriver(new MockNoOpQldbDriver()).build();
        assertNull(writer.readCurrentRevision(null, event));
    }

    @Test
    public void testReadCurrentVersionNullEvent() {
        BaseRevisionWriter writer = BaseRevisionWriter.builder().qldbDriver(new MockNoOpQldbDriver()).build();
        assertNull(writer.readCurrentRevision(new MockTransactionExecutor(), null));
    }

    @Test
    public void testReadCurrentVersionNullTable() {
        LoadEvent event = new LoadEvent();
        event.setOperation(Operation.INSERT);
        event.setVersion(200);
        event.setId(ION_SYSTEM.newString("12345"));

        IonStruct revision = ION_SYSTEM.newEmptyStruct();
        revision.put("foo").newString("bar");
        revision.put("test").newInt(10);
        event.setRevision(revision);

        BaseRevisionWriter writer = BaseRevisionWriter.builder().qldbDriver(new MockNoOpQldbDriver()).build();
        assertNull(writer.readCurrentRevision(new MockTransactionExecutor(), event));
    }

    @Test
    public void testReadCurrentVersionNullId() {
        LoadEvent event = new LoadEvent();
        event.setOperation(Operation.INSERT);
        event.setVersion(200);
        event.setTableName("Person");

        IonStruct revision = ION_SYSTEM.newEmptyStruct();
        revision.put("foo").newString("bar");
        revision.put("test").newInt(10);
        event.setRevision(revision);

        BaseRevisionWriter writer = BaseRevisionWriter.builder().qldbDriver(new MockNoOpQldbDriver()).build();
        assertNull(writer.readCurrentRevision(new MockTransactionExecutor(), event));
    }

    @Test
    public void testAdjustRevisionNullEvent() {
        assertDoesNotThrow(() -> {
            BaseRevisionWriter writer = BaseRevisionWriter.builder().qldbDriver(new MockNoOpQldbDriver()).build();
            writer.adjustRevision(null, null, null);
        });
    }

    @Test
    public void testAdjustRevisionNullEventId() {
        assertDoesNotThrow(() -> {
            LoadEvent event = new LoadEvent();
            BaseRevisionWriter writer = BaseRevisionWriter.builder().qldbDriver(new MockNoOpQldbDriver()).build();
            writer.adjustRevision(null, event, null);
        });
    }

    @Test
    public void testAdjustRevisionValid() {
        LoadEvent event = new LoadEvent();
        event.setId(ION_SYSTEM.newString("9999999"));

        IonStruct revision = ION_SYSTEM.newEmptyStruct();
        revision.put("foo").newString("bar");
        revision.put("test").newInt(10);
        event.setRevision(revision);

        BaseRevisionWriter writer = BaseRevisionWriter.builder().qldbDriver(new MockNoOpQldbDriver()).build();
        writer.adjustRevision(null, event, null);

        assertTrue(revision.containsKey("oldDocumentId"));
        assertEquals(ION_SYSTEM.newString("9999999"), revision.get("oldDocumentId"));
    }

    @Test
    public void testWriteDocumentInsert() {
        BaseRevisionWriter writer = BaseRevisionWriter.builder().qldbDriver(new MockNoOpQldbDriver()).build();
        MockTransactionExecutor txn = new MockTransactionExecutor();

        LoadEvent event = new LoadEvent();
        event.setId(ION_SYSTEM.newString("9999999"));
        event.setOperation(Operation.INSERT);
        event.setTableName("Person");

        IonStruct revision = ION_SYSTEM.newEmptyStruct();
        revision.put("foo").newString("bar");
        revision.put("test").newInt(10);
        event.setRevision(revision);

        writer.writeDocument(txn, event, null);
        assertNotNull(txn.query);
        assertTrue(txn.query.toLowerCase().startsWith("insert"));
        assertNotNull(txn.parameters);
        assertEquals(1, txn.parameters.size());
        assertEquals(revision, txn.parameters.get(0));
    }

    @Test
    public void testWriteDocumentUpdate() {
        BaseRevisionWriter writer = BaseRevisionWriter.builder().qldbDriver(new MockNoOpQldbDriver()).build();
        MockTransactionExecutor txn = new MockTransactionExecutor();

        LoadEvent event = new LoadEvent();
        event.setId(ION_SYSTEM.newString("9999999"));
        event.setOperation(Operation.UPDATE);
        event.setTableName("Person");

        IonStruct revision = ION_SYSTEM.newEmptyStruct();
        revision.put("foo").newString("bar");
        revision.put("test").newInt(10);
        event.setRevision(revision);

        IonStruct currentRevision = ION_SYSTEM.newEmptyStruct();
        IonStruct metadata = ION_SYSTEM.newEmptyStruct();
        metadata.put("id").newString("888888");
        metadata.put("version").newInt(0);
        currentRevision.put("metadata", metadata);

        writer.writeDocument(txn, event, currentRevision);
        assertNotNull(txn.query);
        assertTrue(txn.query.toLowerCase().startsWith("update"));
        assertNotNull(txn.parameters);
        assertEquals(2, txn.parameters.size());
        assertEquals(ION_SYSTEM.newString("888888"), txn.parameters.get(1));
        assertEquals(revision, txn.parameters.get(0));
    }

    @Test
    public void testWriteDocumentDelete() {
        BaseRevisionWriter writer = BaseRevisionWriter.builder().qldbDriver(new MockNoOpQldbDriver()).build();
        MockTransactionExecutor txn = new MockTransactionExecutor();

        LoadEvent event = new LoadEvent();
        event.setId(ION_SYSTEM.newString("9999999"));
        event.setOperation(Operation.DELETE);
        event.setTableName("Person");

        IonStruct revision = ION_SYSTEM.newEmptyStruct();
        revision.put("foo").newString("bar");
        revision.put("test").newInt(10);
        event.setRevision(revision);

        IonStruct currentRevision = ION_SYSTEM.newEmptyStruct();
        IonStruct metadata = ION_SYSTEM.newEmptyStruct();
        metadata.put("id").newString("888888");
        metadata.put("version").newInt(0);
        currentRevision.put("metadata", metadata);

        writer.writeDocument(txn, event, currentRevision);
        assertNotNull(txn.query);
        assertTrue(txn.query.toLowerCase().startsWith("delete"));
        assertNotNull(txn.parameters);
        assertEquals(1, txn.parameters.size());
        assertEquals(ION_SYSTEM.newString("888888"), txn.parameters.get(0));
    }

    @Test
    public void testWriteDocumentInsertAny() {
        BaseRevisionWriter writer = BaseRevisionWriter.builder().qldbDriver(new MockNoOpQldbDriver()).build();
        MockTransactionExecutor txn = new MockTransactionExecutor();

        LoadEvent event = new LoadEvent();
        event.setId(ION_SYSTEM.newString("9999999"));
        event.setOperation(Operation.ANY);
        event.setTableName("Person");

        IonStruct revision = ION_SYSTEM.newEmptyStruct();
        revision.put("foo").newString("bar");
        revision.put("test").newInt(10);
        event.setRevision(revision);

        writer.writeDocument(txn, event, null);
        assertNotNull(txn.query);
        assertTrue(txn.query.toLowerCase().startsWith("insert"));
        assertNotNull(txn.parameters);
        assertEquals(1, txn.parameters.size());
        assertEquals(revision, txn.parameters.get(0));
    }

    @Test
    public void testWriteDocumentUpdateAny() {
        BaseRevisionWriter writer = BaseRevisionWriter.builder().qldbDriver(new MockNoOpQldbDriver()).build();
        MockTransactionExecutor txn = new MockTransactionExecutor();

        LoadEvent event = new LoadEvent();
        event.setId(ION_SYSTEM.newString("9999999"));
        event.setOperation(Operation.ANY);
        event.setTableName("Person");

        IonStruct revision = ION_SYSTEM.newEmptyStruct();
        revision.put("foo").newString("bar");
        revision.put("test").newInt(10);
        event.setRevision(revision);

        IonStruct currentRevision = ION_SYSTEM.newEmptyStruct();
        IonStruct metadata = ION_SYSTEM.newEmptyStruct();
        metadata.put("id").newString("888888");
        metadata.put("version").newInt(0);
        currentRevision.put("metadata", metadata);

        writer.writeDocument(txn, event, currentRevision);
        assertNotNull(txn.query);
        assertTrue(txn.query.toLowerCase().startsWith("update"));
        assertNotNull(txn.parameters);
        assertEquals(2, txn.parameters.size());
        assertEquals(ION_SYSTEM.newString("888888"), txn.parameters.get(1));
        assertEquals(revision, txn.parameters.get(0));
    }

    @Test
    public void testWriteDocumentDeleteAny() {
        BaseRevisionWriter writer = BaseRevisionWriter.builder().qldbDriver(new MockNoOpQldbDriver()).build();
        MockTransactionExecutor txn = new MockTransactionExecutor();

        LoadEvent event = new LoadEvent();
        event.setId(ION_SYSTEM.newString("9999999"));
        event.setOperation(Operation.ANY);
        event.setTableName("Person");

        IonStruct currentRevision = ION_SYSTEM.newEmptyStruct();
        IonStruct metadata = ION_SYSTEM.newEmptyStruct();
        metadata.put("id").newString("888888");
        metadata.put("version").newInt(0);
        currentRevision.put("metadata", metadata);

        writer.writeDocument(txn, event, currentRevision);
        assertNotNull(txn.query);
        assertTrue(txn.query.toLowerCase().startsWith("delete"));
        assertNotNull(txn.parameters);
        assertEquals(1, txn.parameters.size());
        assertEquals(ION_SYSTEM.newString("888888"), txn.parameters.get(0));
    }

    @Test
    public void testValidateStrictInsertWithExistingRevision() {
        BaseRevisionWriter writer = BaseRevisionWriter.builder()
                .qldbDriver(new MockNoOpQldbDriver())
                .strictMode(true)
                .build();

        MockTransactionExecutor txn = new MockTransactionExecutor();

        LoadEvent event = new LoadEvent();
        event.setOperation(Operation.INSERT);
        event.setTableName("Person");
        event.setId(ION_SYSTEM.newString("12345"));
        event.setVersion(0);

        IonStruct currentRevision = ION_SYSTEM.newEmptyStruct();
        IonStruct metadata = ION_SYSTEM.newEmptyStruct();
        metadata.put("id").newString("888888");
        metadata.put("version").newInt(0);
        currentRevision.put("metadata", metadata);

        ValidationResult result = writer.validate(txn, event, currentRevision);
        assertNotNull(result);
        assertTrue(result.skip);
        assertFalse(result.fail);
    }

    @Test
    public void testValidateStrictInsertValid() {
        BaseRevisionWriter writer = BaseRevisionWriter.builder()
                .qldbDriver(new MockNoOpQldbDriver())
                .strictMode(true)
                .build();
        MockTransactionExecutor txn = new MockTransactionExecutor();

        LoadEvent event = new LoadEvent();
        event.setOperation(Operation.INSERT);
        event.setTableName("Person");
        event.setId(ION_SYSTEM.newString("12345"));
        event.setVersion(0);

        IonStruct revision = ION_SYSTEM.newEmptyStruct();
        revision.put("foo").newString("bar");
        revision.put("test").newInt(10);
        event.setRevision(revision);

        ValidationResult result = writer.validate(txn, event, null);
        assertNotNull(result);
        assertFalse(result.skip);
        assertFalse(result.fail);
    }

    @Test
    public void testValidateStrictInsertValidVersionIgnored() {
        BaseRevisionWriter writer = BaseRevisionWriter.builder()
                .qldbDriver(new MockNoOpQldbDriver())
                .strictMode(true)
                .build();
        MockTransactionExecutor txn = new MockTransactionExecutor();

        LoadEvent event = new LoadEvent();
        event.setOperation(Operation.INSERT);
        event.setTableName("Person");
        event.setId(ION_SYSTEM.newString("12345"));
        event.setVersion(100);

        IonStruct revision = ION_SYSTEM.newEmptyStruct();
        revision.put("foo").newString("bar");
        revision.put("test").newInt(10);
        event.setRevision(revision);

        ValidationResult result = writer.validate(txn, event, null);
        assertNotNull(result);
        assertFalse(result.skip);
        assertFalse(result.fail);
    }

    @Test
    public void testValidateNonStrictInsertWithExistingRevision() {
        BaseRevisionWriter writer = BaseRevisionWriter.builder()
                .qldbDriver(new MockNoOpQldbDriver())
                .strictMode(false)
                .build();
        MockTransactionExecutor txn = new MockTransactionExecutor();

        LoadEvent event = new LoadEvent();
        event.setOperation(Operation.INSERT);
        event.setTableName("Person");
        event.setId(ION_SYSTEM.newString("12345"));
        event.setVersion(0);

        IonStruct currentRevision = ION_SYSTEM.newEmptyStruct();
        IonStruct metadata = ION_SYSTEM.newEmptyStruct();
        metadata.put("id").newString("888888");
        metadata.put("version").newInt(0);
        currentRevision.put("metadata", metadata);

        ValidationResult result = writer.validate(txn, event, currentRevision);
        assertNotNull(result);
        assertTrue(result.skip);
        assertFalse(result.fail);
    }

    @Test
    public void testValidateNonStrictInsertValid() {
        BaseRevisionWriter writer = BaseRevisionWriter.builder()
                .qldbDriver(new MockNoOpQldbDriver())
                .strictMode(false)
                .build();
        MockTransactionExecutor txn = new MockTransactionExecutor();

        LoadEvent event = new LoadEvent();
        event.setOperation(Operation.INSERT);
        event.setTableName("Person");
        event.setId(ION_SYSTEM.newString("12345"));
        event.setVersion(0);

        IonStruct revision = ION_SYSTEM.newEmptyStruct();
        revision.put("foo").newString("bar");
        revision.put("test").newInt(10);
        event.setRevision(revision);

        ValidationResult result = writer.validate(txn, event, null);
        assertNotNull(result);
        assertFalse(result.skip);
        assertFalse(result.fail);
    }

    @Test
    public void testValidateNonStrictInsertValidVersionIgnored() {
        BaseRevisionWriter writer = BaseRevisionWriter.builder()
                .qldbDriver(new MockNoOpQldbDriver())
                .strictMode(false)
                .build();
        MockTransactionExecutor txn = new MockTransactionExecutor();

        LoadEvent event = new LoadEvent();
        event.setOperation(Operation.INSERT);
        event.setTableName("Person");
        event.setId(ION_SYSTEM.newString("12345"));
        event.setVersion(100);

        IonStruct revision = ION_SYSTEM.newEmptyStruct();
        revision.put("foo").newString("bar");
        revision.put("test").newInt(10);
        event.setRevision(revision);

        ValidationResult result = writer.validate(txn, event, null);
        assertNotNull(result);
        assertFalse(result.skip);
        assertFalse(result.fail);
    }

    @Test
    public void testValidateUpdateStrictNoCurrentRevision() {
        BaseRevisionWriter writer = BaseRevisionWriter.builder()
                .qldbDriver(new MockNoOpQldbDriver())
                .strictMode(true)
                .build();
        MockTransactionExecutor txn = new MockTransactionExecutor();

        LoadEvent event = new LoadEvent();
        event.setOperation(Operation.UPDATE);
        event.setTableName("Person");
        event.setId(ION_SYSTEM.newString("12345"));
        event.setVersion(1);

        IonStruct revision = ION_SYSTEM.newEmptyStruct();
        revision.put("foo").newString("bar");
        revision.put("test").newInt(10);
        event.setRevision(revision);

        ValidationResult result = writer.validate(txn, event, null);
        assertNotNull(result);
        assertFalse(result.skip);
        assertTrue(result.fail);
    }

    @Test
    public void testValidateUpdateStrictVersionZero() {
        BaseRevisionWriter writer = BaseRevisionWriter.builder()
                .qldbDriver(new MockNoOpQldbDriver())
                .strictMode(true)
                .build();
        MockTransactionExecutor txn = new MockTransactionExecutor();

        LoadEvent event = new LoadEvent();
        event.setOperation(Operation.UPDATE);
        event.setTableName("Person");
        event.setId(ION_SYSTEM.newString("12345"));
        event.setVersion(0);

        IonStruct revision = ION_SYSTEM.newEmptyStruct();
        revision.put("foo").newString("bar");
        revision.put("test").newInt(10);
        event.setRevision(revision);

        IonStruct currentRevision = ION_SYSTEM.newEmptyStruct();
        IonStruct metadata = ION_SYSTEM.newEmptyStruct();
        metadata.put("id").newString("888888");
        metadata.put("version").newInt(0);
        currentRevision.put("metadata", metadata);

        IonStruct data = ION_SYSTEM.newEmptyStruct();
        data.put("foo").newString("old");
        data.put("test").newInt(100);
        currentRevision.put("data", data);

        ValidationResult result = writer.validate(txn, event, currentRevision);
        assertNotNull(result);
        assertFalse(result.fail);
        assertTrue(result.skip);
    }

    @Test
    public void testValidateUpdateStrictVersionTooHigh() {
        BaseRevisionWriter writer = BaseRevisionWriter.builder()
                .qldbDriver(new MockNoOpQldbDriver())
                .strictMode(true)
                .build();
        MockTransactionExecutor txn = new MockTransactionExecutor();

        LoadEvent event = new LoadEvent();
        event.setOperation(Operation.UPDATE);
        event.setTableName("Person");
        event.setId(ION_SYSTEM.newString("12345"));
        event.setVersion(2);

        IonStruct revision = ION_SYSTEM.newEmptyStruct();
        revision.put("foo").newString("bar");
        revision.put("test").newInt(10);
        event.setRevision(revision);

        IonStruct currentRevision = ION_SYSTEM.newEmptyStruct();
        IonStruct metadata = ION_SYSTEM.newEmptyStruct();
        metadata.put("id").newString("888888");
        metadata.put("version").newInt(0);
        currentRevision.put("metadata", metadata);

        IonStruct data = ION_SYSTEM.newEmptyStruct();
        data.put("foo").newString("old");
        data.put("test").newInt(100);
        currentRevision.put("data", data);

        ValidationResult result = writer.validate(txn, event, currentRevision);
        assertNotNull(result);
        assertFalse(result.skip);
        assertTrue(result.fail);
    }

    @Test
    public void testValidateUpdateStrictVersionTooLow() {
        BaseRevisionWriter writer = BaseRevisionWriter.builder()
                .qldbDriver(new MockNoOpQldbDriver())
                .strictMode(true)
                .build();
        MockTransactionExecutor txn = new MockTransactionExecutor();

        LoadEvent event = new LoadEvent();
        event.setOperation(Operation.UPDATE);
        event.setTableName("Person");
        event.setId(ION_SYSTEM.newString("12345"));
        event.setVersion(1);

        IonStruct revision = ION_SYSTEM.newEmptyStruct();
        revision.put("foo").newString("bar");
        revision.put("test").newInt(10);
        event.setRevision(revision);

        IonStruct currentRevision = ION_SYSTEM.newEmptyStruct();
        IonStruct metadata = ION_SYSTEM.newEmptyStruct();
        metadata.put("id").newString("888888");
        metadata.put("version").newInt(2);
        currentRevision.put("metadata", metadata);

        IonStruct data = ION_SYSTEM.newEmptyStruct();
        data.put("foo").newString("old");
        data.put("test").newInt(100);
        currentRevision.put("data", data);

        ValidationResult result = writer.validate(txn, event, currentRevision);
        assertNotNull(result);
        assertFalse(result.fail);
        assertTrue(result.skip);
    }

    @Test
    public void testValidateUpdateStrictValid() {
        BaseRevisionWriter writer = BaseRevisionWriter.builder()
                .qldbDriver(new MockNoOpQldbDriver())
                .strictMode(true)
                .build();
        MockTransactionExecutor txn = new MockTransactionExecutor();

        LoadEvent event = new LoadEvent();
        event.setOperation(Operation.UPDATE);
        event.setTableName("Person");
        event.setId(ION_SYSTEM.newString("12345"));
        event.setVersion(1);

        IonStruct revision = ION_SYSTEM.newEmptyStruct();
        revision.put("foo").newString("bar");
        revision.put("test").newInt(10);
        event.setRevision(revision);

        IonStruct currentRevision = ION_SYSTEM.newEmptyStruct();
        IonStruct metadata = ION_SYSTEM.newEmptyStruct();
        metadata.put("id").newString("888888");
        metadata.put("version").newInt(0);
        currentRevision.put("metadata", metadata);

        IonStruct data = ION_SYSTEM.newEmptyStruct();
        data.put("foo").newString("old");
        data.put("test").newInt(100);
        currentRevision.put("data", data);

        ValidationResult result = writer.validate(txn, event, currentRevision);
        assertNotNull(result);
        assertFalse(result.skip);
        assertFalse(result.fail);
    }

    @Test
    public void testValidateUpdateNonStrictNoCurrentRevision() {
        BaseRevisionWriter writer = BaseRevisionWriter.builder()
                .qldbDriver(new MockNoOpQldbDriver())
                .strictMode(false)
                .build();
        MockTransactionExecutor txn = new MockTransactionExecutor();

        LoadEvent event = new LoadEvent();
        event.setOperation(Operation.UPDATE);
        event.setTableName("Person");
        event.setId(ION_SYSTEM.newString("12345"));
        event.setVersion(1);

        IonStruct revision = ION_SYSTEM.newEmptyStruct();
        revision.put("foo").newString("bar");
        revision.put("test").newInt(10);
        event.setRevision(revision);

        ValidationResult result = writer.validate(txn, event, null);
        assertNotNull(result);
        assertTrue(result.skip);
        assertFalse(result.fail);
    }

    @Test
    public void testValidateUpdateNonStrictVersionZero() {
        BaseRevisionWriter writer = BaseRevisionWriter.builder()
                .qldbDriver(new MockNoOpQldbDriver())
                .strictMode(false)
                .build();
        MockTransactionExecutor txn = new MockTransactionExecutor();

        LoadEvent event = new LoadEvent();
        event.setOperation(Operation.UPDATE);
        event.setTableName("Person");
        event.setId(ION_SYSTEM.newString("12345"));
        event.setVersion(0);

        IonStruct revision = ION_SYSTEM.newEmptyStruct();
        revision.put("foo").newString("bar");
        revision.put("test").newInt(10);
        event.setRevision(revision);

        IonStruct currentRevision = ION_SYSTEM.newEmptyStruct();
        IonStruct metadata = ION_SYSTEM.newEmptyStruct();
        metadata.put("id").newString("888888");
        metadata.put("version").newInt(0);
        currentRevision.put("metadata", metadata);

        IonStruct data = ION_SYSTEM.newEmptyStruct();
        data.put("foo").newString("old");
        data.put("test").newInt(100);
        currentRevision.put("data", data);

        ValidationResult result = writer.validate(txn, event, currentRevision);
        assertNotNull(result);
        assertTrue(result.skip);
        assertFalse(result.fail);
    }

    @Test
    public void testValidateUpdateNonStrictVersionTooHigh() {
        BaseRevisionWriter writer = BaseRevisionWriter.builder()
                .qldbDriver(new MockNoOpQldbDriver())
                .strictMode(false)
                .build();
        MockTransactionExecutor txn = new MockTransactionExecutor();

        LoadEvent event = new LoadEvent();
        event.setOperation(Operation.UPDATE);
        event.setTableName("Person");
        event.setId(ION_SYSTEM.newString("12345"));
        event.setVersion(2);

        IonStruct revision = ION_SYSTEM.newEmptyStruct();
        revision.put("foo").newString("bar");
        revision.put("test").newInt(10);
        event.setRevision(revision);

        IonStruct currentRevision = ION_SYSTEM.newEmptyStruct();
        IonStruct metadata = ION_SYSTEM.newEmptyStruct();
        metadata.put("id").newString("888888");
        metadata.put("version").newInt(0);
        currentRevision.put("metadata", metadata);

        IonStruct data = ION_SYSTEM.newEmptyStruct();
        data.put("foo").newString("old");
        data.put("test").newInt(100);
        currentRevision.put("data", data);

        ValidationResult result = writer.validate(txn, event, currentRevision);
        assertNotNull(result);
        assertFalse(result.skip);
        assertTrue(result.fail);
    }

    @Test
    public void testValidateUpdateNonStrictVersionTooLow() {
        BaseRevisionWriter writer = BaseRevisionWriter.builder()
                .qldbDriver(new MockNoOpQldbDriver())
                .strictMode(false)
                .build();
        MockTransactionExecutor txn = new MockTransactionExecutor();

        LoadEvent event = new LoadEvent();
        event.setOperation(Operation.UPDATE);
        event.setTableName("Person");
        event.setId(ION_SYSTEM.newString("12345"));
        event.setVersion(1);

        IonStruct revision = ION_SYSTEM.newEmptyStruct();
        revision.put("foo").newString("bar");
        revision.put("test").newInt(10);
        event.setRevision(revision);

        IonStruct currentRevision = ION_SYSTEM.newEmptyStruct();
        IonStruct metadata = ION_SYSTEM.newEmptyStruct();
        metadata.put("id").newString("888888");
        metadata.put("version").newInt(2);
        currentRevision.put("metadata", metadata);

        IonStruct data = ION_SYSTEM.newEmptyStruct();
        data.put("foo").newString("old");
        data.put("test").newInt(100);
        currentRevision.put("data", data);

        ValidationResult result = writer.validate(txn, event, currentRevision);
        assertNotNull(result);
        assertTrue(result.skip);
        assertFalse(result.fail);
    }

    @Test
    public void testValidateUpdateNonStrictValid() {
        BaseRevisionWriter writer = BaseRevisionWriter.builder()
                .qldbDriver(new MockNoOpQldbDriver())
                .strictMode(false)
                .build();
        MockTransactionExecutor txn = new MockTransactionExecutor();

        LoadEvent event = new LoadEvent();
        event.setOperation(Operation.UPDATE);
        event.setTableName("Person");
        event.setId(ION_SYSTEM.newString("12345"));
        event.setVersion(1);

        IonStruct revision = ION_SYSTEM.newEmptyStruct();
        revision.put("foo").newString("bar");
        revision.put("test").newInt(10);
        event.setRevision(revision);

        IonStruct currentRevision = ION_SYSTEM.newEmptyStruct();
        IonStruct metadata = ION_SYSTEM.newEmptyStruct();
        metadata.put("id").newString("888888");
        metadata.put("version").newInt(0);
        currentRevision.put("metadata", metadata);

        IonStruct data = ION_SYSTEM.newEmptyStruct();
        data.put("foo").newString("old");
        data.put("test").newInt(100);
        currentRevision.put("data", data);

        ValidationResult result = writer.validate(txn, event, currentRevision);
        assertNotNull(result);
        assertFalse(result.skip);
        assertFalse(result.fail);
    }

    @Test
    public void testValidateDeleteStrictNoCurrentRevision() {
        BaseRevisionWriter writer = BaseRevisionWriter.builder()
                .qldbDriver(new MockNoOpQldbDriver())
                .strictMode(true)
                .build();
        MockTransactionExecutor txn = new MockTransactionExecutor();

        LoadEvent event = new LoadEvent();
        event.setOperation(Operation.DELETE);
        event.setTableName("Person");
        event.setId(ION_SYSTEM.newString("12345"));
        event.setVersion(1);

        IonStruct revision = ION_SYSTEM.newEmptyStruct();
        revision.put("foo").newString("bar");
        revision.put("test").newInt(10);
        event.setRevision(revision);

        ValidationResult result = writer.validate(txn, event, null);
        assertNotNull(result);
        assertFalse(result.skip);
        assertTrue(result.fail);
    }

    @Test
    public void testValidateDeleteStrictVersionTooLow() {
        BaseRevisionWriter writer = BaseRevisionWriter.builder()
                .qldbDriver(new MockNoOpQldbDriver())
                .strictMode(true)
                .build();
        MockTransactionExecutor txn = new MockTransactionExecutor();

        LoadEvent event = new LoadEvent();
        event.setOperation(Operation.DELETE);
        event.setTableName("Person");
        event.setId(ION_SYSTEM.newString("12345"));
        event.setVersion(1);

        IonStruct revision = ION_SYSTEM.newEmptyStruct();
        revision.put("foo").newString("bar");
        revision.put("test").newInt(10);
        event.setRevision(revision);

        IonStruct currentRevision = ION_SYSTEM.newEmptyStruct();
        IonStruct metadata = ION_SYSTEM.newEmptyStruct();
        metadata.put("id").newString("888888");
        metadata.put("version").newInt(10);
        currentRevision.put("metadata", metadata);

        IonStruct data = ION_SYSTEM.newEmptyStruct();
        data.put("foo").newString("old");
        data.put("test").newInt(100);
        currentRevision.put("data", data);

        ValidationResult result = writer.validate(txn, event, currentRevision);
        assertNotNull(result);
        assertFalse(result.fail);
        assertTrue(result.skip);
    }

    @Test
    public void testValidateDeleteStrictVersionTooHigh() {
        BaseRevisionWriter writer = BaseRevisionWriter.builder()
                .qldbDriver(new MockNoOpQldbDriver())
                .strictMode(true)
                .build();
        MockTransactionExecutor txn = new MockTransactionExecutor();

        LoadEvent event = new LoadEvent();
        event.setOperation(Operation.DELETE);
        event.setTableName("Person");
        event.setId(ION_SYSTEM.newString("12345"));
        event.setVersion(10);

        IonStruct revision = ION_SYSTEM.newEmptyStruct();
        revision.put("foo").newString("bar");
        revision.put("test").newInt(10);
        event.setRevision(revision);

        IonStruct currentRevision = ION_SYSTEM.newEmptyStruct();
        IonStruct metadata = ION_SYSTEM.newEmptyStruct();
        metadata.put("id").newString("888888");
        metadata.put("version").newInt(1);
        currentRevision.put("metadata", metadata);

        IonStruct data = ION_SYSTEM.newEmptyStruct();
        data.put("foo").newString("old");
        data.put("test").newInt(100);
        currentRevision.put("data", data);

        ValidationResult result = writer.validate(txn, event, currentRevision);
        assertNotNull(result);
        assertFalse(result.skip);
        assertTrue(result.fail);
    }

    @Test
    public void testValidateDeleteStrictVersionSame() {
        BaseRevisionWriter writer = BaseRevisionWriter.builder()
                .qldbDriver(new MockNoOpQldbDriver())
                .strictMode(true)
                .build();
        MockTransactionExecutor txn = new MockTransactionExecutor();

        LoadEvent event = new LoadEvent();
        event.setOperation(Operation.DELETE);
        event.setTableName("Person");
        event.setId(ION_SYSTEM.newString("12345"));
        event.setVersion(1);

        IonStruct revision = ION_SYSTEM.newEmptyStruct();
        revision.put("foo").newString("bar");
        revision.put("test").newInt(10);
        event.setRevision(revision);

        IonStruct currentRevision = ION_SYSTEM.newEmptyStruct();
        IonStruct metadata = ION_SYSTEM.newEmptyStruct();
        metadata.put("id").newString("888888");
        metadata.put("version").newInt(1);
        currentRevision.put("metadata", metadata);

        IonStruct data = ION_SYSTEM.newEmptyStruct();
        data.put("foo").newString("old");
        data.put("test").newInt(100);
        currentRevision.put("data", data);

        ValidationResult result = writer.validate(txn, event, currentRevision);
        assertNotNull(result);
        assertFalse(result.fail);
        assertTrue(result.skip);
    }

    @Test
    public void testValidateDeleteStrictValid() {
        BaseRevisionWriter writer = BaseRevisionWriter.builder()
                .qldbDriver(new MockNoOpQldbDriver())
                .strictMode(true)
                .build();
        MockTransactionExecutor txn = new MockTransactionExecutor();

        LoadEvent event = new LoadEvent();
        event.setOperation(Operation.DELETE);
        event.setTableName("Person");
        event.setId(ION_SYSTEM.newString("12345"));
        event.setVersion(1);

        IonStruct revision = ION_SYSTEM.newEmptyStruct();
        revision.put("foo").newString("bar");
        revision.put("test").newInt(10);
        event.setRevision(revision);

        IonStruct currentRevision = ION_SYSTEM.newEmptyStruct();
        IonStruct metadata = ION_SYSTEM.newEmptyStruct();
        metadata.put("id").newString("888888");
        metadata.put("version").newInt(0);
        currentRevision.put("metadata", metadata);

        IonStruct data = ION_SYSTEM.newEmptyStruct();
        data.put("foo").newString("old");
        data.put("test").newInt(100);
        currentRevision.put("data", data);

        ValidationResult result = writer.validate(txn, event, currentRevision);
        assertNotNull(result);
        assertFalse(result.skip);
        assertFalse(result.fail);
    }

    @Test
    public void testValidateDeleteNonStrictNoCurrentRevision() {
        BaseRevisionWriter writer = BaseRevisionWriter.builder()
                .qldbDriver(new MockNoOpQldbDriver())
                .strictMode(false)
                .build();
        MockTransactionExecutor txn = new MockTransactionExecutor();

        LoadEvent event = new LoadEvent();
        event.setOperation(Operation.DELETE);
        event.setTableName("Person");
        event.setId(ION_SYSTEM.newString("12345"));
        event.setVersion(1);

        IonStruct revision = ION_SYSTEM.newEmptyStruct();
        revision.put("foo").newString("bar");
        revision.put("test").newInt(10);
        event.setRevision(revision);

        ValidationResult result = writer.validate(txn, event, null);
        assertNotNull(result);
        assertTrue(result.skip);
        assertFalse(result.fail);
    }

    @Test
    public void testValidateDeleteNonStrictVersionTooLow() {
        BaseRevisionWriter writer = BaseRevisionWriter.builder()
                .qldbDriver(new MockNoOpQldbDriver())
                .strictMode(false)
                .build();
        MockTransactionExecutor txn = new MockTransactionExecutor();

        LoadEvent event = new LoadEvent();
        event.setOperation(Operation.DELETE);
        event.setTableName("Person");
        event.setId(ION_SYSTEM.newString("12345"));
        event.setVersion(1);

        IonStruct revision = ION_SYSTEM.newEmptyStruct();
        revision.put("foo").newString("bar");
        revision.put("test").newInt(10);
        event.setRevision(revision);

        IonStruct currentRevision = ION_SYSTEM.newEmptyStruct();
        IonStruct metadata = ION_SYSTEM.newEmptyStruct();
        metadata.put("id").newString("888888");
        metadata.put("version").newInt(10);
        currentRevision.put("metadata", metadata);

        IonStruct data = ION_SYSTEM.newEmptyStruct();
        data.put("foo").newString("old");
        data.put("test").newInt(100);
        currentRevision.put("data", data);

        ValidationResult result = writer.validate(txn, event, currentRevision);
        assertNotNull(result);
        assertTrue(result.skip);
        assertFalse(result.fail);
    }

    @Test
    public void testValidateDeleteNonStrictVersionTooHigh() {
        BaseRevisionWriter writer = BaseRevisionWriter.builder()
                .qldbDriver(new MockNoOpQldbDriver())
                .strictMode(false)
                .build();
        MockTransactionExecutor txn = new MockTransactionExecutor();

        LoadEvent event = new LoadEvent();
        event.setOperation(Operation.DELETE);
        event.setTableName("Person");
        event.setId(ION_SYSTEM.newString("12345"));
        event.setVersion(10);

        IonStruct revision = ION_SYSTEM.newEmptyStruct();
        revision.put("foo").newString("bar");
        revision.put("test").newInt(10);
        event.setRevision(revision);

        IonStruct currentRevision = ION_SYSTEM.newEmptyStruct();
        IonStruct metadata = ION_SYSTEM.newEmptyStruct();
        metadata.put("id").newString("888888");
        metadata.put("version").newInt(1);
        currentRevision.put("metadata", metadata);

        IonStruct data = ION_SYSTEM.newEmptyStruct();
        data.put("foo").newString("old");
        data.put("test").newInt(100);
        currentRevision.put("data", data);

        ValidationResult result = writer.validate(txn, event, currentRevision);
        assertNotNull(result);
        assertFalse(result.skip);
        assertTrue(result.fail);
    }

    @Test
    public void testValidateDeleteNonStrictVersionSame() {
        BaseRevisionWriter writer = BaseRevisionWriter.builder()
                .qldbDriver(new MockNoOpQldbDriver())
                .strictMode(false)
                .build();
        MockTransactionExecutor txn = new MockTransactionExecutor();

        LoadEvent event = new LoadEvent();
        event.setOperation(Operation.DELETE);
        event.setTableName("Person");
        event.setId(ION_SYSTEM.newString("12345"));
        event.setVersion(1);

        IonStruct revision = ION_SYSTEM.newEmptyStruct();
        revision.put("foo").newString("bar");
        revision.put("test").newInt(10);
        event.setRevision(revision);

        IonStruct currentRevision = ION_SYSTEM.newEmptyStruct();
        IonStruct metadata = ION_SYSTEM.newEmptyStruct();
        metadata.put("id").newString("888888");
        metadata.put("version").newInt(1);
        currentRevision.put("metadata", metadata);

        IonStruct data = ION_SYSTEM.newEmptyStruct();
        data.put("foo").newString("old");
        data.put("test").newInt(100);
        currentRevision.put("data", data);

        ValidationResult result = writer.validate(txn, event, currentRevision);
        assertNotNull(result);
        assertTrue(result.skip);
        assertFalse(result.fail);
    }

    @Test
    public void testValidateDeleteNonStrictValid() {
        BaseRevisionWriter writer = BaseRevisionWriter.builder()
                .qldbDriver(new MockNoOpQldbDriver())
                .strictMode(false)
                .build();
        MockTransactionExecutor txn = new MockTransactionExecutor();

        LoadEvent event = new LoadEvent();
        event.setOperation(Operation.DELETE);
        event.setTableName("Person");
        event.setId(ION_SYSTEM.newString("12345"));
        event.setVersion(1);

        IonStruct revision = ION_SYSTEM.newEmptyStruct();
        revision.put("foo").newString("bar");
        revision.put("test").newInt(10);
        event.setRevision(revision);

        IonStruct currentRevision = ION_SYSTEM.newEmptyStruct();
        IonStruct metadata = ION_SYSTEM.newEmptyStruct();
        metadata.put("id").newString("888888");
        metadata.put("version").newInt(0);
        currentRevision.put("metadata", metadata);

        IonStruct data = ION_SYSTEM.newEmptyStruct();
        data.put("foo").newString("old");
        data.put("test").newInt(100);
        currentRevision.put("data", data);

        ValidationResult result = writer.validate(txn, event, currentRevision);
        assertNotNull(result);
        assertFalse(result.skip);
        assertFalse(result.fail);
    }

    @Test
    public void testValidateAnyStrictVersionTooLow() {
        BaseRevisionWriter writer = BaseRevisionWriter.builder()
                .qldbDriver(new MockNoOpQldbDriver())
                .strictMode(true)
                .build();
        MockTransactionExecutor txn = new MockTransactionExecutor();

        LoadEvent event = new LoadEvent();
        event.setOperation(Operation.ANY);
        event.setTableName("Person");
        event.setId(ION_SYSTEM.newString("12345"));
        event.setVersion(1);

        IonStruct revision = ION_SYSTEM.newEmptyStruct();
        revision.put("foo").newString("bar");
        revision.put("test").newInt(10);
        event.setRevision(revision);

        IonStruct currentRevision = ION_SYSTEM.newEmptyStruct();
        IonStruct metadata = ION_SYSTEM.newEmptyStruct();
        metadata.put("id").newString("888888");
        metadata.put("version").newInt(10);
        currentRevision.put("metadata", metadata);

        IonStruct data = ION_SYSTEM.newEmptyStruct();
        data.put("foo").newString("old");
        data.put("test").newInt(100);
        currentRevision.put("data", data);

        ValidationResult result = writer.validate(txn, event, currentRevision);
        assertNotNull(result);
        assertFalse(result.fail);
        assertTrue(result.skip);
    }

    @Test
    public void testValidateAnyStrictVersionTooHigh() {
        BaseRevisionWriter writer = BaseRevisionWriter.builder()
                .qldbDriver(new MockNoOpQldbDriver())
                .strictMode(true)
                .build();
        MockTransactionExecutor txn = new MockTransactionExecutor();

        LoadEvent event = new LoadEvent();
        event.setOperation(Operation.ANY);
        event.setTableName("Person");
        event.setId(ION_SYSTEM.newString("12345"));
        event.setVersion(10);

        IonStruct revision = ION_SYSTEM.newEmptyStruct();
        revision.put("foo").newString("bar");
        revision.put("test").newInt(10);
        event.setRevision(revision);

        IonStruct currentRevision = ION_SYSTEM.newEmptyStruct();
        IonStruct metadata = ION_SYSTEM.newEmptyStruct();
        metadata.put("id").newString("888888");
        metadata.put("version").newInt(1);
        currentRevision.put("metadata", metadata);

        IonStruct data = ION_SYSTEM.newEmptyStruct();
        data.put("foo").newString("old");
        data.put("test").newInt(100);
        currentRevision.put("data", data);

        ValidationResult result = writer.validate(txn, event, currentRevision);
        assertNotNull(result);
        assertFalse(result.skip);
        assertTrue(result.fail);
    }

    @Test
    public void testValidateAnyStrictVersionSame() {
        BaseRevisionWriter writer = BaseRevisionWriter.builder()
                .qldbDriver(new MockNoOpQldbDriver())
                .strictMode(true)
                .build();
        MockTransactionExecutor txn = new MockTransactionExecutor();

        LoadEvent event = new LoadEvent();
        event.setOperation(Operation.ANY);
        event.setTableName("Person");
        event.setId(ION_SYSTEM.newString("12345"));
        event.setVersion(1);

        IonStruct revision = ION_SYSTEM.newEmptyStruct();
        revision.put("foo").newString("bar");
        revision.put("test").newInt(10);
        event.setRevision(revision);

        IonStruct currentRevision = ION_SYSTEM.newEmptyStruct();
        IonStruct metadata = ION_SYSTEM.newEmptyStruct();
        metadata.put("id").newString("888888");
        metadata.put("version").newInt(1);
        currentRevision.put("metadata", metadata);

        IonStruct data = ION_SYSTEM.newEmptyStruct();
        data.put("foo").newString("old");
        data.put("test").newInt(100);
        currentRevision.put("data", data);

        ValidationResult result = writer.validate(txn, event, currentRevision);
        assertNotNull(result);
        assertFalse(result.fail);
        assertTrue(result.skip);
    }

    @Test
    public void testValidateAnyStrictValid() {
        BaseRevisionWriter writer = BaseRevisionWriter.builder()
                .qldbDriver(new MockNoOpQldbDriver())
                .strictMode(true)
                .build();
        MockTransactionExecutor txn = new MockTransactionExecutor();

        LoadEvent event = new LoadEvent();
        event.setOperation(Operation.ANY);
        event.setTableName("Person");
        event.setId(ION_SYSTEM.newString("12345"));
        event.setVersion(1);

        IonStruct revision = ION_SYSTEM.newEmptyStruct();
        revision.put("foo").newString("bar");
        revision.put("test").newInt(10);
        event.setRevision(revision);

        IonStruct currentRevision = ION_SYSTEM.newEmptyStruct();
        IonStruct metadata = ION_SYSTEM.newEmptyStruct();
        metadata.put("id").newString("888888");
        metadata.put("version").newInt(0);
        currentRevision.put("metadata", metadata);

        IonStruct data = ION_SYSTEM.newEmptyStruct();
        data.put("foo").newString("old");
        data.put("test").newInt(100);
        currentRevision.put("data", data);

        ValidationResult result = writer.validate(txn, event, currentRevision);
        assertNotNull(result);
        assertFalse(result.skip);
        assertFalse(result.fail);
    }

    @Test
    public void testValidateAnyNonStrictVersionTooLow() {
        BaseRevisionWriter writer = BaseRevisionWriter.builder()
                .qldbDriver(new MockNoOpQldbDriver())
                .strictMode(false)
                .build();
        MockTransactionExecutor txn = new MockTransactionExecutor();

        LoadEvent event = new LoadEvent();
        event.setOperation(Operation.ANY);
        event.setTableName("Person");
        event.setId(ION_SYSTEM.newString("12345"));
        event.setVersion(1);

        IonStruct revision = ION_SYSTEM.newEmptyStruct();
        revision.put("foo").newString("bar");
        revision.put("test").newInt(10);
        event.setRevision(revision);

        IonStruct currentRevision = ION_SYSTEM.newEmptyStruct();
        IonStruct metadata = ION_SYSTEM.newEmptyStruct();
        metadata.put("id").newString("888888");
        metadata.put("version").newInt(10);
        currentRevision.put("metadata", metadata);

        IonStruct data = ION_SYSTEM.newEmptyStruct();
        data.put("foo").newString("old");
        data.put("test").newInt(100);
        currentRevision.put("data", data);

        ValidationResult result = writer.validate(txn, event, currentRevision);
        assertNotNull(result);
        assertTrue(result.skip);
        assertFalse(result.fail);
    }

    @Test
    public void testValidateAnyNonStrictVersionTooHigh() {
        BaseRevisionWriter writer = BaseRevisionWriter.builder()
                .qldbDriver(new MockNoOpQldbDriver())
                .strictMode(false)
                .build();
        MockTransactionExecutor txn = new MockTransactionExecutor();

        LoadEvent event = new LoadEvent();
        event.setOperation(Operation.ANY);
        event.setTableName("Person");
        event.setId(ION_SYSTEM.newString("12345"));
        event.setVersion(10);

        IonStruct revision = ION_SYSTEM.newEmptyStruct();
        revision.put("foo").newString("bar");
        revision.put("test").newInt(10);
        event.setRevision(revision);

        IonStruct currentRevision = ION_SYSTEM.newEmptyStruct();
        IonStruct metadata = ION_SYSTEM.newEmptyStruct();
        metadata.put("id").newString("888888");
        metadata.put("version").newInt(1);
        currentRevision.put("metadata", metadata);

        IonStruct data = ION_SYSTEM.newEmptyStruct();
        data.put("foo").newString("old");
        data.put("test").newInt(100);
        currentRevision.put("data", data);

        ValidationResult result = writer.validate(txn, event, currentRevision);
        assertNotNull(result);
        assertFalse(result.skip);
        assertTrue(result.fail);
    }

    @Test
    public void testValidateAnyNonStrictVersionSame() {
        BaseRevisionWriter writer = BaseRevisionWriter.builder()
                .qldbDriver(new MockNoOpQldbDriver())
                .strictMode(false)
                .build();
        MockTransactionExecutor txn = new MockTransactionExecutor();

        LoadEvent event = new LoadEvent();
        event.setOperation(Operation.ANY);
        event.setTableName("Person");
        event.setId(ION_SYSTEM.newString("12345"));
        event.setVersion(1);

        IonStruct revision = ION_SYSTEM.newEmptyStruct();
        revision.put("foo").newString("bar");
        revision.put("test").newInt(10);
        event.setRevision(revision);

        IonStruct currentRevision = ION_SYSTEM.newEmptyStruct();
        IonStruct metadata = ION_SYSTEM.newEmptyStruct();
        metadata.put("id").newString("888888");
        metadata.put("version").newInt(1);
        currentRevision.put("metadata", metadata);

        IonStruct data = ION_SYSTEM.newEmptyStruct();
        data.put("foo").newString("old");
        data.put("test").newInt(100);
        currentRevision.put("data", data);

        ValidationResult result = writer.validate(txn, event, currentRevision);
        assertNotNull(result);
        assertTrue(result.skip);
        assertFalse(result.fail);
    }

    @Test
    public void testValidateAnyNonStrictValid() {
        BaseRevisionWriter writer = BaseRevisionWriter.builder()
                .qldbDriver(new MockNoOpQldbDriver())
                .strictMode(false)
                .build();
        MockTransactionExecutor txn = new MockTransactionExecutor();

        LoadEvent event = new LoadEvent();
        event.setOperation(Operation.ANY);
        event.setTableName("Person");
        event.setId(ION_SYSTEM.newString("12345"));
        event.setVersion(1);

        IonStruct revision = ION_SYSTEM.newEmptyStruct();
        revision.put("foo").newString("bar");
        revision.put("test").newInt(10);
        event.setRevision(revision);

        IonStruct currentRevision = ION_SYSTEM.newEmptyStruct();
        IonStruct metadata = ION_SYSTEM.newEmptyStruct();
        metadata.put("id").newString("888888");
        metadata.put("version").newInt(0);
        currentRevision.put("metadata", metadata);

        IonStruct data = ION_SYSTEM.newEmptyStruct();
        data.put("foo").newString("old");
        data.put("test").newInt(100);
        currentRevision.put("data", data);

        ValidationResult result = writer.validate(txn, event, currentRevision);
        assertNotNull(result);
        assertFalse(result.skip);
        assertFalse(result.fail);
    }
    
    
    static class MockNoOpQldbDriver implements QldbDriver {
        @Override
        public void execute(ExecutorNoReturn executorNoReturn) {
        }

        @Override
        public void execute(ExecutorNoReturn executorNoReturn, RetryPolicy retryPolicy) {
        }

        @Override
        public <T> T execute(Executor<T> executor) {
            return null;
        }

        @Override
        public <T> T execute(Executor<T> executor, RetryPolicy retryPolicy) {
            return null;
        }

        @Override
        public Iterable<String> getTableNames() {
            return null;
        }

        @Override
        public void close() {
        }
    }


    static class MockTransactionExecutor extends TransactionExecutor {
        public String query;
        public List<IonValue> parameters;


        public MockTransactionExecutor() {
            super(null);
        }

        public Result execute(String statement) {
            this.query = statement;
            return null;
        }

        public Result execute(String statement, IonValue... parameters) {
            this.parameters = Arrays.asList(parameters);
            this.query = statement;
            return null;
        }

        public Result execute(String statement, List<IonValue> parameters) {
            this.parameters = parameters;
            this.query = statement;
            return null;
        }
    }
}
