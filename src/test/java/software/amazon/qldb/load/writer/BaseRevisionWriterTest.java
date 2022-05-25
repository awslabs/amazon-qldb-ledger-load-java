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
    public void testBuilderIsCorrectType() {
        RevisionWriterBuilder builder = BaseRevisionWriter.builder();
        assertNotNull(builder);
        assertInstanceOf(BaseRevisionWriter.BaseRevisionWriterBuilder.class, builder);
    }

    @Test
    public void testStrictModeIsDefault() {
        BaseRevisionWriter writer = (BaseRevisionWriter) makeWriter();
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

        BaseRevisionWriter writer = (BaseRevisionWriter) makeWriter();
        assertNull(writer.readCurrentRevision(null, event));
    }

    @Test
    public void testReadCurrentVersionNullEvent() {
        BaseRevisionWriter writer = (BaseRevisionWriter) makeWriter();
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

        BaseRevisionWriter writer = (BaseRevisionWriter) makeWriter();
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

        BaseRevisionWriter writer = (BaseRevisionWriter) makeWriter();
        assertNull(writer.readCurrentRevision(new MockTransactionExecutor(), event));
    }

    @Test
    public void testAdjustRevisionNullEvent() {
        assertDoesNotThrow(() -> {
            BaseRevisionWriter writer = (BaseRevisionWriter) makeWriter();
            writer.adjustRevision(null, null, null);
        });
    }

    @Test
    public void testAdjustRevisionNullEventId() {
        assertDoesNotThrow(() -> {
            LoadEvent event = new LoadEvent();
            BaseRevisionWriter writer = (BaseRevisionWriter) makeWriter();
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

        BaseRevisionWriter writer = (BaseRevisionWriter) makeWriter();
        writer.adjustRevision(null, event, null);

        assertTrue(revision.containsKey("oldDocumentId"));
        assertEquals(ION_SYSTEM.newString("9999999"), revision.get("oldDocumentId"));
    }

    @Test
    public void testWriteDocumentInsert() {
        BaseRevisionWriter writer = (BaseRevisionWriter) makeWriter();
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
        BaseRevisionWriter writer = (BaseRevisionWriter) makeWriter();
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
        BaseRevisionWriter writer = (BaseRevisionWriter) makeWriter();
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
        BaseRevisionWriter writer = (BaseRevisionWriter) makeWriter();
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
        BaseRevisionWriter writer = (BaseRevisionWriter) makeWriter();
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
        BaseRevisionWriter writer = (BaseRevisionWriter) makeWriter();
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
        RevisionWriter writer = makeWriter(true);
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
        RevisionWriter writer = makeWriter(true);
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
        RevisionWriter writer = makeWriter(true);
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
        RevisionWriter writer = makeWriter(false);
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
        RevisionWriter writer = makeWriter(false);
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
        RevisionWriter writer = makeWriter(false);
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
        RevisionWriter writer = makeWriter(true);
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
        RevisionWriter writer = makeWriter(true);
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
        RevisionWriter writer = makeWriter(true);
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
        RevisionWriter writer = makeWriter(true);
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
        RevisionWriter writer = makeWriter(true);
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
        RevisionWriter writer = makeWriter(false);
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
        RevisionWriter writer = makeWriter(false);
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
        RevisionWriter writer = makeWriter(false);
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
        RevisionWriter writer = makeWriter(false);
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
        RevisionWriter writer = makeWriter(false);
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
        RevisionWriter writer = makeWriter(true);
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
        RevisionWriter writer = makeWriter(true);
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
        RevisionWriter writer = makeWriter(true);
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
        RevisionWriter writer = makeWriter(true);
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
        RevisionWriter writer = makeWriter(true);
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
        RevisionWriter writer = makeWriter(false);
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
        RevisionWriter writer = makeWriter(false);
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
        RevisionWriter writer = makeWriter(false);
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
        RevisionWriter writer = makeWriter(false);
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
        RevisionWriter writer = makeWriter(false);
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
        RevisionWriter writer = makeWriter(true);
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
        RevisionWriter writer = makeWriter(true);
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
        RevisionWriter writer = makeWriter(true);
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
        RevisionWriter writer = makeWriter(true);
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
        RevisionWriter writer = makeWriter(false);
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
        RevisionWriter writer = makeWriter(false);
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
        RevisionWriter writer = makeWriter(false);
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
        RevisionWriter writer = makeWriter(false);
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


    private RevisionWriter makeWriter(boolean strictMode) {
        BaseRevisionWriter.BaseRevisionWriterBuilder builder = (BaseRevisionWriter.BaseRevisionWriterBuilder) BaseRevisionWriter.builder();

        return builder
                .qldbDriver(new MockNoOpQldbDriver())
                .strictMode(strictMode)
                .build();
    }


    private RevisionWriter makeWriter() {
        BaseRevisionWriter.BaseRevisionWriterBuilder builder = (BaseRevisionWriter.BaseRevisionWriterBuilder) BaseRevisionWriter.builder();
        return builder
                .qldbDriver(new MockNoOpQldbDriver())
                .build();
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
