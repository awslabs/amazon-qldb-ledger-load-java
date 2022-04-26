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
package software.amazon.qldb.load;

import com.amazon.ion.IonString;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;
import com.amazon.ion.system.IonSystemBuilder;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;


public class LoadEventTest {
    private final static IonSystem ION_SYSTEM = IonSystemBuilder.standard().build();

    @Test
    public void testEmptyConstructor() {
        assertDoesNotThrow(() -> new LoadEvent());
    }

    @Test
    public void testSetIdValid() {
        LoadEvent event = new LoadEvent();
        event.setId(ION_SYSTEM.newString("hello"));
        IonValue val = event.getId();

        assertNotNull(val);
        assertInstanceOf(IonString.class, val);
        assertEquals("hello", ((IonString) val).stringValue());
    }

    @Test
    public void testSetIdNullParam() {
        LoadEvent event = new LoadEvent();
        event.setId((IonValue) null);
        assertNull(event.getId());
    }

    @Test
    public void testSetIdNullIonTypeParam() {
        LoadEvent event = new LoadEvent();
        event.setId(ION_SYSTEM.newNull());
        assertNull(event.getId());
    }

    @Test
    public void testSetIdNullIonValueParam() {
        LoadEvent event = new LoadEvent();
        event.setId(ION_SYSTEM.newString(null));
        assertNull(event.getId());
    }

    @Test
    public void testSetIdStringValid() {
        LoadEvent event = new LoadEvent();
        event.setId("foo");
        assertEquals(ION_SYSTEM.newString("foo"), event.getId());
    }

    @Test
    public void testSetIdStringValidTrimmed() {
        LoadEvent event = new LoadEvent();
        event.setId(" foo ");
        assertEquals(ION_SYSTEM.newString("foo"), event.getId());
    }

    @Test
    public void testSetIdStringNull() {
        LoadEvent event = new LoadEvent();
        event.setId((String) null);
        assertNull(event.getId());
    }

    @Test
    public void testSetIdStringEmpty() {
        LoadEvent event = new LoadEvent();
        event.setId("");
        assertNull(event.getId());
    }

    @Test
    public void testSetIdStringTrimmedToNull() {
        LoadEvent event = new LoadEvent();
        event.setId("   ");
        assertNull(event.getId());
    }

    @Test
    public void testSetTableNameValid() {
        LoadEvent event = new LoadEvent();
        event.setTableName("foo");
        assertEquals("foo", event.getTableName());
    }

    @Test
    public void testSetTableNameTrimValid() {
        LoadEvent event = new LoadEvent();
        event.setTableName("  foo  ");
        assertEquals("foo", event.getTableName());
    }

    @Test
    public void testSetTableNameNull() {
        LoadEvent event = new LoadEvent();
        event.setTableName(null);
        assertNull(event.getTableName());
    }

    @Test
    public void testSetTableNameEmptyString() {
        LoadEvent event = new LoadEvent();
        event.setTableName("");
        assertNull(event.getTableName());
    }

    @Test
    public void testSetTableNameWhitespace() {
        LoadEvent event = new LoadEvent();
        event.setTableName("   ");
        assertNull(event.getTableName());
    }

    @Test
    public void testSetOperationVaid() {
        LoadEvent event = new LoadEvent();
        event.setOperation(Operation.INSERT);
        assertEquals(Operation.INSERT, event.getOperation());
    }

    @Test
    public void testSetOperationNullParam() {
        LoadEvent event = new LoadEvent();
        event.setOperation(null);
        assertNull(event.getOperation());
    }

    @Test
    public void testGetVersionDefault() {
        LoadEvent event = new LoadEvent();
        assertTrue(event.getVersion() < 0);
    }

    @Test
    public void testSetVersion() {
        LoadEvent event = new LoadEvent();
        event.setVersion(100);
        assertEquals(100, event.getVersion());
    }

    @Test
    public void testSetRevisionNullParam() {
        LoadEvent event = new LoadEvent();
        event.setRevision(null);
        assertNull(event.getRevision());
    }

    @Test
    public void testSetRevisionNullIonValueParam() {
        LoadEvent event = new LoadEvent();
        event.setRevision(ION_SYSTEM.newNullStruct());
        assertNull(event.getRevision());
    }

    @Test
    public void testSetRevisionEmptyStructAllowed() {
        LoadEvent event = new LoadEvent();
        event.setRevision(ION_SYSTEM.newEmptyStruct());
        assertEquals(ION_SYSTEM.newEmptyStruct(), event.getRevision());
    }

    @Test
    public void testSetRevisionValid() {
        LoadEvent event = new LoadEvent();

        IonStruct arg = ION_SYSTEM.newEmptyStruct();
        arg.put("foo").newString("bar");
        arg.put("test").newInt(10);
        event.setRevision(arg);

        IonStruct result = event.getRevision();
        assertNotNull(result);
        assertTrue(result.containsKey("foo"));
        assertEquals(ION_SYSTEM.newString("bar"), result.get("foo"));
        assertTrue(result.containsKey("test"));
        assertEquals(ION_SYSTEM.newInt(10), result.get("test"));
    }

    @Test
    public void testToIonEmptyConstructor() {
        LoadEvent event = new LoadEvent();
        IonStruct result = event.toIon();
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testToIonAllFields() {
        LoadEvent event = new LoadEvent();
        event.setOperation(Operation.INSERT);
        event.setVersion(200);
        event.setId(ION_SYSTEM.newString("12345"));
        event.setTableName("Person");
        event.setGroupingValue("aaa");

        IonStruct revision = ION_SYSTEM.newEmptyStruct();
        revision.put("foo").newString("bar");
        revision.put("test").newInt(10);
        event.setRevision(revision);

        IonStruct ion = ION_SYSTEM.newEmptyStruct();
        ion.put("op").newString(Operation.INSERT.name());
        ion.put("version").newInt(200);
        ion.put("id").newString("12345");
        ion.put("table").newString("Person");
        ion.put("group").newString("aaa");

        IonStruct data = ION_SYSTEM.newEmptyStruct();
        data.put("foo").newString("bar");
        data.put("test").newInt(10);
        ion.put("data", data);

        assertEquals(ion, event.toIon());
    }

    @Test
    public void testFromIonNullParam() {
        assertNull(LoadEvent.fromIon(null));
    }

    @Test
    public void testFromIonEmptyStructParam() {
        assertNull(LoadEvent.fromIon(ION_SYSTEM.newEmptyStruct()));
    }

    @Test
    public void testFromIonNullStructParam() {
        assertNull(LoadEvent.fromIon(ION_SYSTEM.newNullStruct()));
    }

    @Test
    public void testFromIonValid() {
        IonStruct ion = ION_SYSTEM.newEmptyStruct();
        ion.put("op").newString(Operation.INSERT.name());
        ion.put("version").newInt(200);
        ion.put("id").newString("12345");
        ion.put("table").newString("Person");
        ion.put("group").newString("aaa");

        IonStruct data = ION_SYSTEM.newEmptyStruct();
        data.put("foo").newString("bar");
        data.put("test").newInt(10);
        ion.put("data", data);

        LoadEvent event = LoadEvent.fromIon(ion);
        assertEquals(Operation.INSERT, event.getOperation());
        assertEquals(200, event.getVersion());
        assertEquals(ION_SYSTEM.newString("12345"), event.getId());
        assertEquals("Person", event.getTableName());
        assertEquals("aaa", event.getGroupingValue());

        IonStruct child = ION_SYSTEM.newEmptyStruct();
        child.put("foo").newString("bar");
        child.put("test").newInt(10);

        assertEquals(child, event.getRevision());
    }

    @Test
    public void testFromCommittedRevisionNullStruct() {
        assertNull(LoadEvent.fromCommittedRevision(null, "xyz"));
    }

    @Test
    public void testFromCommittedRevisionEmptyStruct() {
        assertNull(LoadEvent.fromCommittedRevision(ION_SYSTEM.newEmptyStruct(), "xyz"));
    }

    @Test
    public void testFromCommittedRevisionNullTable() {
        IonStruct revision = ION_SYSTEM.newEmptyStruct();
        revision.put("metadata").newEmptyStruct();
        revision.put("blockAddress").newEmptyStruct();

        assertNull(LoadEvent.fromCommittedRevision(revision, null));
    }

    @Test
    public void testFromCommittedRevisionNoMetadata() {
        IonStruct revision = ION_SYSTEM.newEmptyStruct();
        revision.put("blockAddress").newEmptyStruct();
        revision.put("data").newEmptyStruct();

        assertNull(LoadEvent.fromCommittedRevision(revision, "xyz"));
    }

    @Test
    public void testFromCommittedRevisionGoodInsert() {
        IonStruct revision = ION_SYSTEM.newEmptyStruct();
        revision.put("blockAddress").newEmptyStruct();

        IonStruct metadata = ION_SYSTEM.newEmptyStruct();
        metadata.put("id").newString("1234");
        metadata.put("version").newInt(0);
        revision.put("metadata", metadata);

        IonStruct data = ION_SYSTEM.newEmptyStruct();
        data.put("id").newString("1234");
        revision.put("data", data);

        LoadEvent event = LoadEvent.fromCommittedRevision(revision, "xyz");

        assertEquals(Operation.INSERT, event.getOperation());
        assertEquals("xyz", event.getTableName());
        assertEquals(0, event.getVersion());
        assertEquals("1234", ((IonString) event.getId()).stringValue());
        assertNotNull(event.getRevision());

        IonStruct rev = event.getRevision();
        assertTrue(rev.containsKey("id"));
        assertEquals("1234", ((IonString) rev.get("id")).stringValue());
    }

    @Test
    public void testFromCommittedRevisionGoodUpdate() {
        IonStruct revision = ION_SYSTEM.newEmptyStruct();
        revision.put("blockAddress").newEmptyStruct();

        IonStruct metadata = ION_SYSTEM.newEmptyStruct();
        metadata.put("id").newString("1234");
        metadata.put("version").newInt(1);
        revision.put("metadata", metadata);

        IonStruct data = ION_SYSTEM.newEmptyStruct();
        data.put("id").newString("1234");
        revision.put("data", data);

        LoadEvent event = LoadEvent.fromCommittedRevision(revision, "xyz");

        assertEquals(Operation.UPDATE, event.getOperation());
        assertEquals("xyz", event.getTableName());
        assertEquals(1, event.getVersion());
        assertEquals("1234", ((IonString) event.getId()).stringValue());
        assertNotNull(event.getRevision());

        IonStruct rev = event.getRevision();
        assertTrue(rev.containsKey("id"));
        assertEquals("1234", ((IonString) rev.get("id")).stringValue());
    }

    @Test
    public void testFromCommittedRevisionGoodDelete() {
        IonStruct revision = ION_SYSTEM.newEmptyStruct();
        revision.put("blockAddress").newEmptyStruct();

        IonStruct metadata = ION_SYSTEM.newEmptyStruct();
        metadata.put("id").newString("1234");
        metadata.put("version").newInt(4);
        revision.put("metadata", metadata);

        LoadEvent event = LoadEvent.fromCommittedRevision(revision, "xyz");

        assertEquals(Operation.DELETE, event.getOperation());
        assertEquals("xyz", event.getTableName());
        assertEquals(4, event.getVersion());
        assertEquals("1234", ((IonString) event.getId()).stringValue());
        assertNull(event.getRevision());
    }

    @Test void testFromStringNull() {
        assertNull(LoadEvent.fromString(null));
    }

    @Test void testFromStringEmptyString() {
        assertNull(LoadEvent.fromString(""));
    }

    @Test void testFromStringEmptyIon() {
        assertNull(LoadEvent.fromString("{}"));
    }

    @Test void testFromStringJSONString() {
        String json = "{\"op\": \"INSERT\", \"id\": \"12345\", \"version\": 200, \"group\": \"aaa\", \"table\": \"Person\", \"data\": {\"foo\": \"bar\", \"test\": 10}}";
        LoadEvent event = LoadEvent.fromString(json);
        assertNotNull(event);

        assertEquals(Operation.INSERT, event.getOperation());
        assertEquals(200, event.getVersion());
        assertEquals(ION_SYSTEM.newString("12345"), event.getId());
        assertEquals("Person", event.getTableName());
        assertEquals("aaa", event.getGroupingValue());

        IonStruct child = ION_SYSTEM.newEmptyStruct();
        child.put("foo").newString("bar");
        child.put("test").newInt(10);

        assertEquals(child, event.getRevision());
    }

    @Test
    public void testFromStringIonString() {
        IonStruct ion = ION_SYSTEM.newEmptyStruct();
        ion.put("op").newString(Operation.INSERT.name());
        ion.put("version").newInt(200);
        ion.put("id").newString("12345");
        ion.put("table").newString("Person");
        ion.put("group").newString("aaa");

        IonStruct data = ION_SYSTEM.newEmptyStruct();
        data.put("foo").newString("bar");
        data.put("test").newInt(10);
        ion.put("data", data);

        LoadEvent event = LoadEvent.fromString(ion.toString());
        assertEquals(Operation.INSERT, event.getOperation());
        assertEquals(200, event.getVersion());
        assertEquals(ION_SYSTEM.newString("12345"), event.getId());
        assertEquals("Person", event.getTableName());
        assertEquals("aaa", event.getGroupingValue());

        IonStruct child = ION_SYSTEM.newEmptyStruct();
        child.put("foo").newString("bar");
        child.put("test").newInt(10);

        assertEquals(child, event.getRevision());
    }

    @Test void testfromStringInvalidJSON() {
        String json = "{foo:bar, blah:{}";
        assertNull(LoadEvent.fromString(json));
    }

    @Test
    public void testIsValidValid() {
        LoadEvent event = new LoadEvent();
        event.setOperation(Operation.INSERT);
        event.setVersion(200);
        event.setId(ION_SYSTEM.newString("12345"));
        event.setTableName("Person");
        event.setGroupingValue("aaa");

        IonStruct revision = ION_SYSTEM.newEmptyStruct();
        revision.put("foo").newString("bar");
        revision.put("test").newInt(10);
        event.setRevision(revision);

        assertTrue(event.isValid());
    }

    @Test
    public void testIsValidNoOperation() {
        LoadEvent event = new LoadEvent();
        event.setVersion(200);
        event.setId(ION_SYSTEM.newString("12345"));
        event.setTableName("Person");

        IonStruct revision = ION_SYSTEM.newEmptyStruct();
        revision.put("foo").newString("bar");
        revision.put("test").newInt(10);
        event.setRevision(revision);

        assertFalse(event.isValid());
    }

    @Test
    public void testIsValidNoTable() {
        LoadEvent event = new LoadEvent();
        event.setOperation(Operation.INSERT);
        event.setVersion(200);
        event.setId(ION_SYSTEM.newString("12345"));

        IonStruct revision = ION_SYSTEM.newEmptyStruct();
        revision.put("foo").newString("bar");
        revision.put("test").newInt(10);
        event.setRevision(revision);

        assertFalse(event.isValid());
    }

    @Test
    public void testIsValidInsertNoData() {
        LoadEvent event = new LoadEvent();
        event.setOperation(Operation.INSERT);
        event.setVersion(200);
        event.setId(ION_SYSTEM.newString("12345"));
        event.setTableName("Person");

        assertFalse(event.isValid());
    }

    @Test
    public void testIsValidUpdateNoData() {
        LoadEvent event = new LoadEvent();
        event.setOperation(Operation.UPDATE);
        event.setVersion(200);
        event.setId(ION_SYSTEM.newString("12345"));
        event.setTableName("Person");

        assertFalse(event.isValid());
    }

    @Test
    public void testIsValidDeleteNoData() {
        LoadEvent event = new LoadEvent();
        event.setOperation(Operation.DELETE);
        event.setVersion(200);
        event.setId(ION_SYSTEM.newString("12345"));
        event.setTableName("Person");

        assertTrue(event.isValid());
    }
}