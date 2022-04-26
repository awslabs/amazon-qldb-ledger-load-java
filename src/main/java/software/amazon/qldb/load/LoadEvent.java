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

import com.amazon.ion.*;
import com.amazon.ion.system.IonSystemBuilder;
import software.amazon.qldb.load.util.LoaderUtils;

import java.util.Iterator;


/**
 * Represents a pending insert, update, or delete of a single document revision against a ledger.  Events may have a
 * grouping value to specify event ordering within a group for loaders that support it (e.g. FIFO loaders).
 */
public class LoadEvent {
    protected final static IonSystem ION_SYSTEM = IonSystemBuilder.standard().build();

    private Operation operation;
    private String tableName;
    private String groupingValue;
    private IonValue id;
    private IonStruct revision;
    private int version = -1;


    /**
     * @return  The unique/idempotence identifier for the record delivered in this load event
     */
    public IonValue getId() {
        return id;
    }

    public void setId(IonValue id) {
        if (id == null || id.isNullValue())
            this.id = null;
        else
            this.id = id;
    }

    public void setId(String str) {
        if (str != null) {
            str = str.trim();
            if (str.length() == 0)
                str = null;
        }

        if (str == null)
            this.id = null;
        else
            this.id = ION_SYSTEM.newString(str);
    }

    /**
     * @return The table that this event will be written to in the target ledger
     */
    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        if (tableName != null) {
            tableName = tableName.trim();
            if (tableName.length() == 0)
                tableName = null;
        }

        this.tableName = tableName;
    }

    /**
     * @return The operation (insert/update/delete) for this event
     */
    public Operation getOperation() {
        return operation;
    }

    public void setOperation(Operation operation) {
        this.operation = operation;
    }

    /**
     * @return The data to load into the target ledger
     */
    public IonStruct getRevision() {
        return revision;
    }

    public void setRevision(IonStruct revision) {
        if (revision == null || revision.isNullValue())
            this.revision = null;
        else
            this.revision = revision;
    }

    /**
     * @return The revision number of the document contained in this event.  This is used to make sure revisions are
     * delivered in order to the target ledger.
     */
    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    /**
     * @return If the event is delivered over a FIFO channel, this value is used to determine grouping/sharding.
     */
    public String getGroupingValue() {
        return groupingValue;
    }

    public void setGroupingValue(String groupingValue) {
        this.groupingValue = groupingValue;
    }

    /**
     * Performs validation checks on this load event object.
     *
     * @return True if validation succeeds.  False otherwise.
     */
    public boolean isValid() {
        if (operation == null || tableName == null)
            return false;

        return revision != null || (operation != Operation.INSERT && operation != Operation.UPDATE);
    }


    /**
     * @return An Ion representation of this load event
     */
    public IonStruct toIon() {
        IonStruct ion = ION_SYSTEM.newEmptyStruct();

        if (this.operation != null)
            ion.put("op").newString(this.operation.name());

        if (this.id != null)
            ion.put("id", this.id.clone());

        if (this.tableName != null)
            ion.put("table").newString(this.tableName);

        if (this.revision != null)
            ion.put("data", this.revision.clone());

        if (this.version >= 0)
            ion.put("version").newInt(this.version);

        if (this.groupingValue != null)
            ion.put("group").newString(this.groupingValue);

        return ion;
    }

    /**
     * @return A formatted Ion string representation of this event
     */
    public String toPrettyString() {
        return toIon().toPrettyString();
    }

    /**
     * @return The Ion text representation of this event
     */
    public String toString() {
        return toIon().toString();
    }

    /**
     * Creates an ID for this event that can be used to uniquely identify it for de-duplication purposes. The ID is a
     * Base64-encoded SHA-256 hash of the Ion representation of this event.
     *
     * @return The de-duplication ID for this event.
     */
    public String getDeduplicationId() {
        return LoaderUtils.hashIonValue(toIon());
    }


    /**
     * Creates a load event from a parsed JSON representation of the event.
     *
     * @param ion The Ion document to convert to a LoadEvent
     * @return A load event populated from the Ion object
     */
    public static LoadEvent fromIon(IonStruct ion) {
        if (ion == null || ion.isNullValue() || ion.isEmpty())
            return null;

        LoadEvent event = new LoadEvent();

        if (ion.containsKey("id"))
            event.setId(ion.get("id"));

        if (ion.containsKey("data") && ion.get("data").getType() == IonType.STRUCT)
            event.setRevision((IonStruct) ion.get("data").clone());

        if (ion.containsKey("table") && ion.get("table").getType() == IonType.STRING)
            event.setTableName(((IonString) ion.get("table")).stringValue());

        if (ion.containsKey("op") && ion.get("op").getType() == IonType.STRING)
            event.setOperation(Operation.forString(((IonString) ion.get("op")).stringValue()));

        if (ion.containsKey("version") && ion.get("version").getType() == IonType.INT)
            event.setVersion(((IonInt) ion.get("version")).intValue());

        if (ion.containsKey("group") && ion.get("group").getType() == IonType.STRING)
            event.setGroupingValue(((IonString) ion.get("group")).stringValue());

        return event;
    }


    /**
     * Creates a LoadEvent from an IonStruct that represents a committed-view revision from a ledger (contains metadata,
     * blockAddress, etc. fields).
     *
     * @param revision  The revision to load from
     * @param tableName The ledger table the revision belongs to
     * @return A load event object populated from the revision and its metadata or null if the arguments are null or
     *         empty.
     */
    public static LoadEvent fromCommittedRevision(IonStruct revision, String tableName) {
        if (revision == null || revision.isNullValue() || revision.isEmpty())
            return null;

        if (tableName == null)
            return null;

        if (!revision.containsKey("metadata"))
            return null;

        IonStruct metadata = (IonStruct) revision.get("metadata");
        String docId = ((IonString) metadata.get("id")).stringValue();
        int version = ((IonInt) metadata.get("version")).intValue();

        IonStruct data = revision.containsKey("data") ? (IonStruct) revision.get("data") : null;

        Operation op = null;
        if (data == null)
            op = Operation.DELETE;
        else if (version == 0)
            op = Operation.INSERT;
        else
            op = Operation.UPDATE;

        LoadEvent event = new LoadEvent();
        event.setOperation(op);
        event.setId(docId);
        event.setTableName(tableName);
        event.setVersion(version);

        if (data != null)
            event.setRevision(data.clone());

        return event;
    }


    /**
     * Creates a LoadEvent from a JSON or Ion-formatted string.
     *
     * @param str The string representation of the event to load.
     * @return A fully-populated load event or null if the string is null, empty, or represents an empty Ion document.
     */
    public static LoadEvent fromString(String str) {
        if (str == null)
            return null;

        LoadEvent event = null;

        try {
            IonDatagram datagram = ION_SYSTEM.getLoader().load(str);
            Iterator<IonValue> iter = datagram.iterator();
            if (iter.hasNext()) {
                IonValue val = iter.next();
                if (val.getType() != IonType.STRUCT) {
                    return null;
                }

                event = LoadEvent.fromIon((IonStruct) val);
            }
        } catch (Exception ignored) {
        }

        return event;
    }
}
